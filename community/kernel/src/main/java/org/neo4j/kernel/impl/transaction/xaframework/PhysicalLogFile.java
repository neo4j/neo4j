/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.xaframework.log.pruning.LogPruneStrategy;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.transaction.xaframework.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.xaframework.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.readLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogVersions.CURRENT_LOG_VERSION;

/**
 * {@link LogFile} backed by one or more files in a {@link FileSystemAbstraction}.
 */
public class PhysicalLogFile extends LifecycleAdapter implements LogFile
{
    public static final String DEFAULT_NAME = "nioneo_logical.log";
    private final long rotateAtSize;
    private final FileSystemAbstraction fileSystem;
    private final LogPruneStrategy pruneStrategy;
    private final TransactionIdStore transactionIdStore;
    private final PhysicalLogFiles logFiles;
    private final TransactionMetadataCache transactionMetadataCache;
    private final Visitor<ReadableLogChannel, IOException> recoveredDataVisitor;
    private final Monitor monitor;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate( 16 );
    private final LogRotationControl logRotationControl;
    private PhysicalWritableLogChannel writer;
    private final LogVersionRepository logVersionRepository;
    private PhysicalLogVersionedStoreChannel channel;
    private final LogVersionBridge readerLogVersionBridge;

    public PhysicalLogFile( FileSystemAbstraction fileSystem, PhysicalLogFiles logFiles, long rotateAtSize,
                            LogPruneStrategy pruneStrategy, TransactionIdStore transactionIdStore,
                            LogVersionRepository logVersionRepository, Monitor monitor,
                            LogRotationControl logRotationControl,
                            TransactionMetadataCache transactionMetadataCache,
                            Visitor<ReadableLogChannel, IOException> recoveredDataVisitor )
    {
        this.fileSystem = fileSystem;
        this.rotateAtSize = rotateAtSize;
        this.pruneStrategy = pruneStrategy;
        this.transactionIdStore = transactionIdStore;
        this.logVersionRepository = logVersionRepository;
        this.monitor = monitor;
        this.logRotationControl = logRotationControl;
        this.transactionMetadataCache = transactionMetadataCache;
        this.recoveredDataVisitor = recoveredDataVisitor;
        this.logFiles = logFiles;
        this.readerLogVersionBridge = new ReaderLogVersionBridge( fileSystem, logFiles );
    }

    @Override
    public void init() throws Throwable
    {
        long lastLogVersionUsed = logVersionRepository.getCurrentLogVersion();
        channel = openLogChannelForVersion( lastLogVersionUsed );
        writer = new PhysicalWritableLogChannel( channel );
    }

    @Override
    public void start() throws Throwable
    {
        doRecoveryOn( channel, recoveredDataVisitor );
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        logRotationControl.awaitAllTransactionsClosed();
        logRotationControl.forceEverything();
        /*
         *  We simply increment the version, essentially "rotating" away
         *  the current active log file, to avoid having a recovery on
         *  next startup. Not necessary, simply speeds up the startup
         *  process.
         */
        logVersionRepository.incrementAndGetVersion();
    }

    @Override
    public void shutdown() throws Throwable
    {
        writer.close();
        channel.close();
    }

    private PhysicalLogVersionedStoreChannel openLogChannelForVersion( long forVersion ) throws IOException
    {
        File toOpen = logFiles.getLogFileForVersion( forVersion );
        StoreChannel storeChannel = fileSystem.open( toOpen, "rw" );
        LogHeader header = readLogHeader( headerBuffer, storeChannel, false );
        if ( header == null )
        {
            // Either the header is not there in full or the file was new. Don't care
            long lastTxId = transactionIdStore.getLastCommittedTransactionId();
            writeLogHeader( headerBuffer, forVersion, lastTxId );
            transactionMetadataCache.putHeader( forVersion, lastTxId );
            storeChannel.writeAll( headerBuffer );
            monitor.opened( toOpen, forVersion, lastTxId, true );
        }
        byte formatVersion = header == null ? CURRENT_LOG_VERSION : header.logFormatVersion;
        return new PhysicalLogVersionedStoreChannel( storeChannel, forVersion, formatVersion );
    }

    private void doRecoveryOn( PhysicalLogVersionedStoreChannel toRecover,
                               Visitor<ReadableLogChannel, IOException> recoveredDataVisitor ) throws IOException
    {
        if ( new LogRecoveryCheck( toRecover ).recoveryRequired() )
        {   // There's already data in here, which means recovery will need to be performed.
            ReadableLogChannel recoveredDataChannel =
                    new ReadAheadLogChannel( toRecover, NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE );
            recoveredDataVisitor.visit( recoveredDataChannel );
            // intentionally keep it open since we're continuing using the underlying channel for the writer below
            logRotationControl.forceEverything();
        }
    }

    @Override
    public void checkRotation() throws IOException
    {
        // Whereas channel.size() should be fine, we're safer calling position() due to possibility
        // of this file being memory mapped or whatever.
        if ( channel.position() >= rotateAtSize )
        {
            synchronized ( this )
            {
                // Good old double-checked locking
                if ( channel.position() >= rotateAtSize )
                {
                    forceRotate();
                }
            }
        }
    }

    // Do not expose this through the interface; only used in robustness testing.
    public synchronized void forceRotate() throws IOException
    {
        channel = rotate( channel );
        writer.setChannel( channel );
    }

    private PhysicalLogVersionedStoreChannel rotate( LogVersionedStoreChannel currentLog )
            throws IOException
    {
        /*
         * First we flush the store. If we fail now or during the flush, on
         * recovery we'll discover the current log file and replay it. Everything
         * will be ok.
         */
        logRotationControl.awaitAllTransactionsClosed();
        logRotationControl.forceEverything();
        /*
         * The store is now flushed. If we fail now the recovery code will open the
         * current log file and replay everything. That's unnecessary but totally ok.
         */
        long newLogVersion = logVersionRepository.incrementAndGetVersion();
        /*
         * The log version is now in the store, flushed and persistent. If we crash
         * now, on recovery we'll attempt to open the version we're about to create
         * (but haven't yet), discover it's not there. That will lead to creating
         * the file, setting the header and continuing. We'll do just that now.
         * Note that by this point, rotation is done. The next few lines are
         * "simply overhead" for continuing to work with the new file.
         */
        PhysicalLogVersionedStoreChannel newLog = openLogChannelForVersion( newLogVersion );
        currentLog.close();
        pruneStrategy.prune();
        return newLog;
    }

    @Override
    public WritableLogChannel getWriter()
    {
        return writer;
    }

    @Override
    public ReadableLogChannel getReader( LogPosition position ) throws IOException
    {
        PhysicalLogVersionedStoreChannel logChannel = openForVersion( logFiles, fileSystem, position.getLogVersion() );
        logChannel.position( position.getByteOffset() );
        return new ReadAheadLogChannel( logChannel, readerLogVersionBridge, DEFAULT_READ_AHEAD_SIZE );
    }

    public static PhysicalLogVersionedStoreChannel openForVersion( PhysicalLogFiles logFiles,
                                                                   FileSystemAbstraction fileSystem,
                                                                   long version ) throws IOException
    {
        final File fileToOpen = logFiles.getLogFileForVersion( version );
        final StoreChannel rawChannel = fileSystem.open( fileToOpen, "r" );
        ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        LogHeader header = readLogHeader( buffer, rawChannel, true );
        assert header.logVersion == version;
        return new PhysicalLogVersionedStoreChannel( rawChannel, version, header.logFormatVersion );
    }
    public interface Monitor
    {
        void opened( File logFile, long logVersion, long lastTransactionId, boolean clean );

        void failureToTruncate( File logFile, IOException e );
    }

    public static final Monitor NO_MONITOR = new Monitor()
    {
        @Override
        public void opened( File logFile, long logVersion, long lastTransactionId, boolean clean )
        {
        }

        @Override
        public void failureToTruncate( File logFile, IOException e )
        {
        }
    };

    public static class LoggingMonitor implements Monitor
    {
        private final StringLogger logger;

        public LoggingMonitor( StringLogger logger )
        {
            this.logger = logger;
        }

        @Override
        public void opened( File logFile, long logVersion, long lastTransactionId, boolean clean )
        {
            logger.info( "Opened logical log [" + logFile + "] version=" + logVersion + ", lastTxId="
                    + lastTransactionId + " (" + (clean ? "clean" : "recovered") + ")" );
        }

        @Override
        public void failureToTruncate( File logFile, IOException e )
        {
            logger.warn( "Failed to truncate " + logFile + " at correct size", e );
        }
    }

    @Override
    public void accept( LogFileVisitor visitor, LogPosition startingFromPosition ) throws IOException
    {
        try ( ReadableLogChannel reader = getReader( startingFromPosition ) )
        {
            visitor.visit( startingFromPosition, reader );
        }
    }

    @Override
    public void accept( LogHeaderVisitor visitor ) throws IOException
    {
        // Start from the where we're currently at and go backwards in time (versions)
        long logVersion = logFiles.getHighestLogVersion();
        long highTransactionId = transactionIdStore.getLastCommittedTransactionId();
        while ( logFiles.versionExists( logVersion ) )
        {
            long previousLogLastTxId = transactionMetadataCache.getLogHeader( logVersion );
            if ( previousLogLastTxId == -1 )
            {
                LogHeader header = readLogHeader( fileSystem, logFiles.getLogFileForVersion( logVersion ) );
                assert logVersion == header.logVersion;
                transactionMetadataCache.putHeader( header.logVersion, header.lastCommittedTxId );
                previousLogLastTxId = header.lastCommittedTxId;
            }

            long lowTransactionId = previousLogLastTxId + 1;
            LogPosition position = new LogPosition( logVersion, LOG_HEADER_SIZE );
            if ( !visitor.visit( position, lowTransactionId, highTransactionId ) )
            {
                break;
            }
            logVersion--;
            highTransactionId = previousLogLastTxId;
        }
    }
}
