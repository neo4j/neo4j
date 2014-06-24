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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader.writeLogHeader;

/**
 * {@link LogFile} backup by one or more files in a {@link FileSystemAbstraction}.
 */
public class PhysicalLogFile extends LifecycleAdapter implements LogFile, LogVersionBridge
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

    public PhysicalLogFile( FileSystemAbstraction fileSystem, PhysicalLogFiles logFiles, long rotateAtSize,
            LogPruneStrategy pruneStrategy, TransactionIdStore transactionIdStore,
            LogVersionRepository logVersionRepository, Monitor monitor, LogRotationControl logRotationControl,
            TransactionMetadataCache transactionMetadataCache, Visitor<ReadableLogChannel, IOException> recoveredDataVisitor )
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
        logVersionRepository.incrementVersion();
    }

    private PhysicalLogVersionedStoreChannel openLogChannelForVersion( long forVersion ) throws IOException
    {
        File toOpen = logFiles.getVersionFileName( forVersion );
        PhysicalLogVersionedStoreChannel channel = openFileChannel( toOpen, "rw" );
        long[] header = readLogHeader( headerBuffer, channel, false );
        if ( header == null )
        {
            // Either the header is not there in full or the file was new. Don't care
            long lastTxId = transactionIdStore.getLastCommittingTransactionId();
            writeLogHeader( headerBuffer, forVersion, lastTxId );
            transactionMetadataCache.putHeader( forVersion, lastTxId );
            channel.writeAll( headerBuffer );
            channel.setVersion( forVersion );
            monitor.opened( toOpen, forVersion, lastTxId, true );
        }
        return channel;
    }

    private void doRecoveryOn( PhysicalLogVersionedStoreChannel toRecover,
            Visitor<ReadableLogChannel, IOException> recoveredDataVisitor ) throws IOException
    {
        if ( new LogRecoveryCheck( toRecover ).recoveryRequired() )
        { // There are already data in here, which means recovery will need to be performed.
            ReadableLogChannel recoveredDataChannel = new ReadAheadLogChannel( toRecover, NO_MORE_CHANNELS,
                    ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE );
            recoveredDataVisitor.visit( recoveredDataChannel );
            // intentionally keep it open since we're continuing using the underlying channel for the writer below
            logRotationControl.forceEverything();
            // TODO rotate after recovery?
        }
    }

    @Override
    public void checkRotation() throws IOException
    {
        // Whereas channel.size() should be fine, we're safer calling position() due to possibility
        // of this file being memory mapped or whatever.
        if ( channel.position() >= rotateAtSize )
        {
            channel = rotate( channel );
            writer.setChannel( channel );
        }
    }

    private synchronized PhysicalLogVersionedStoreChannel rotate( VersionedStoreChannel currentLog )
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

    //    @Override
    //    public Long getFirstCommittedTxId( long version )
    //    {
    //        if ( version == 0 )
    //        {
    //            return 1L;
    //        }
    //
    //        // First committed tx for version V = last committed tx version V-1 + 1
    //        Long header = positionCache.getHeader( version - 1 );
    //        if ( header != null )
    //        // It existed in cache
    //        {
    //            return header + 1;
    //        }
    //
    //        // Wasn't cached, go look for it
    //        synchronized ( this )
    //        {
    //            if ( version > logVersion )
    //            {
    //                throw new IllegalArgumentException( "Too high version " + version + ", active is " + logVersion );
    //            }
    //            else if ( version == logVersion )
    //            {
    //                throw new IllegalArgumentException( "Last committed tx for the active log isn't determined yet" );
    //            }
    //            else if ( version == logVersion - 1 )
    //            {
    //                return previousLogLastCommittedTx;
    //            }
    //            else
    //            {
    //                File file = getFileName( version );
    //                if ( fileSystem.fileExists( file ) )
    //                {
    //                    try
    //                    {
    //                        long[] headerLongs = VersionAwareLogEntryReader.readLogHeader( fileSystem, file );
    //                        return headerLongs[1] + 1;
    //                    }
    //                    catch ( IOException e )
    //                    {
    //                        throw new RuntimeException( e );
    //                    }
    //                }
    //            }
    //        }
    //        return null;
    //    }
    //
    //
    //    @Override
    //    public Long getFirstStartRecordTimestamp( long version ) throws IOException
    //    {
    //        ReadableByteChannel log = null;
    //        try
    //        {
    //            ByteBuffer buffer = LogExtractor.newLogReaderBuffer();
    //            log = getLogicalLog( version );
    //            VersionAwareLogEntryReader.readLogHeader( buffer, log, true );
    //            LogDeserializer deserializer = new LogDeserializer( buffer, commandReaderFactory );
    //
    //            TimeWrittenConsumer consumer = new TimeWrittenConsumer();
    //
    //            try ( Cursor<LogEntry, IOException> cursor = deserializer.cursor( log ) )
    //            {
    //                while( cursor.next( consumer ) )
    //                {
    //                    ;
    //                }
    //            }
    //            return consumer.getTimeWritten();
    //        }
    //        finally
    //        {
    //            if ( log != null )
    //            {
    //                log.close();
    //            }
    //        }
    //    }
    //
    @Override
    public WritableLogChannel getWriter()
    {
        return writer;
    }

    @Override
    public ReadableLogChannel getReader( LogPosition position ) throws IOException
    {
        VersionedStoreChannel channel = openLogChannel( position );
        return new ReadAheadLogChannel( channel, this, 4 * 1024 );
    }

    @Override
    public VersionedStoreChannel next( VersionedStoreChannel channel ) throws IOException
    {
        PhysicalLogVersionedStoreChannel nextChannel;
        try
        {
            nextChannel = openLogChannel( new LogPosition( channel.getVersion() + 1, 0 ) );
        }
        catch ( FileNotFoundException e )
        {
            return channel;
        }
        // TODO read header properly
        channel.close();
        nextChannel.position( VersionAwareLogEntryReader.LOG_HEADER_SIZE );
        return nextChannel;
    }

    private PhysicalLogVersionedStoreChannel openLogChannel( LogPosition position ) throws IOException
    {
        long version = position.getLogVersion();
        File fileToOpen = logFiles.getVersionFileName( version );
        PhysicalLogVersionedStoreChannel channel = openFileChannel( fileToOpen, "r", version );
        channel.position( position.getByteOffset() );
        return channel;
    }

    private PhysicalLogVersionedStoreChannel openFileChannel( File file, String mode ) throws IOException
    {
        return new PhysicalLogVersionedStoreChannel( fileSystem.open( file, mode ) );
    }

    private PhysicalLogVersionedStoreChannel openFileChannel( File file, String mode, long version ) throws IOException
    {
        return new PhysicalLogVersionedStoreChannel( fileSystem.open( file, mode ), version );
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
    public void accept( LogFileVisitor visitor ) throws IOException
    {
        long currentLogVersion = logFiles.getHighestLogVersion();
        LogPosition position = new LogPosition( currentLogVersion, 16 );
        while( logFiles.versionExists( currentLogVersion ) && visitor.visit( position, getReader( position  ) ) )
        {
            currentLogVersion--;
        }
    }
}
