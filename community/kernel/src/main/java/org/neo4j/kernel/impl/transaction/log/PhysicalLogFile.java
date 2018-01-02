/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

/**
 * {@link LogFile} backed by one or more files in a {@link FileSystemAbstraction}.
 */
public class PhysicalLogFile extends LifecycleAdapter implements LogFile
{
    public interface Monitor
    {
        void opened( File logFile, long logVersion, long lastTransactionId, boolean clean );

        class Adapter implements Monitor
        {
            @Override
            public void opened( File logFile, long logVersion, long lastTransactionId, boolean clean )
            {
            }
        }
    }

    public static final String DEFAULT_NAME = "neostore.transaction.db";
    public static final String REGEX_DEFAULT_NAME = "neostore\\.transaction\\.db";
    public static final String DEFAULT_VERSION_SUFFIX = ".";
    public static final String REGEX_DEFAULT_VERSION_SUFFIX = "\\.";
    private final long rotateAtSize;
    private final FileSystemAbstraction fileSystem;
    private final TransactionIdStore transactionIdStore;
    private final PhysicalLogFiles logFiles;
    private final TransactionMetadataCache transactionMetadataCache;
    private final Monitor monitor;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
    private PhysicalWritableLogChannel writer;
    private final LogVersionRepository logVersionRepository;
    private final LogVersionBridge readerLogVersionBridge;

    private volatile PhysicalLogVersionedStoreChannel channel;

    public PhysicalLogFile( FileSystemAbstraction fileSystem, PhysicalLogFiles logFiles, long rotateAtSize,
            TransactionIdStore transactionIdStore,
            LogVersionRepository logVersionRepository, Monitor monitor,
            TransactionMetadataCache transactionMetadataCache )
    {
        this.fileSystem = fileSystem;
        this.rotateAtSize = rotateAtSize;
        this.transactionIdStore = transactionIdStore;
        this.logVersionRepository = logVersionRepository;
        this.monitor = monitor;
        this.transactionMetadataCache = transactionMetadataCache;
        this.logFiles = logFiles;
        this.readerLogVersionBridge = new ReaderLogVersionBridge( fileSystem, logFiles );
    }

    @Override
    public void init() throws Throwable
    {
        // Make sure at least a bare bones log file is available before recovery
        long lastLogVersionUsed = logVersionRepository.getCurrentLogVersion();
        channel = openLogChannelForVersion( lastLogVersionUsed );
        channel.close();
    }

    @Override
    public void start() throws Throwable
    {
        // Recovery has taken place before this, so the log file has been truncated to last known good tx
        // Just read header and move to the end

        long lastLogVersionUsed = logVersionRepository.getCurrentLogVersion();
        channel = openLogChannelForVersion( lastLogVersionUsed );
        // Move to the end
        channel.position( channel.size() );

        writer = new PhysicalWritableLogChannel( channel );
    }

    // In order to be able to write into a logfile after life.stop during shutdown sequence
    // we will close channel and writer only during shutdown phase when all pending changes (like last
    // checkpoint) are already in
    @Override
    public void shutdown() throws Throwable
    {
        if ( writer != null )
        {
            writer.close();
        }
        if ( channel != null )
        {
            channel.close();
        }
    }

    @Override
    public boolean rotationNeeded() throws IOException
    {
        /*
         * Whereas channel.size() should be fine, we're safer calling position() due to possibility
         * of this file being memory mapped or whatever.
         */
        return channel.position() >= rotateAtSize;
    }

    @Override
    public synchronized void rotate() throws IOException
    {
        channel = rotate( channel );
        writer.setChannel( channel );
    }

    private PhysicalLogVersionedStoreChannel rotate( LogVersionedStoreChannel currentLog ) throws IOException
    {
        /*
         * The store is now flushed. If we fail now the recovery code will open the
         * current log file and replay everything. That's unnecessary but totally ok.
         */
        long newLogVersion = logVersionRepository.incrementAndGetVersion();
        /*
         * Rotation can happen at any point, although not concurrently with an append,
         * although an append may have (most likely actually) left at least some bytes left
         * in the buffer for future flushing. Flushing that buffer now makes the last appended
         * transaction complete in the log we're rotating away. Awesome.
         */
        writer.emptyBufferIntoChannelAndClearIt().flush();
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
        return newLog;
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

    @Override
    public WritableLogChannel getWriter()
    {
        return writer;
    }

    @Override
    public ReadableVersionableLogChannel getReader( LogPosition position ) throws IOException
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

        if ( !fileSystem.fileExists( fileToOpen ) )
        {
            throw new FileNotFoundException( String.format( "File does not exist [%s]",
                    fileToOpen.getCanonicalPath() ) );
        }

        StoreChannel rawChannel;
        try
        {
            rawChannel = fileSystem.open( fileToOpen, "rw" );
        }
        catch ( FileNotFoundException cause )
        {
            throw Exceptions.withCause( new FileNotFoundException( String.format( "File could not be opened [%s]",
                    fileToOpen.getCanonicalPath() ) ), cause );
        }

        ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        LogHeader header = readLogHeader( buffer, rawChannel, true );
        assert header != null && header.logVersion == version;

        return new PhysicalLogVersionedStoreChannel( rawChannel, version, header.logFormatVersion );
    }


    public static PhysicalLogVersionedStoreChannel tryOpenForVersion( PhysicalLogFiles logFiles,
            FileSystemAbstraction fileSystem, long version )
    {
        try
        {
            return openForVersion( logFiles, fileSystem, version );
        }
        catch ( IOException ex )
        {
            return null;
        }
    }

    @Override
    public void accept( LogFileVisitor visitor, LogPosition startingFromPosition ) throws IOException
    {
        try ( ReadableVersionableLogChannel reader = getReader( startingFromPosition ) )
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
            LogPosition position = LogPosition.start( logVersion );
            if ( !visitor.visit( position, lowTransactionId, highTransactionId ) )
            {
                break;
            }
            logVersion--;
            highTransactionId = previousLogLastTxId;
        }
    }

    @Override
    public File currentLogFile()
    {
        return logFiles.getLogFileForVersion( logFiles.getHighestLogVersion() );
    }

    @Override
    public long currentLogVersion()
    {
        return logFiles.getHighestLogVersion();
    }
}
