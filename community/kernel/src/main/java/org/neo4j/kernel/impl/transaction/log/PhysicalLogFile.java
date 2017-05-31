/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.function.Supplier;

import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

/**
 * {@link LogFile} backed by one or more files in a {@link FileSystemAbstraction}.
 */
public class PhysicalLogFile implements LogFile, Lifecycle
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

    public static final Monitor NO_MONITOR = new Monitor.Adapter();

    public static final String DEFAULT_NAME = "neostore.transaction.db";
    public static final String REGEX_DEFAULT_NAME = "neostore\\.transaction\\.db";
    public static final String DEFAULT_VERSION_SUFFIX = ".";
    public static final String REGEX_DEFAULT_VERSION_SUFFIX = "\\.";
    private final long rotateAtSize;
    private final FileSystemAbstraction fileSystem;
    private final Supplier<Long> lastCommittedId;
    private final PhysicalLogFiles logFiles;
    private final LogHeaderCache logHeaderCache;
    private final Monitor monitor;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
    private PositionAwarePhysicalFlushableChannel writer;
    private final LogVersionRepository logVersionRepository;
    private final LogVersionBridge readerLogVersionBridge;

    private volatile PhysicalLogVersionedStoreChannel channel;

    public PhysicalLogFile( FileSystemAbstraction fileSystem, PhysicalLogFiles logFiles, long rotateAtSize,
                            Supplier<Long> lastCommittedId, LogVersionRepository logVersionRepository,
                            Monitor monitor, LogHeaderCache logHeaderCache
    )
    {
        this.fileSystem = fileSystem;
        this.rotateAtSize = rotateAtSize;
        this.lastCommittedId = lastCommittedId;
        this.logVersionRepository = logVersionRepository;
        this.monitor = monitor;
        this.logHeaderCache = logHeaderCache;
        this.logFiles = logFiles;
        this.readerLogVersionBridge = new ReaderLogVersionBridge( fileSystem, logFiles );
    }

    @Override
    public void init() throws IOException
    {
        // Make sure at least a bare bones log file is available before recovery
        long lastLogVersionUsed = logVersionRepository.getCurrentLogVersion();
        channel = createLogChannelForVersion( lastLogVersionUsed );
        channel.close();
    }

    @Override
    public void start() throws IOException
    {
        // Recovery has taken place before this, so the log file has been truncated to last known good tx
        // Just read header and move to the end

        long lastLogVersionUsed = logVersionRepository.getCurrentLogVersion();
        channel = createLogChannelForVersion( lastLogVersionUsed );
        // Move to the end
        channel.position( channel.size() );

        writer = new PositionAwarePhysicalFlushableChannel( channel );
    }

    @Override
    public void stop()
    {
        // nothing to stop
    }

    // In order to be able to write into a logfile after life.stop during shutdown sequence
    // we will close channel and writer only during shutdown phase when all pending changes (like last
    // checkpoint) are already in
    @Override
    public void shutdown() throws IOException
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

    /**
     * Rotates the current log file, continuing into next (version) log file.
     * This method must be recovery safe, which means a crash at any point should be recoverable.
     * Concurrent readers must also be able to parry for concurrent rotation.
     * Concurrent writes will not be an issue since rotation and writing contends on the same monitor.
     *
     * Steps during rotation are:
     * <ol>
     * <li>1: Increment log version, {@link LogVersionRepository#incrementAndGetVersion()} (also flushes the store)</li>
     * <li>2: Flush current log</li>
     * <li>3: Create new log file</li>
     * <li>4: Write header</li>
     * </ol>
     *
     * Recovery: what happens if crash between:
     * <ol>
     * <li>1-2: New log version has been set, starting the writer will create the new log file idempotently.
     * At this point there may have been half-written transactions in the previous log version,
     * although they haven't been considered committed and so they will be truncated from log during recovery</li>
     * <li>2-3: New log version has been set, starting the writer will create the new log file idempotently.
     * At this point there may be complete transactions in the previous log version which may not have been
     * acknowledged to be committed back to the user, but will be considered committed anyway.</li>
     * <li>3-4: New log version has been set, starting the writer will see that the new file exists and
     * will be forgiving when trying to read the header of it, so that if it isn't complete a fresh
     * header will be set (TODO actually there's a problem here where the last committed tx from
     * {@link TransactionIdStore} is read and placed in the new header before recovery is completed, which means
     * that, reading (2-3), this number may be a lower number than what the previous log actually contains</li>
     * </ol>
     *
     * Reading: what happens when rotation is between:
     * <ol>
     * <li>1-2: Reader bridge will see that there's a new version (when asking {@link LogVersionRepository}
     * and try to open it. The log file doesn't exist yet though. The bridge can parry for this by catching
     * {@link FileNotFoundException} and tell the reader that the stream has ended</li>
     * <li>2-3: Same as (1-2)</li>
     * <li>3-4: Here the new log file exists, but the header may not be fully written yet.
     * the reader will fail when trying to read the header since it's reading it strictly and bridge
     * catches that exception, treating it the same as if the file didn't exist.</li>
     * </ol>
     *
     * @param currentLog current {@link LogVersionedStoreChannel channel} to flush and close.
     * @return the channel of the newly opened/created log file.
     * @throws IOException if an error regarding closing or opening log files occur.
     */
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
        writer.prepareForFlush().flush();
        /*
         * The log version is now in the store, flushed and persistent. If we crash
         * now, on recovery we'll attempt to open the version we're about to create
         * (but haven't yet), discover it's not there. That will lead to creating
         * the file, setting the header and continuing. We'll do just that now.
         * Note that by this point, rotation is done. The next few lines are
         * "simply overhead" for continuing to work with the new file.
         */
        PhysicalLogVersionedStoreChannel newLog = createLogChannelForVersion( newLogVersion );
        currentLog.close();
        return newLog;
    }

    /**
     * Creates a new channel for the specified version, creating the backing file if it doesn't already exist.
     * If the file exists then the header is verified to be of correct version. Having an existing file there
     * could happen after a previous crash in the middle of rotation, where the new file was created,
     * but the incremented log version changed hadn't made it to persistent storage.
     *
     * @param forVersion log version for the file/channel to create.
     * @return {@link PhysicalLogVersionedStoreChannel} for newly created/opened log file.
     * @throws IOException if there's any I/O related error.
     */
    private PhysicalLogVersionedStoreChannel createLogChannelForVersion( long forVersion ) throws IOException
    {
        File toOpen = logFiles.getLogFileForVersion( forVersion );
        StoreChannel storeChannel = fileSystem.open( toOpen, "rw" );
        LogHeader header = readLogHeader( headerBuffer, storeChannel, false, toOpen );
        if ( header == null )
        {
            // Either the header is not there in full or the file was new. Don't care
            long lastTxId = lastCommittedId.get();
            writeLogHeader( headerBuffer, forVersion, lastTxId );
            logHeaderCache.putHeader( forVersion, lastTxId );
            storeChannel.writeAll( headerBuffer );
            monitor.opened( toOpen, forVersion, lastTxId, true );
        }
        byte formatVersion = header == null ? CURRENT_LOG_VERSION : header.logFormatVersion;
        return new PhysicalLogVersionedStoreChannel( storeChannel, forVersion, formatVersion );
    }

    @Override
    public FlushablePositionAwareChannel getWriter()
    {
        return writer;
    }

    @Override
    public ReadableLogChannel getReader( LogPosition position ) throws IOException
    {
        PhysicalLogVersionedStoreChannel logChannel =
                openForVersion( logFiles, fileSystem, position.getLogVersion(), false );
        logChannel.position( position.getByteOffset() );
        return new ReadAheadLogChannel( logChannel, readerLogVersionBridge );
    }

    public static PhysicalLogVersionedStoreChannel openForVersion( PhysicalLogFiles logFiles,
            FileSystemAbstraction fileSystem,
            long version, boolean write ) throws IOException
    {
        final File fileToOpen = logFiles.getLogFileForVersion( version );

        if ( !fileSystem.fileExists( fileToOpen ) )
        {
            throw new FileNotFoundException( String.format( "File does not exist [%s]",
                    fileToOpen.getCanonicalPath() ) );
        }

        StoreChannel rawChannel = null;
        try
        {
            rawChannel = fileSystem.open( fileToOpen, write ? "rw" : "r" );

            ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
            LogHeader header = readLogHeader( buffer, rawChannel, true, fileToOpen );
            assert header != null && header.logVersion == version;
            return new PhysicalLogVersionedStoreChannel( rawChannel, version, header.logFormatVersion );
        }
        catch ( FileNotFoundException cause )
        {
            throw Exceptions.withCause( new FileNotFoundException( String.format( "File could not be opened [%s]",
                    fileToOpen.getCanonicalPath() ) ), cause );
        }
        catch ( Throwable unexpectedError )
        {
            if ( rawChannel != null )
            {
                // If we managed to open the file before failing, then close the channel
                try
                {
                    rawChannel.close();
                }
                catch ( IOException e )
                {
                    unexpectedError.addSuppressed( e );
                }
            }
            throw unexpectedError;
        }
    }

    public static PhysicalLogVersionedStoreChannel tryOpenForVersion( PhysicalLogFiles logFiles,
            FileSystemAbstraction fileSystem, long version, boolean write )
    {
        try
        {
            return openForVersion( logFiles, fileSystem, version, write );
        }
        catch ( IOException ex )
        {
            return null;
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
        long highTransactionId = lastCommittedId.get();
        while ( logFiles.versionExists( logVersion ) )
        {
            Long previousLogLastTxId = logHeaderCache.getLogHeader( logVersion );
            if ( previousLogLastTxId == null )
            {
                LogHeader header = readLogHeader( fileSystem, logFiles.getLogFileForVersion( logVersion ), false );
                if ( header != null )
                {
                    assert logVersion == header.logVersion;
                    logHeaderCache.putHeader( header.logVersion, header.lastCommittedTxId );
                    previousLogLastTxId = header.lastCommittedTxId;
                }
            }

            if ( previousLogLastTxId != null )
            {
                long lowTransactionId = previousLogLastTxId + 1;
                LogPosition position = LogPosition.start( logVersion );
                if ( !visitor.visit( position, lowTransactionId, highTransactionId ) )
                {
                    break;
                }
                highTransactionId = previousLogLastTxId;
            }
            logVersion--;
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
