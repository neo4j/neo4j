/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.log.files;

import org.eclipse.collections.api.block.procedure.primitive.LongObjectProcedure;
import org.eclipse.collections.api.map.primitive.LongObjectMap;

import java.io.Flushable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DbmsLogEntryWriterFactory;
import org.neo4j.kernel.impl.transaction.UnclosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PositionAwarePhysicalFlushableChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvents;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.configuration.GraphDatabaseSettings.transaction_log_buffer_size;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.rotation.FileLogRotation.transactionLogRotation;
import static org.neo4j.util.Preconditions.checkArgument;

/**
 * {@link LogFile} backed by one or more files in a {@link FileSystemAbstraction}.
 */
public class TransactionLogFile extends LifecycleAdapter implements LogFile
{
    private static final String TRANSACTION_LOG_FILE_ROTATION_TAG = "transactionLogFileRotation";
    private final AtomicReference<ThreadLink> threadLinkHead = new AtomicReference<>( ThreadLink.END );
    private final Lock forceLock = new ReentrantLock();
    private final AtomicLong rotateAtSize;
    private final TransactionLogFilesHelper fileHelper;
    private final TransactionLogFilesContext context;
    private final LogVersionBridge readerLogVersionBridge;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;
    private final TransactionLogFileInformation logFileInformation;
    private final TransactionLogChannelAllocator channelAllocator;
    private final DatabaseHealth databaseHealth;
    private final String baseName;
    private final LogRotation logRotation;

    private volatile PhysicalLogVersionedStoreChannel channel;
    private PositionAwarePhysicalFlushableChecksumChannel writer;
    private LogVersionRepository logVersionRepository;
    private final LogHeaderCache logHeaderCache;
    private final FileSystemAbstraction fileSystem;
    private final ConcurrentMap<Long,List<StoreChannel>> externalFileReaders = new ConcurrentHashMap<>();
    private TransactionLogWriter transactionLogWriter;

    TransactionLogFile( LogFiles logFiles, TransactionLogFilesContext context, String baseName )
    {
        this.baseName = baseName;
        this.context = context;
        this.rotateAtSize = context.getRotationThreshold();
        this.fileSystem = context.getFileSystem();
        this.databaseHealth = context.getDatabaseHealth();
        this.fileHelper = new TransactionLogFilesHelper( fileSystem, logFiles.logFilesDirectory(), baseName );
        this.logHeaderCache = new LogHeaderCache( 1000 );
        this.logFileInformation = new TransactionLogFileInformation( logFiles, logHeaderCache, context );
        this.channelAllocator = new TransactionLogChannelAllocator( context, fileHelper, logHeaderCache,
                new LogFileChannelNativeAccessor( fileSystem, context ) );
        this.readerLogVersionBridge = new ReaderLogVersionBridge( this );
        this.pageCacheTracer = context.getDatabaseTracers().getPageCacheTracer();
        this.logRotation = transactionLogRotation( this, context.getClock(), databaseHealth, context.getMonitors().newMonitor( LogRotationMonitor.class ) );
        this.memoryTracker = context.getMemoryTracker();
    }

    @Override
    public void init() throws IOException
    {
        logVersionRepository = context.getLogVersionRepository();
    }

    @Override
    public void start() throws IOException
    {
        long currentLogVersion = logVersionRepository.getCurrentLogVersion();
        channel = createLogChannelForVersion( currentLogVersion, context::getLastCommittedTransactionId );
        context.getMonitors().newMonitor( LogRotationMonitor.class ).started( channel.getPath(), currentLogVersion );

        //try to set position
        seekChannelPosition( currentLogVersion );

        writer = new PositionAwarePhysicalFlushableChecksumChannel( channel,
                new NativeScopedBuffer( context.getConfig().get( transaction_log_buffer_size ), memoryTracker ) );
        transactionLogWriter = new TransactionLogWriter( writer, new DbmsLogEntryWriterFactory( context.getKernelVersionProvider() ) );
    }

    // In order to be able to write into a logfile after life.stop during shutdown sequence
    // we will close channel and writer only during shutdown phase when all pending changes (like last
    // checkpoint) are already in
    @Override
    public void shutdown() throws IOException
    {
        IOUtils.closeAll( writer );
    }

    @Override
    public PhysicalLogVersionedStoreChannel openForVersion( long version ) throws IOException
    {
        return openForVersion( version, false );
    }

    @Override
    public PhysicalLogVersionedStoreChannel openForVersion( long version, boolean raw ) throws IOException
    {
        return channelAllocator.openLogChannel( version, raw );
    }

    /**
     * Creates a new channel for the specified version, creating the backing file if it doesn't already exist.
     * If the file exists then the header is verified to be of correct version. Having an existing file there
     * could happen after a previous crash in the middle of rotation, where the new file was created,
     * but the incremented log version changed hadn't made it to persistent storage.
     *
     * @param version log version for the file/channel to create.
     * @param lastTransactionIdSupplier supplier of last transaction id that was written into previous log file
     * @return {@link PhysicalLogVersionedStoreChannel} for newly created/opened log file.
     * @throws IOException if there's any I/O related error.
     */
    @Override
    public PhysicalLogVersionedStoreChannel createLogChannelForVersion( long version, LongSupplier lastTransactionIdSupplier ) throws IOException
    {
        return channelAllocator.createLogChannel( version, lastTransactionIdSupplier );
    }

    @Override
    public boolean rotationNeeded() throws IOException
    {
        return writer.getCurrentPosition().getByteOffset() >= rotateAtSize.get();
    }

    @Override
    public void truncate() throws IOException
    {
        truncate( writer.getCurrentPosition() );
    }

    @Override
    public synchronized void truncate( LogPosition targetPosition ) throws IOException
    {
        long currentVersion = writer.getCurrentPosition().getLogVersion();
        long targetVersion = targetPosition.getLogVersion();
        if ( currentVersion < targetVersion )
        {
            throw new IllegalArgumentException( "Log position requested for restore points to the log file that is higher than " +
                    "existing available highest log file. Requested restore position: " + targetPosition + ", " +
                    "current log file version: " + currentVersion + "." );
        }

        LogPosition lastClosed = context.getLastClosedTransactionPosition();
        if ( isCoveredByCommittedTransaction( targetPosition, targetVersion, lastClosed ) )
        {
            throw new IllegalArgumentException( "Log position requested to be used for restore belongs to the log file that " +
                    "was already appended by transaction and cannot be restored. " +
                    "Last closed position: " + lastClosed + ", requested restore: " + targetPosition );
        }

        writer.prepareForFlush().flush();
        if ( currentVersion != targetVersion )
        {
            var oldChannel = channel;
            channel = createLogChannelForVersion( targetVersion, context::committingTransactionId );
            writer.setChannel( channel );
            oldChannel.close();

            // delete newer files
            for ( long i = currentVersion; i > targetVersion; i-- )
            {
                fileSystem.deleteFile( fileHelper.getLogFileForVersion( i ) );
            }
        }

        //truncate current file
        channel.truncate( targetPosition.getByteOffset() );
        channel.position( channel.size() );
    }

    @Override
    public synchronized LogPosition append( ByteBuffer byteBuffer, OptionalLong transactionId ) throws IOException
    {
        checkArgument( byteBuffer.isDirect(), "Its is required for byte buffer to be direct." );
        var transactionLogWriter = getTransactionLogWriter();

        try ( var logAppend = context.getDatabaseTracers().getDatabaseTracer().logAppend() )
        {
            if ( transactionId.isPresent() )
            {
                logRotation.batchedRotateLogIfNeeded( logAppend, transactionId.getAsLong() - 1 );
            }
            var logPositionBefore = transactionLogWriter.getCurrentPosition();
            transactionLogWriter.append( byteBuffer );
            var logPositionAfter = transactionLogWriter.getCurrentPosition();
            logAppend.appendToLogFile( logPositionBefore, logPositionAfter );
            return logPositionBefore;
        }
    }

    @Override
    public synchronized Path rotate() throws IOException
    {
        return rotate( context::committingTransactionId );
    }

    public synchronized Path rotate( long lastTransactionId ) throws IOException
    {
        return rotate( () -> lastTransactionId );
    }

    @Override
    public LogRotation getLogRotation()
    {
        return logRotation;
    }

    @Override
    public TransactionLogWriter getTransactionLogWriter()
    {
        return transactionLogWriter;
    }

    @Override
    public void flush() throws IOException
    {
        writer.prepareForFlush().flush();
    }

    @Override
    public ReadableLogChannel getReader( LogPosition position ) throws IOException
    {
        return getReader( position, readerLogVersionBridge );
    }

    @Override
    public ReadableLogChannel getRawReader( LogPosition position ) throws IOException
    {
        return getReader( position, readerLogVersionBridge, true );
    }

    @Override
    public ReadableLogChannel getReader( LogPosition position, LogVersionBridge logVersionBridge ) throws IOException
    {
        return getReader( position, logVersionBridge, false );
    }

    private ReadableLogChannel getReader( LogPosition position, LogVersionBridge logVersionBridge, boolean raw ) throws IOException
    {
        PhysicalLogVersionedStoreChannel logChannel = openForVersion( position.getLogVersion(), raw );
        logChannel.position( position.getByteOffset() );
        return new ReadAheadLogChannel( logChannel, logVersionBridge, memoryTracker, raw );
    }

    @Override
    public void accept( LogFileVisitor visitor, LogPosition startingFromPosition ) throws IOException
    {
        try ( ReadableLogChannel reader = getReader( startingFromPosition ) )
        {
            visitor.visit( reader );
        }
    }

    @Override
    public TransactionLogFileInformation getLogFileInformation()
    {
        return logFileInformation;
    }

    @Override
    public long getLogVersion( Path file )
    {
        return TransactionLogFilesHelper.getLogVersion( file );
    }

    @Override
    public Path getLogFileForVersion( long version )
    {
        return fileHelper.getLogFileForVersion( version );
    }

    @Override
    public Path getHighestLogFile()
    {
        return getLogFileForVersion( getHighestLogVersion() );
    }

    @Override
    public boolean versionExists( long version )
    {
        return fileSystem.fileExists( getLogFileForVersion( version ) );
    }

    @Override
    public LogHeader extractHeader( long version ) throws IOException
    {
        return extractHeader( version, true );
    }

    @Override
    public boolean hasAnyEntries( long version )
    {
        try
        {
            Path logFile = getLogFileForVersion( version );
            var logHeader = extractHeader( version, false );
            if ( logHeader == null )
            {
                return false;
            }
            int headerSize = Math.toIntExact( logHeader.getStartPosition().getByteOffset() );
            if ( fileSystem.getFileSize( logFile ) <= headerSize )
            {
                return false;
            }
            try ( StoreChannel channel = fileSystem.read( logFile ) )
            {
                try ( var scopedBuffer = new HeapScopedBuffer( headerSize + 1, context.getMemoryTracker() ) )
                {
                    var buffer = scopedBuffer.getBuffer();
                    channel.readAll( buffer );
                    buffer.flip();
                    return buffer.get( headerSize ) != 0;
                }
            }
        }
        catch ( IOException e )
        {
            return false;
        }
    }

    @Override
    public long getCurrentLogVersion()
    {
        if ( logVersionRepository != null )
        {
            return logVersionRepository.getCurrentLogVersion();
        }
        return getHighestLogVersion();
    }

    @Override
    public long getHighestLogVersion()
    {
        RangeLogVersionVisitor visitor = new RangeLogVersionVisitor();
        accept( visitor );
        return visitor.getHighestVersion();
    }

    @Override
    public long getLowestLogVersion()
    {
        RangeLogVersionVisitor visitor = new RangeLogVersionVisitor();
        accept( visitor );
        return visitor.getLowestVersion();
    }

    @Override
    public void accept( LogVersionVisitor visitor )
    {
        try
        {
            for ( Path file : fileHelper.getMatchedFiles() )
            {
                visitor.visit( file, getLogVersion( file ) );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void accept( LogHeaderVisitor visitor ) throws IOException
    {
        // Start from the where we're currently at and go backwards in time (versions)
        long logVersion = getHighestLogVersion();
        long highTransactionId = context.getLastCommittedTransactionId();
        while ( versionExists( logVersion ) )
        {
            LogHeader logHeader = extractHeader( logVersion, false );
            if ( logHeader != null )
            {
                long lowTransactionId = logHeader.getLastCommittedTxId() + 1;
                LogPosition position = logHeader.getStartPosition();
                if ( !visitor.visit( logHeader, position, lowTransactionId, highTransactionId ) )
                {
                    break;
                }
                highTransactionId = logHeader.getLastCommittedTxId();
            }
            logVersion--;
        }
    }

    @Override
    public Path[] getMatchedFiles() throws IOException
    {
        return fileHelper.getMatchedFiles();
    }

    @Override
    public void combine( Path additionalLogFilesDirectory ) throws IOException
    {
        long highestLogVersion = getHighestLogVersion();
        var logHelper = new TransactionLogFilesHelper( fileSystem, additionalLogFilesDirectory, baseName );
        for ( Path matchedFile : logHelper.getMatchedFiles() )
        {
            long newFileVersion = ++highestLogVersion;
            Path newFileName = fileHelper.getLogFileForVersion( newFileVersion );
            fileSystem.renameFile( matchedFile, newFileName );
            try ( StoreChannel channel = fileSystem.write( newFileName ) )
            {
                LogHeader logHeader = readLogHeader( fileSystem, newFileName, memoryTracker );
                LogHeader writeHeader = new LogHeader( logHeader, newFileVersion );
                LogHeaderWriter.writeLogHeader( channel, writeHeader, memoryTracker );
            }
        }
    }

    /**
     * Called by the appender that just appended a transaction to the log.
     *
     * @param logForceEvents A trace event for the given log append operation.
     * @return {@code true} if we got lucky and were the ones forcing the log.
     */
    @Override
    public boolean forceAfterAppend( LogForceEvents logForceEvents ) throws IOException
    {
        // There's a benign race here, where we add our link before we update our next pointer.
        // This is okay, however, because unparkAll() spins when it sees a null next pointer.
        ThreadLink threadLink = new ThreadLink( Thread.currentThread() );
        threadLink.next = threadLinkHead.getAndSet( threadLink );
        boolean attemptedForce = false;

        try ( LogForceWaitEvent logForceWaitEvent = logForceEvents.beginLogForceWait() )
        {
            do
            {
                if ( forceLock.tryLock() )
                {
                    attemptedForce = true;
                    try
                    {
                        forceLog( logForceEvents );
                        // In the event of any failure a database panic will be raised and thrown here
                    }
                    finally
                    {
                        forceLock.unlock();

                        // We've released the lock, so unpark anyone who might have decided park while we were working.
                        // The most recently parked thread is the one most likely to still have warm caches, so that's
                        // the one we would prefer to unpark. Luckily, the stack nature of the ThreadLinks makes it easy
                        // to get to.
                        ThreadLink nextWaiter = threadLinkHead.get();
                        nextWaiter.unpark();
                    }
                }
                else
                {
                    waitForLogForce();
                }
            }
            while ( !threadLink.done );

            // If there were many threads committing simultaneously and I wasn't the lucky one
            // actually doing the forcing (where failure would throw panic exception) I need to
            // explicitly check if everything is OK before considering this transaction committed.
            if ( !attemptedForce )
            {
                databaseHealth.assertHealthy( IOException.class );
            }
        }
        return attemptedForce;
    }

    @Override
    public void locklessForce( LogForceEvents logForceEvents ) throws IOException
    {
        try ( LogForceEvent logForceEvent = logForceEvents.beginLogForce() )
        {
            databaseHealth.assertHealthy( IOException.class );
            flush();
        }
        catch ( final Throwable panic )
        {
            databaseHealth.panic( panic );
            throw panic;
        }
    }

    @Override
    public void registerExternalReaders( LongObjectMap<StoreChannel> internalChannels )
    {
        internalChannels.forEachKeyValue( (LongObjectProcedure<StoreChannel>) ( version, channel ) ->
                        externalFileReaders.computeIfAbsent( version, any -> new CopyOnWriteArrayList<>() ).add( channel ) );
    }

    @Override
    public void unregisterExternalReader( long version, StoreChannel channel )
    {
        externalFileReaders.computeIfPresent( version, ( aLong, storeChannels ) ->
        {
            storeChannels.remove( channel );
            if ( storeChannels.isEmpty() )
            {
                return null;
            }
            return storeChannels;
        } );
    }

    @Override
    public void terminateExternalReaders( long maxDeletedVersion )
    {
        externalFileReaders.entrySet().removeIf( entry ->
        {
            if ( entry.getKey() <= maxDeletedVersion )
            {
                IOUtils.closeAllSilently( entry.getValue() );
                return true;
            }
            return false;
        } );
    }

    @VisibleForTesting
    public ConcurrentMap<Long,List<StoreChannel>> getExternalFileReaders()
    {
        return externalFileReaders;
    }

    private synchronized Path rotate( LongSupplier committedTransactIdSupplier ) throws IOException
    {
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( TRANSACTION_LOG_FILE_ROTATION_TAG ) ) )
        {
            channel = rotate( channel, cursorContext, committedTransactIdSupplier );
            writer.setChannel( channel );
            return channel.getPath();
        }
    }

    /**
     * Rotates the current log file, continuing into next (version) log file.
     * This method must be recovery safe, which means a crash at any point should be recoverable.
     * Concurrent readers must also be able to parry for concurrent rotation.
     * Concurrent writes will not be an issue since rotation and writing contends on the same monitor.
     *
     * Steps during rotation are:
     * <ol>
     * <li>1: Increment log version, {@link LogVersionRepository#incrementAndGetVersion(CursorContext)} (also flushes the store)</li>
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
     * header will be set.</li>
     * </ol>
     *
     * Reading: what happens when rotation is between:
     * <ol>
     * <li>1-2: Reader bridge will see that there's a new version (when asking {@link LogVersionRepository}
     * and try to open it. The log file doesn't exist yet though. The bridge can parry for this by catching
     * {@link NoSuchFileException} and tell the reader that the stream has ended</li>
     * <li>2-3: Same as (1-2)</li>
     * <li>3-4: Here the new log file exists, but the header may not be fully written yet.
     * the reader will fail when trying to read the header since it's reading it strictly and bridge
     * catches that exception, treating it the same as if the file didn't exist.</li>
     * </ol>
     *
     * @param currentLog current {@link LogVersionedStoreChannel channel} to flush and close.
     * @param cursorContext underlying page cursor context.
     * @return the channel of the newly opened/created log file.
     * @throws IOException if an error regarding closing or opening log files occur.
     */
    private PhysicalLogVersionedStoreChannel rotate( LogVersionedStoreChannel currentLog, CursorContext cursorContext, LongSupplier lastTransactionIdSupplier )
            throws IOException
    {
        /*
         * The store is now flushed. If we fail now the recovery code will open the
         * current log file and replay everything. That's unnecessary but totally ok.
         */
        long newLogVersion = logVersionRepository.incrementAndGetVersion( cursorContext );

        /*
         * Rotation can happen at any point, although not concurrently with an append,
         * although an append may have (most likely actually) left at least some bytes left
         * in the buffer for future flushing. Flushing that buffer now makes the last appended
         * transaction complete in the log we're rotating away. Awesome.
         */
        writer.prepareForFlush().flush();
        currentLog.truncate( currentLog.position() );

        /*
         * The log version is now in the store, flushed and persistent. If we crash
         * now, on recovery we'll attempt to open the version we're about to create
         * (but haven't yet), discover it's not there. That will lead to creating
         * the file, setting the header and continuing.
         * We using committing transaction id as a source of last transaction id here since
         * we can have transactions that are not yet published as committed but were already stored
         * into transaction log that was just rotated.
         */
        PhysicalLogVersionedStoreChannel newLog = createLogChannelForVersion( newLogVersion, lastTransactionIdSupplier );
        currentLog.close();
        return newLog;
    }

    private static boolean isCoveredByCommittedTransaction( LogPosition targetPosition, long targetVersion, LogPosition lastClosed )
    {
        return lastClosed.getLogVersion() > targetVersion ||
                lastClosed.getLogVersion() == targetVersion && lastClosed.getByteOffset() > targetPosition.getByteOffset();
    }

    private void seekChannelPosition( long currentLogVersion ) throws IOException
    {
        jumpToTheLastClosedTxPosition( currentLogVersion );
        LogPosition position;
        try
        {
            position = scanToEndOfLastLogEntry();
        }
        catch ( Exception e )
        {
            // If we can't read the log, it could be that the last-closed-transaction position in the meta-data store is wrong.
            // We can try again by scanning the log file from the start.
            jumpToLogStart( currentLogVersion );
            try
            {
                position = scanToEndOfLastLogEntry();
            }
            catch ( Exception exception )
            {
                exception.addSuppressed( e );
                throw exception;
            }
        }
        channel.position( position.getByteOffset() );
    }

    private LogPosition scanToEndOfLastLogEntry() throws IOException
    {
        // scroll all over possible checkpoints
        try ( ReadAheadLogChannel readAheadLogChannel = new ReadAheadLogChannel( new UnclosableChannel( channel ), memoryTracker ) )
        {
            LogEntryReader logEntryReader = context.getLogEntryReader();
            LogEntry entry;
            do
            {
                // seek to the end the records.
                entry = logEntryReader.readLogEntry( readAheadLogChannel );
            }
            while ( entry != null );
            return logEntryReader.lastPosition();
        }
    }

    private void jumpToTheLastClosedTxPosition( long currentLogVersion ) throws IOException
    {
        LogPosition logPosition = context.getLastClosedTransactionPosition();
        long lastTxOffset = logPosition.getByteOffset();
        long lastTxLogVersion = logPosition.getLogVersion();
        long headerSize = extractHeader( currentLogVersion ).getStartPosition().getByteOffset();
        if ( lastTxOffset < headerSize || channel.size() < lastTxOffset )
        {
            return;
        }
        if ( lastTxLogVersion == currentLogVersion )
        {
            channel.position( lastTxOffset );
        }
    }

    private void jumpToLogStart( long currentLogVersion ) throws IOException
    {
        long headerSize = extractHeader( currentLogVersion ).getStartPosition().getByteOffset();
        channel.position( headerSize );
    }

    private LogHeader extractHeader( long version, boolean strict ) throws IOException
    {
        LogHeader logHeader = logHeaderCache.getLogHeader( version );
        if ( logHeader == null )
        {
            logHeader = readLogHeader( fileSystem, getLogFileForVersion( version ), strict, context.getMemoryTracker() );
            if ( !strict && logHeader == null )
            {
                return null;
            }
            logHeaderCache.putHeader( version, logHeader );
        }

        return logHeader;
    }

    private void forceLog( LogForceEvents logForceEvents ) throws IOException
    {
        ThreadLink links = threadLinkHead.getAndSet( ThreadLink.END );
        try ( LogForceEvent logForceEvent = logForceEvents.beginLogForce() )
        {
            force();
        }
        catch ( final Throwable panic )
        {
            databaseHealth.panic( panic );
            throw panic;
        }
        finally
        {
            unparkAll( links );
        }
    }

    private static void unparkAll( ThreadLink links )
    {
        do
        {
            links.done = true;
            links.unpark();
            ThreadLink tmp;
            do
            {
                // Spin because of the race:y update when consing.
                tmp = links.next;
            }
            while ( tmp == null );
            links = tmp;
        }
        while ( links != ThreadLink.END );
    }

    private void waitForLogForce()
    {
        long parkTime = TimeUnit.MILLISECONDS.toNanos( 100 );
        LockSupport.parkNanos( this, parkTime );
    }

    private void force() throws IOException
    {
        // Empty buffer into writer. We want to synchronize with appenders somehow so that they
        // don't append while we're doing that. The way rotation is coordinated we can't synchronize
        // on logFile because it would cause deadlocks. Synchronizing on writer assumes that appenders
        // also synchronize on writer.
        Flushable flushable;
        synchronized ( this )
        {
            databaseHealth.assertHealthy( IOException.class );
            flushable = writer.prepareForFlush();
        }
        // Force the writer outside of the lock.
        // This allows other threads access to the buffer while the writer is being forced.
        try
        {
            flushable.flush();
        }
        catch ( ClosedChannelException ignored )
        {
            // This is ok, we were already successful in emptying the buffer, so the channel being closed here means
            // that some other thread is rotating the log and has closed the underlying channel. But since we were
            // successful in emptying the buffer *UNDER THE LOCK* we know that the rotating thread included the changes
            // we emptied into the channel, and thus it is already flushed by that thread.
        }
    }
}
