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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.recovery.LogTailScannerMonitor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static java.lang.Math.min;
import static java.lang.Math.subtractExact;
import static java.lang.String.format;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.fs.FileUtils.getCanonicalFile;
import static org.neo4j.kernel.impl.transaction.log.files.RangeLogVersionVisitor.UNKNOWN;

public abstract class AbstractLogTailScanner
{
    static final long NO_TRANSACTION_ID = -1;
    protected final LogFiles logFiles;
    protected final LogEntryReader logEntryReader;
    protected final LogTailScannerMonitor monitor;
    protected final Log log;
    protected final MemoryTracker memoryTracker;
    private LogTailInformation logTailInformation;

    AbstractLogTailScanner( LogFiles logFiles,
            LogEntryReader logEntryReader, Monitors monitors,
            LogProvider log, MemoryTracker memoryTracker )
    {
        this.logFiles = logFiles;
        this.logEntryReader = logEntryReader;
        this.monitor = monitors.newMonitor( LogTailScannerMonitor.class );
        this.log = log.getLog( getClass() );
        this.memoryTracker = memoryTracker;
    }

    protected abstract LogTailInformation findLogTail() throws IOException;

    protected void verifyReaderPosition( long version, LogPosition logPosition ) throws IOException
    {
        LogFile logFile = logFiles.getLogFile();
        long highestLogVersion = logFile.getHighestLogVersion();
        try ( var channel = logFile.openForVersion( version ) )
        {
            verifyLogVersion( version, logPosition );
            long logFileSize = channel.size();
            long channelLeftovers = subtractExact( logFileSize, logPosition.getByteOffset() );
            if ( channelLeftovers != 0 )
            {
                // channel has more data than entry reader can read. Only one valid case for this kind of situation is
                // pre-allocated log file that has some space left

                // if this log file is not the last one and we have some unreadable bytes in the end its an indication of corrupted log files
                verifyLastFile( highestLogVersion, version, logPosition, logFileSize, channelLeftovers );

                // to double check that even when we encountered end of records position we do not have anything after that
                // we will try to read some data (up to 12K) in advance to check that only zero's are available there
                verifyNoMoreReadableDataAvailable( version, channel, logPosition, channelLeftovers );
            }
        }
    }

    private void verifyLogVersion( long version, LogPosition logPosition )
    {
        if ( logPosition.getLogVersion() != version )
        {
            throw new IllegalStateException( format( "Expected to observe log positions only for log file with version %d but encountered " +
                            "version %d while reading %s.", version, logPosition.getLogVersion(),
                    getCanonicalFile( logFiles.getLogFile().getLogFileForVersion( version ) ) ) );
        }
    }

    static void throwUnableToCleanRecover( Throwable t )
    {
        throw new RuntimeException(
                "Error reading transaction logs, recovery not possible. To force the database to start anyway, you can specify '" +
                        GraphDatabaseInternalSettings.fail_on_corrupted_log_files.name() + "=false'. This will try to recover as much " +
                        "as possible and then truncate the corrupt part of the transaction log. Doing this means your database " +
                        "integrity might be compromised, please consider restoring from a consistent backup instead.", t );
    }

    private void verifyLastFile( long highestLogVersion, long version, LogPosition logPosition, long logFileSize, long channelLeftovers )
    {
        if ( version != highestLogVersion )
        {
            throw new RuntimeException(
                    format( "Transaction log files with version %d has %d unreadable bytes. Was able to read upto %d but %d is available.",
                            version, channelLeftovers, logPosition.getByteOffset(), logFileSize ) );
        }
    }

    private void verifyNoMoreReadableDataAvailable( long version, LogVersionedStoreChannel channel, LogPosition logPosition, long channelLeftovers )
            throws IOException
    {
        long initialPosition = channel.position();
        try
        {
            channel.position( logPosition.getByteOffset() );
            try ( var scopedBuffer = new HeapScopedBuffer( safeCastLongToInt( min( kibiBytes( 12 ), channelLeftovers ) ), memoryTracker ) )
            {
                ByteBuffer byteBuffer = scopedBuffer.getBuffer();
                channel.readAll( byteBuffer );
                byteBuffer.flip();
                if ( !isAllZerosBuffer( byteBuffer ) )
                {
                    throw new RuntimeException( format( "Transaction log files with version %d has some data available after last readable log entry. " +
                                    "Last readable position %d, read ahead buffer content: %s.", version, logPosition.getByteOffset(),
                            dumpBufferToString( byteBuffer ) ) );
                }
            }
        }
        finally
        {
            channel.position( initialPosition );
        }
    }

    LogTailInformation checkpointTailInformation( long highestLogVersion, LogEntryStart latestStartEntry,
            long oldestVersionFound, byte latestLogEntryVersion, CheckpointInfo latestCheckPoint,
            boolean corruptedTransactionLogs, StoreId storeId ) throws IOException
    {
        LogPosition checkPointLogPosition = latestCheckPoint.getTransactionLogPosition();
        InlinedLogTailScanner.ExtractedTransactionRecord transactionRecord = extractFirstTxIdAfterPosition( checkPointLogPosition, highestLogVersion );
        long firstTxIdAfterPosition = transactionRecord.getId();
        boolean startRecordAfterCheckpoint = (firstTxIdAfterPosition != NO_TRANSACTION_ID) ||
                ((latestStartEntry != null) &&
                        (latestStartEntry.getStartPosition().compareTo( latestCheckPoint.getTransactionLogPosition() ) >= 0));
        boolean corruptedLogs = transactionRecord.isFailure() || corruptedTransactionLogs;
        return new LogTailInformation( latestCheckPoint, corruptedLogs || startRecordAfterCheckpoint,
                firstTxIdAfterPosition, oldestVersionFound == UNKNOWN, highestLogVersion, latestLogEntryVersion, storeId );
    }

    /**
     * Extracts txId from first commit entry, when starting reading at the given {@code position}.
     * If no commit entry found in the version, the reader will continue into next version(s) up till
     * {@code maxLogVersion} until finding one.
     *
     * @param initialPosition {@link LogPosition} to start scan from.
     * @param maxLogVersion max log version to scan.
     * @return value object that contains first transaction id of closes commit entry to {@code initialPosition},
     * or {@link InlinedLogTailScanner#NO_TRANSACTION_ID} if not found. And failure flag that will be set to true if
     * there was some exception during transaction log processing.
     * @throws IOException on channel close I/O error.
     */
    protected ExtractedTransactionRecord extractFirstTxIdAfterPosition( LogPosition initialPosition, long maxLogVersion ) throws IOException
    {
        long initialVersion = initialPosition.getLogVersion();
        long logVersion = initialVersion;
        var logFile = logFiles.getLogFile();
        while ( logVersion <= maxLogVersion )
        {
            if ( logFile.versionExists( logVersion ) )
            {
                LogPosition currentPosition = logVersion != initialVersion ? logFile.extractHeader( logVersion ).getStartPosition() : initialPosition;
                try ( LogVersionedStoreChannel storeChannel = logFile.openForVersion( logVersion ) )
                {
                    storeChannel.position( currentPosition.getByteOffset() );
                    try ( LogEntryCursor cursor = new LogEntryCursor( logEntryReader, new ReadAheadLogChannel( storeChannel, memoryTracker ) ) )
                    {
                        while ( cursor.next() )
                        {
                            LogEntry entry = cursor.get();
                            if ( entry instanceof LogEntryCommit )
                            {
                                return new InlinedLogTailScanner.ExtractedTransactionRecord( ((LogEntryCommit) entry).getTxId() );
                            }
                        }
                    }
                }
                catch ( Throwable t )
                {
                    monitor.corruptedLogFile( currentPosition.getLogVersion(), t );
                    return new InlinedLogTailScanner.ExtractedTransactionRecord( true );
                }
                logVersion = currentPosition.getLogVersion() + 1;
            }
            else
            {
                logVersion += 1;
            }
        }
        return new InlinedLogTailScanner.ExtractedTransactionRecord();
    }

    /**
     * Collects information about the tail of the transaction log, i.e. last checkpoint, last entry etc.
     * Since this is an expensive task we do it once and reuse the result. This method is thus lazy and the first one
     * calling it will take the hit.
     * <p>
     * This is only intended to be used during startup. If you need to track the state of the tail, that can be done more
     * efficiently at runtime, and this method should then only be used to restore said state.
     *
     * @return snapshot of the state of the transaction logs tail at startup.
     * @throws UnderlyingStorageException if any errors occurs while parsing the transaction logs
     */
    public LogTailInformation getTailInformation() throws UnderlyingStorageException
    {
        if ( logTailInformation == null )
        {
            try
            {
                logTailInformation = findLogTail();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Error encountered while parsing transaction logs", e );
            }
        }

        return logTailInformation;
    }

    private static String dumpBufferToString( ByteBuffer byteBuffer )
    {
        byte[] data = new byte[byteBuffer.limit()];
        byteBuffer.get( data );
        return Arrays.toString( data );
    }

    private static boolean isAllZerosBuffer( ByteBuffer byteBuffer )
    {
        if ( byteBuffer.hasArray() )
        {
            byte[] array = byteBuffer.array();
            for ( byte b : array )
            {
                if ( b != 0 )
                {
                    return false;
                }
            }
        }
        else
        {
            while ( byteBuffer.hasRemaining() )
            {
                if ( byteBuffer.get() != 0 )
                {
                    return false;
                }
            }
        }
        return true;
    }

    static class ExtractedTransactionRecord
    {
        private final long id;
        private final boolean failure;

        ExtractedTransactionRecord()
        {
            this( NO_TRANSACTION_ID, false );
        }

        ExtractedTransactionRecord( long txId )
        {
            this( txId, false );
        }

        ExtractedTransactionRecord( boolean failure )
        {
            this( NO_TRANSACTION_ID, failure );
        }

        private ExtractedTransactionRecord( long txId, boolean failure )
        {
            this.id = txId;
            this.failure = failure;
        }

        public long getId()
        {
            return id;
        }

        public boolean isFailure()
        {
            return failure;
        }
    }
}
