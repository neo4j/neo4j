/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.recovery;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;

import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.kernel.recovery.Recovery.throwUnableToCleanRecover;

/**
 * This class collects information about the latest entries in the transaction log. Since the only way we have to collect
 * said information is to scan the transaction log from beginning to end, which is costly, we do this once and save the
 * result for others to consume.
 * <p>
 * Due to the nature of transaction logs and log rotation, a single transaction log file has to be scanned forward, and
 * if the required data is not found we search backwards through log file versions.
 */
public class LogTailScanner
{
    static long NO_TRANSACTION_ID = -1;
    private final LogFiles logFiles;
    private final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader;
    private LogTailInformation logTailInformation;
    private final LogTailScannerMonitor monitor;
    private final boolean failOnCorruptedLogFiles;

    public LogTailScanner( LogFiles logFiles,
            LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader, Monitors monitors )
    {
        this( logFiles, logEntryReader, monitors, false );
    }

    public LogTailScanner( LogFiles logFiles,
            LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader, Monitors monitors,
            boolean failOnCorruptedLogFiles )
    {
        this.logFiles = logFiles;
        this.logEntryReader = logEntryReader;
        this.monitor = monitors.newMonitor( LogTailScannerMonitor.class );
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
    }

    private LogTailInformation findLogTail() throws IOException
    {
        final long highestLogVersion = logFiles.getHighestLogVersion();
        long version = highestLogVersion;
        long versionToSearchForCommits = highestLogVersion;
        LogEntryStart latestStartEntry = null;
        long oldestStartEntryTransaction = -1;
        long oldestVersionFound = -1;
        LogEntryVersion latestLogEntryVersion = null;
        boolean startRecordAfterCheckpoint = false;
        boolean corruptedTransactionLogs = false;

        while ( version >= logFiles.getLowestLogVersion() && version >= INITIAL_LOG_VERSION )
        {
            oldestVersionFound = version;
            CheckPoint latestCheckPoint = null;
            try ( LogVersionedStoreChannel channel = logFiles.openForVersion( version );
                  ReadAheadLogChannel readAheadLogChannel = new ReadAheadLogChannel( channel );
                  LogEntryCursor cursor = new LogEntryCursor( logEntryReader, readAheadLogChannel ) )
            {
                LogEntry entry;
                long maxEntryReadPosition = 0;
                while ( cursor.next() )
                {
                    entry = cursor.get();

                    // Collect data about latest checkpoint
                    if ( entry instanceof CheckPoint )
                    {
                        latestCheckPoint = entry.as();
                    }
                    else if ( entry instanceof LogEntryCommit )
                    {
                        if ( oldestStartEntryTransaction == NO_TRANSACTION_ID )
                        {
                            oldestStartEntryTransaction = ((LogEntryCommit) entry).getTxId();
                        }
                    }
                    else if ( entry instanceof LogEntryStart )
                    {
                        LogEntryStart startEntry = entry.as();
                        if ( version == versionToSearchForCommits )
                        {
                            latestStartEntry = startEntry;
                        }
                        startRecordAfterCheckpoint = true;
                    }

                    // Collect data about latest entry version, only in first log file
                    if ( version == versionToSearchForCommits || latestLogEntryVersion == null )
                    {
                        latestLogEntryVersion = entry.getVersion();
                    }
                    maxEntryReadPosition = readAheadLogChannel.position();
                }
                if ( hasUnreadableBytes( channel, maxEntryReadPosition ) )
                {
                    corruptedTransactionLogs = true;
                }
            }
             catch ( Error | ClosedByInterruptException e )
            {
                // These should not be parsing errors
                throw e;
            }
            catch ( Throwable t )
            {
                monitor.corruptedLogFile( version, t );
                if ( failOnCorruptedLogFiles )
                {
                    throwUnableToCleanRecover( t );
                }
                corruptedTransactionLogs = true;
            }

            if ( latestCheckPoint != null )
            {
                return checkpointTailInformation( highestLogVersion, latestStartEntry, oldestVersionFound,
                        latestLogEntryVersion, latestCheckPoint, corruptedTransactionLogs );
            }

            version--;

            // if we have found no commits in the latest log, keep searching in the next one
            if ( latestStartEntry == null )
            {
                versionToSearchForCommits--;
            }
        }

        return new LogTailInformation( corruptedTransactionLogs || startRecordAfterCheckpoint,
                oldestStartEntryTransaction, oldestVersionFound, highestLogVersion, latestLogEntryVersion );
    }

    private boolean hasUnreadableBytes( LogVersionedStoreChannel channel, long maxEntryReadEndPosition ) throws IOException
    {
        return channel.position() > maxEntryReadEndPosition;
    }

    protected LogTailInformation checkpointTailInformation( long highestLogVersion, LogEntryStart latestStartEntry,
            long oldestVersionFound, LogEntryVersion latestLogEntryVersion, CheckPoint latestCheckPoint,
            boolean corruptedTransactionLogs ) throws IOException
    {
        LogPosition checkPointLogPosition = latestCheckPoint.getLogPosition();
        ExtractedTransactionRecord transactionRecord = extractFirstTxIdAfterPosition( checkPointLogPosition, highestLogVersion );
        long firstTxIdAfterPosition = transactionRecord.getId();
        boolean startRecordAfterCheckpoint = (firstTxIdAfterPosition != NO_TRANSACTION_ID) ||
                ((latestStartEntry != null) &&
                        (latestStartEntry.getStartPosition().compareTo( latestCheckPoint.getLogPosition() ) >= 0));
        boolean corruptedLogs = transactionRecord.isFailure() || corruptedTransactionLogs;
        return new LogTailInformation( latestCheckPoint, corruptedLogs || startRecordAfterCheckpoint,
                firstTxIdAfterPosition, oldestVersionFound, highestLogVersion, latestLogEntryVersion );
    }

    /**
     * Extracts txId from first commit entry, when starting reading at the given {@code position}.
     * If no commit entry found in the version, the reader will continue into next version(s) up till
     * {@code maxLogVersion} until finding one.
     *
     * @param initialPosition {@link LogPosition} to start scan from.
     * @param maxLogVersion max log version to scan.
     * @return value object that contains first transaction id of closes commit entry to {@code initialPosition},
     * or {@link LogTailInformation#NO_TRANSACTION_ID} if not found. And failure flag that will be set to true if
     * there was some exception during transaction log processing.
     * @throws IOException on channel close I/O error.
     */
    protected ExtractedTransactionRecord extractFirstTxIdAfterPosition( LogPosition initialPosition, long maxLogVersion ) throws IOException
    {
        LogPosition currentPosition = initialPosition;
        while ( currentPosition.getLogVersion() <= maxLogVersion )
        {
            LogVersionedStoreChannel storeChannel = tryOpenStoreChannel( currentPosition );
            if ( storeChannel != null )
            {
                try
                {
                    storeChannel.position( currentPosition.getByteOffset() );
                    try ( ReadAheadLogChannel logChannel = new ReadAheadLogChannel( storeChannel );
                            LogEntryCursor cursor = new LogEntryCursor( logEntryReader, logChannel ) )
                    {
                        while ( cursor.next() )
                        {
                            LogEntry entry = cursor.get();
                            if ( entry instanceof LogEntryCommit )
                            {
                                return new ExtractedTransactionRecord( ((LogEntryCommit) entry).getTxId() );
                            }
                        }
                    }
                }
                catch ( Throwable t )
                {
                    monitor.corruptedLogFile( currentPosition.getLogVersion(), t );
                    return new ExtractedTransactionRecord( true );
                }
                finally
                {
                    storeChannel.close();
                }
            }

            currentPosition = LogPosition.start( currentPosition.getLogVersion() + 1 );
        }
        return new ExtractedTransactionRecord();
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

    private PhysicalLogVersionedStoreChannel tryOpenStoreChannel( LogPosition currentPosition )
    {
        try
        {
            return logFiles.openForVersion( currentPosition.getLogVersion() );
        }
        catch ( IOException e )
        {
            return null;
        }
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

    public static class LogTailInformation
    {

        public final CheckPoint lastCheckPoint;
        public final long firstTxIdAfterLastCheckPoint;
        public final long oldestLogVersionFound;
        public final long currentLogVersion;
        public final LogEntryVersion latestLogEntryVersion;
        private final boolean recordAfterCheckpoint;

        public LogTailInformation( boolean recordAfterCheckpoint, long firstTxIdAfterLastCheckPoint,
                long oldestLogVersionFound, long currentLogVersion,
                LogEntryVersion latestLogEntryVersion )
        {
            this( null, recordAfterCheckpoint, firstTxIdAfterLastCheckPoint, oldestLogVersionFound, currentLogVersion,
                    latestLogEntryVersion );
        }

        LogTailInformation( CheckPoint lastCheckPoint, boolean recordAfterCheckpoint, long firstTxIdAfterLastCheckPoint,
                long oldestLogVersionFound, long currentLogVersion, LogEntryVersion latestLogEntryVersion )
        {
            this.lastCheckPoint = lastCheckPoint;
            this.firstTxIdAfterLastCheckPoint = firstTxIdAfterLastCheckPoint;
            this.oldestLogVersionFound = oldestLogVersionFound;
            this.currentLogVersion = currentLogVersion;
            this.latestLogEntryVersion = latestLogEntryVersion;
            this.recordAfterCheckpoint = recordAfterCheckpoint;
        }

        public boolean commitsAfterLastCheckpoint()
        {
            return recordAfterCheckpoint;
        }
    }

}
