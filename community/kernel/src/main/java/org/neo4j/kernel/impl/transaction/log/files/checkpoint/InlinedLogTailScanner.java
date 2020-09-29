/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.nio.channels.ClosedByInterruptException;

import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryInlinedCheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.kernel.impl.transaction.log.files.RangeLogVersionVisitor.UNKNOWN;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;

/**
 * This class collects information about the latest entries in the transaction log. Since the only way we have to collect
 * said information is to scan the transaction log from beginning to end, which is costly, we do this once and save the
 * result for others to consume.
 * <p>
 * Due to the nature of transaction logs and log rotation, a single transaction log file has to be scanned forward, and
 * if the required data is not found we search backwards through log file versions.
 */
public class InlinedLogTailScanner extends AbstractLogTailScanner
{
    private final boolean failOnCorruptedLogFiles;

    InlinedLogTailScanner( LogFiles logFiles, LogEntryReader logEntryReader, Monitors monitors, boolean failOnCorruptedLogFiles, MemoryTracker memoryTracker )
    {
        this( logFiles, logEntryReader, monitors, failOnCorruptedLogFiles, NullLogProvider.getInstance(), memoryTracker );
    }

    InlinedLogTailScanner( LogFiles logFiles,
                           LogEntryReader logEntryReader, Monitors monitors,
                           boolean failOnCorruptedLogFiles, LogProvider log, MemoryTracker memoryTracker )
    {
        super( logFiles, logEntryReader, monitors, log, memoryTracker );
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
    }

    InlinedLogTailScanner( LogFiles logFiles, TransactionLogFilesContext context )
    {
        this( logFiles, context.getLogEntryReader(), context.getMonitors(), context.isFailOnCorruptedLogFiles(), context.getLogProvider(),
                context.getMemoryTracker() );
    }

    protected LogTailInformation findLogTail() throws IOException
    {
        LogFile logFile = logFiles.getLogFile();
        final long highestLogVersion = logFile.getHighestLogVersion();
        long version = highestLogVersion;
        long versionToSearchForCommits = highestLogVersion;
        LogEntryStart latestStartEntry = null;
        long oldestStartEntryTransaction = NO_TRANSACTION_ID;
        long oldestVersionFound = -1;
        byte latestLogEntryVersion = 0;
        boolean startRecordAfterCheckpoint = false;
        boolean corruptedTransactionLogs = false;

        while ( version >= logFile.getLowestLogVersion() && version >= INITIAL_LOG_VERSION )
        {
            log.info( "Scanning log file with version %d for checkpoint entries", version );

            oldestVersionFound = version;
            CheckpointInfo latestCheckPoint = null;
            StoreId storeId = StoreId.UNKNOWN;
            try ( LogVersionedStoreChannel channel = logFile.openForVersion( version );
                  var readAheadChannel = new ReadAheadLogChannel( channel, memoryTracker );
                  LogEntryCursor cursor = new LogEntryCursor( logEntryReader, readAheadChannel ) )
            {
                LogHeader logHeader = logFile.extractHeader( version );
                storeId = logHeader.getStoreId();
                LogEntry entry;
                long position = logHeader.getStartPosition().getByteOffset();
                long channelVersion = version;
                while ( cursor.next() )
                {
                    entry = cursor.get();

                    // Collect data about latest checkpoint
                    if ( entry instanceof LogEntryInlinedCheckPoint )
                    {
                        latestCheckPoint = new CheckpointInfo( (LogEntryInlinedCheckPoint) entry, storeId, new LogPosition( channelVersion, position ) );
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
                        LogEntryStart startEntry = (LogEntryStart) entry;
                        if ( version == versionToSearchForCommits )
                        {
                            latestStartEntry = startEntry;
                        }
                        startRecordAfterCheckpoint = true;
                    }

                    // Collect data about latest entry version, only in first log file
                    if ( version == versionToSearchForCommits || latestLogEntryVersion == 0 )
                    {
                        latestLogEntryVersion = entry.getVersion();
                    }
                    position = channel.position();
                    channelVersion = channel.getVersion();
                }

                verifyReaderPosition( version, logEntryReader.lastPosition() );
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
                        latestLogEntryVersion, latestCheckPoint, corruptedTransactionLogs, storeId );
            }

            version--;

            // if we have found no commits in the latest log, keep searching in the next one
            if ( latestStartEntry == null )
            {
                versionToSearchForCommits--;
            }
        }

        return new LogTailInformation( corruptedTransactionLogs || startRecordAfterCheckpoint,
                oldestStartEntryTransaction, oldestVersionFound == UNKNOWN, highestLogVersion, latestLogEntryVersion );
    }
}
