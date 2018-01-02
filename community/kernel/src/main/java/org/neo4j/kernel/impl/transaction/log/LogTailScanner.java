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

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;

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
    private final PhysicalLogFiles logFiles;
    private final FileSystemAbstraction fileSystem;
    private final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader;
    private LogTailInformation logTailInformation;

    public LogTailScanner( PhysicalLogFiles logFiles, FileSystemAbstraction fileSystem,
            LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader )
    {
        this.logFiles = logFiles;
        this.fileSystem = fileSystem;
        this.logEntryReader = logEntryReader;
    }

    private LogTailInformation update() throws IOException
    {
        final long fromVersionBackwards = logFiles.getHighestLogVersion();
        long version = fromVersionBackwards;
        long versionToSearchForCommits = fromVersionBackwards;
        LogEntryStart latestStartEntry = null;
        LogEntryStart oldestStartEntry = null;
        long oldestVersionFound = -1;
        LogEntryVersion latestLogEntryVersion = null;

        while ( version >= INITIAL_LOG_VERSION )
        {
            LogVersionedStoreChannel channel =
                    PhysicalLogFile.tryOpenForVersion( logFiles, fileSystem, version, false );
            if ( channel == null )
            {
                break;
            }

            oldestVersionFound = version;

            CheckPoint latestCheckPoint = null;
            ReadableLogChannel recoveredDataChannel =
                    new ReadAheadLogChannel( channel, NO_MORE_CHANNELS );
            boolean firstStartEntry = true;

            try ( LogEntryCursor cursor = new LogEntryCursor( logEntryReader, recoveredDataChannel ) )
            {
                LogEntry entry;
                while ( cursor.next() )
                {
                    entry = cursor.get();

                    // Collect data about latest checkpoint
                    if ( entry instanceof CheckPoint )
                    {
                        latestCheckPoint = entry.as();
                    }

                    // Collect data about latest commits
                    if ( entry instanceof LogEntryStart )
                    {
                        LogEntryStart startEntry = entry.as();
                        if ( version == versionToSearchForCommits )
                        {
                            latestStartEntry = startEntry;
                        }

                        // The scan goes backwards by log version, although forward per log version
                        // Oldest start entry will be the first in the last log version scanned.
                        if ( firstStartEntry )
                        {
                            oldestStartEntry = startEntry;
                            firstStartEntry = false;
                        }
                    }

                    // Collect data about latest entry version, only in first log file
                    if ( version == versionToSearchForCommits || latestLogEntryVersion == null )
                    {
                        latestLogEntryVersion = entry.getVersion();
                    }
                }
            }

            if ( latestCheckPoint != null )
            {
                return latestCheckPoint( fromVersionBackwards, version, latestStartEntry, oldestVersionFound,
                        latestCheckPoint, latestLogEntryVersion );
            }

            version--;

            // if we have found no commits in the latest log, keep searching in the next one
            if ( latestStartEntry == null )
            {
                versionToSearchForCommits--;
            }
        }

        boolean commitsAfterCheckPoint = oldestStartEntry != null;
        long firstTxAfterPosition = commitsAfterCheckPoint
                ? extractFirstTxIdAfterPosition( oldestStartEntry.getStartPosition(), fromVersionBackwards )
                : LogTailInformation.NO_TRANSACTION_ID;

        return new LogTailInformation( null, commitsAfterCheckPoint, firstTxAfterPosition, oldestVersionFound,
                fromVersionBackwards, latestLogEntryVersion );
    }

    protected LogTailInformation latestCheckPoint( long fromVersionBackwards, long version,
            LogEntryStart latestStartEntry, long oldestVersionFound, CheckPoint latestCheckPoint,
            LogEntryVersion latestLogEntryVersion ) throws IOException
    {
        // Is the latest start entry in this log file version later than what the latest check point targets?
        LogPosition target = latestCheckPoint.getLogPosition();
        boolean startEntryAfterCheckPoint = latestStartEntry != null &&
                latestStartEntry.getStartPosition().compareTo( target ) >= 0;
        if ( !startEntryAfterCheckPoint )
        {
            if ( target.getLogVersion() < version )
            {
                // This check point entry targets a previous log file.
                // Go there and see if there's a transaction. Reader is capped to that log version.
                startEntryAfterCheckPoint = extractFirstTxIdAfterPosition( target, version ) !=
                        LogTailInformation.NO_TRANSACTION_ID;
            }
        }

        // Extract first transaction id after check point target position.
        // Reader may continue into log files after the initial version.
        long firstTxIdAfterCheckPoint = startEntryAfterCheckPoint
                ? extractFirstTxIdAfterPosition( target, fromVersionBackwards )
                : LogTailInformation.NO_TRANSACTION_ID;
        return new LogTailInformation( latestCheckPoint, startEntryAfterCheckPoint,
                firstTxIdAfterCheckPoint, oldestVersionFound, fromVersionBackwards, latestLogEntryVersion );
    }

    /**
     * Extracts txId from first commit entry, when starting reading at the given {@code position}.
     * If no commit entry found in the version, the reader will continue into next version(s) up till
     * {@code maxLogVersion} until finding one.
     *
     * @param initialPosition {@link LogPosition} to start scan from.
     * @param maxLogVersion max log version to scan.
     * @return txId of closes commit entry to {@code initialPosition}, or {@link LogTailInformation#NO_TRANSACTION_ID}
     * if not found.
     * @throws IOException on I/O error.
     */
    protected long extractFirstTxIdAfterPosition( LogPosition initialPosition, long maxLogVersion ) throws IOException
    {
        LogPosition currentPosition = initialPosition;
        while ( currentPosition.getLogVersion() <= maxLogVersion )
        {
            LogVersionedStoreChannel storeChannel = PhysicalLogFile.tryOpenForVersion( logFiles, fileSystem,
                    currentPosition.getLogVersion(), false );
            if ( storeChannel != null )
            {
                try
                {
                    storeChannel.position( currentPosition.getByteOffset() );
                    try ( ReadAheadLogChannel logChannel = new ReadAheadLogChannel( storeChannel, NO_MORE_CHANNELS );
                          LogEntryCursor cursor = new LogEntryCursor( logEntryReader, logChannel ) )
                    {
                        while ( cursor.next() )
                        {
                            LogEntry entry = cursor.get();
                            if ( entry instanceof LogEntryCommit )
                            {
                                return ((LogEntryCommit) entry).getTxId();
                            }
                        }
                    }
                }
                finally
                {
                    storeChannel.close();
                }
            }

            currentPosition = LogPosition.start( currentPosition.getLogVersion() + 1 );
        }
        return LogTailInformation.NO_TRANSACTION_ID;
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
                logTailInformation = update();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Error encountered while parsing transaction logs", e );
            }
        }

        return logTailInformation;
    }

    public static class LogTailInformation
    {
        public static long NO_TRANSACTION_ID = -1;

        public final CheckPoint lastCheckPoint;
        public final boolean commitsAfterLastCheckPoint;
        public final long firstTxIdAfterLastCheckPoint;
        public final long oldestLogVersionFound;
        public final long currentLogVersion;
        public final LogEntryVersion latestLogEntryVersion;

        public LogTailInformation( CheckPoint lastCheckPoint, boolean commitsAfterLastCheckPoint,
                long firstTxIdAfterLastCheckPoint, long oldestLogVersionFound, long currentLogVersion, LogEntryVersion latestLogEntryVersion )
        {
            this.lastCheckPoint = lastCheckPoint;
            this.commitsAfterLastCheckPoint = commitsAfterLastCheckPoint;
            this.firstTxIdAfterLastCheckPoint = firstTxIdAfterLastCheckPoint;
            this.oldestLogVersionFound = oldestLogVersionFound;
            this.currentLogVersion = currentLogVersion;
            this.latestLogEntryVersion = latestLogEntryVersion;
        }
    }
}
