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
package org.neo4j.kernel.recovery;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;

public class LatestCheckPointFinder
{
    private final PhysicalLogFiles logFiles;
    private final FileSystemAbstraction fileSystem;
    private final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader;

    public LatestCheckPointFinder( PhysicalLogFiles logFiles, FileSystemAbstraction fileSystem,
            LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader )
    {
        this.logFiles = logFiles;
        this.fileSystem = fileSystem;
        this.logEntryReader = logEntryReader;
    }

    public LatestCheckPoint find( long fromVersionBackwards ) throws IOException
    {
        long version = fromVersionBackwards;
        long versionToSearchForCommits = fromVersionBackwards;
        LogEntryStart latestStartEntry = null;
        LogEntryStart oldestStartEntry = null;
        long oldestVersionFound = -1;
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
                    if ( entry instanceof CheckPoint )
                    {
                        latestCheckPoint = entry.as();
                    }
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
                }
            }

            if ( latestCheckPoint != null )
            {
                return latestCheckPoint( fromVersionBackwards, version, latestStartEntry, oldestVersionFound,
                        latestCheckPoint );
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
                : LatestCheckPoint.NO_TRANSACTION_ID;

        return new LatestCheckPoint( null, commitsAfterCheckPoint, firstTxAfterPosition, oldestVersionFound );
    }

    protected LatestCheckPoint latestCheckPoint( long fromVersionBackwards, long version, LogEntryStart
            latestStartEntry,
            long oldestVersionFound, CheckPoint latestCheckPoint ) throws IOException
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
                        LatestCheckPoint.NO_TRANSACTION_ID;
            }
        }

        // Extract first transaction id after check point target position.
        // Reader may continue into log files after the initial version.
        long firstTxIdAfterCheckPoint = startEntryAfterCheckPoint
                ? extractFirstTxIdAfterPosition( target, fromVersionBackwards )
                : LatestCheckPoint.NO_TRANSACTION_ID;
        return new LatestCheckPoint( latestCheckPoint, startEntryAfterCheckPoint,
                firstTxIdAfterCheckPoint, oldestVersionFound );
    }

    /**
     * Extracts txId from first commit entry, when starting reading at the given {@code position}.
     * If no commit entry found in the version, the reader will continue into next version(s) up till
     * {@code maxLogVersion} until finding one.
     *
     * @param initialPosition {@link LogPosition} to start scan from.
     * @param maxLogVersion max log version to scan.
     * @return txId of closes commit entry to {@code initialPosition}, or {@link LatestCheckPoint#NO_TRANSACTION_ID}
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
        return LatestCheckPoint.NO_TRANSACTION_ID;
    }

    public static class LatestCheckPoint
    {
        public static long NO_TRANSACTION_ID = -1;

        public final CheckPoint checkPoint;
        public final boolean commitsAfterCheckPoint;
        public final long firstTxIdAfterLastCheckPoint;
        public final long oldestLogVersionFound;

        public LatestCheckPoint( CheckPoint checkPoint, boolean commitsAfterCheckPoint,
                long firstTxIdAfterLastCheckPoint, long oldestLogVersionFound )
        {
            this.checkPoint = checkPoint;
            this.commitsAfterCheckPoint = commitsAfterCheckPoint;
            this.firstTxIdAfterLastCheckPoint = firstTxIdAfterLastCheckPoint;
            this.oldestLogVersionFound = oldestLogVersionFound;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            LatestCheckPoint that = (LatestCheckPoint) o;

            return commitsAfterCheckPoint == that.commitsAfterCheckPoint &&
                   firstTxIdAfterLastCheckPoint == that.firstTxIdAfterLastCheckPoint &&
                   oldestLogVersionFound == that.oldestLogVersionFound &&
                   (checkPoint == null ? that.checkPoint == null : checkPoint.equals( that.checkPoint ));
        }

        @Override
        public int hashCode()
        {
            int result = checkPoint != null ? checkPoint.hashCode() : 0;
            result = 31 * result + (commitsAfterCheckPoint ? 1 : 0);
            if ( commitsAfterCheckPoint )
            {
                result = 31 * result + Long.hashCode( firstTxIdAfterLastCheckPoint );
            }
            result = 31 * result + Long.hashCode( oldestLogVersionFound );
            return result;
        }

        @Override
        public String toString()
        {
            return "LatestCheckPoint{" +
                   "checkPoint=" + checkPoint +
                   ", commitsAfterCheckPoint=" + commitsAfterCheckPoint +
                   (commitsAfterCheckPoint ? ", firstTxIdAfterLastCheckPoint=" + firstTxIdAfterLastCheckPoint : "") +
                   ", oldestLogVersionFound=" + oldestLogVersionFound +
                   '}';
        }
    }
}
