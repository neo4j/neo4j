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
package org.neo4j.kernel.recovery;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;

public class LatestCheckPointFinder
{
    private final PhysicalLogFiles logFiles;
    private final FileSystemAbstraction fileSystem;
    private final LogEntryReader<ReadableLogChannel> logEntryReader;

    public LatestCheckPointFinder( PhysicalLogFiles logFiles, FileSystemAbstraction fileSystem,
            LogEntryReader<ReadableLogChannel> logEntryReader )
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
        long oldestVersionFound = -1;
        while ( version >= INITIAL_LOG_VERSION )
        {
            LogVersionedStoreChannel channel = PhysicalLogFile.tryOpenForVersion( logFiles, fileSystem, version );
            if ( channel == null )
            {
                break;
            }

            oldestVersionFound = version;

            CheckPoint latestCheckPoint = null;
            ReadableLogChannel recoveredDataChannel =
                    new ReadAheadLogChannel( channel, NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE );

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
                    if ( entry instanceof LogEntryStart && ( version == versionToSearchForCommits ) )
                    {
                        latestStartEntry = entry.as();
                    }
                }
            }

            if ( latestCheckPoint != null )
            {
                boolean commitsAfterCheckPoint = latestStartEntry != null &&
                        latestStartEntry.getStartPosition().compareTo( latestCheckPoint.getLogPosition() ) >= 0;
                return new LatestCheckPoint( latestCheckPoint, commitsAfterCheckPoint , oldestVersionFound );
            }

            version--;

            // if we have found no commits in the latest log, keep searching in the next one
            if ( latestStartEntry == null )
            {
                versionToSearchForCommits--;
            }
        }

        return new LatestCheckPoint( null, latestStartEntry != null, oldestVersionFound );
    }

    public static class LatestCheckPoint
    {
        public final CheckPoint checkPoint;
        public final boolean commitsAfterCheckPoint;
        public final long oldestLogVersionFound;

        public LatestCheckPoint( CheckPoint checkPoint, boolean commitsAfterCheckPoint, long oldestLogVersionFound )
        {
            this.checkPoint = checkPoint;
            this.commitsAfterCheckPoint = commitsAfterCheckPoint;
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
                   oldestLogVersionFound == that.oldestLogVersionFound &&
                   (checkPoint == null ? that.checkPoint == null : checkPoint.equals( that.checkPoint ));
        }

        @Override
        public int hashCode()
        {
            int result = checkPoint != null ? checkPoint.hashCode() : 0;
            result = 31 * result + (commitsAfterCheckPoint ? 1 : 0);
            result = 31 * result + (int) (oldestLogVersionFound ^ (oldestLogVersionFound >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            return "LatestCheckPoint{" +
                   "checkPoint=" + checkPoint +
                   ", commitsAfterCheckPoint=" + commitsAfterCheckPoint +
                   ", oldestLogVersionFound=" + oldestLogVersionFound +
                   '}';
        }
    }
}
