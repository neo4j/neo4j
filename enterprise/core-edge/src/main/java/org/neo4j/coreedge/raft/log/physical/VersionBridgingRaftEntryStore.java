/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.log.physical;

import java.io.IOException;

import org.neo4j.coreedge.raft.log.RaftLogMetadataCache;
import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

/**
 * A {@link RaftEntryStore} that uses a {@link VersionIndexRanges} to iterate over the
 */
public class VersionBridgingRaftEntryStore implements RaftEntryStore
{
    private final VersionIndexRanges ranges;
    private final SingleVersionReader reader;
    private final RaftLogMetadataCache raftLogMetadataCache;

    public VersionBridgingRaftEntryStore( VersionIndexRanges ranges, SingleVersionReader reader, RaftLogMetadataCache
            raftLogMetadataCache )
    {
        this.ranges = ranges;
        this.reader = reader;
        this.raftLogMetadataCache = raftLogMetadataCache;
    }

    @Override
    public IOCursor<RaftLogAppendRecord> getEntriesFrom( long logIndex ) throws IOException
    {
        return new IOCursor<RaftLogAppendRecord>()
        {
            private CursorValue<RaftLogAppendRecord> cursorValue = new CursorValue<>();
            private long nextIndex = logIndex;
            private VersionIndexRange currentRange = VersionIndexRange.OUT_OF_RANGE;
            private IOCursor<RaftLogAppendRecord> versionCursor;

            @Override
            public boolean next() throws IOException
            {
                if ( !currentRange.includes( nextIndex ) )
                {
                    currentRange = ranges.versionForIndex( nextIndex );
                    if ( !currentRange.includes( nextIndex ) )
                    {
                        return false;
                    }
                    close();
                    RaftLogMetadataCache.RaftLogEntryMetadata metadata = raftLogMetadataCache.getMetadata( nextIndex );
                    LogPosition thePosition = new LogPosition( currentRange.version, LogHeader.LOG_HEADER_SIZE );
                    if( metadata != null )
                    {
                        thePosition = metadata.getStartPosition();
                    }
                    versionCursor = reader.readEntriesFrom( thePosition );
                }
                while ( versionCursor.next() )
                {
                    RaftLogAppendRecord record = versionCursor.get();
                    if ( record.logIndex() == nextIndex )
                    {
                        cursorValue.set( record );
                        nextIndex++;
                        return true;
                    }
                }
                cursorValue.invalidate();
                return false;
            }

            @Override
            public void close() throws IOException
            {
                if ( versionCursor != null )
                {
                    versionCursor.close();
                }
            }

            @Override
            public RaftLogAppendRecord get()
            {
                return cursorValue.get();
            }
        };
    }
}
