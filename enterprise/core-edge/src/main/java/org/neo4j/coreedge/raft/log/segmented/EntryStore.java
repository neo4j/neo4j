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
package org.neo4j.coreedge.raft.log.segmented;

import java.io.IOException;

import org.neo4j.coreedge.raft.log.EntryRecord;
import org.neo4j.coreedge.raft.log.RaftLogCompactedException;
import org.neo4j.coreedge.raft.log.segmented.OpenEndRangeMap.ValueRange;
import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.IOCursor;

/**
 * The entrie store allows iterating over RAFT log entries efficiently and handles moving from one
 * segment to the next in a transparent manner. It can thus be mainly viewed as a factory for a
 * smart segment-crossing cursor.
 */
class EntryStore
{
    private Segments segments;

    EntryStore( Segments segments )
    {
        this.segments = segments;
    }

    IOCursor<EntryRecord> getEntriesFrom( long logIndex ) throws IOException, RaftLogCompactedException
    {
        return new IOCursor<EntryRecord>()
        {
            IOCursor<EntryRecord> reader;
            ValueRange<Long,SegmentFile> segmentRange = null;
            long currentIndex = logIndex - 1;
            long limit = Long.MAX_VALUE;
            CursorValue<EntryRecord> currentRecord = new CursorValue<>();

            @Override
            public boolean next() throws IOException
            {
                currentIndex++;

                if ( segmentRange == null || currentIndex >= limit )
                {
                    if ( !nextSegment() )
                    {
                        return false;
                    }
                }

                if ( reader.next() )
                {
                    currentRecord.set( reader.get() );
                    return true;
                }

                currentRecord.invalidate();
                return false;
            }

            private boolean nextSegment() throws IOException
            {
                segmentRange = segments.getForIndex( currentIndex );
                if ( !segmentRange.value().isPresent() )
                {
                    currentRecord.invalidate();
                    return false;
                }
                else if( segmentRange.limit().isPresent() )
                {
                    limit = segmentRange.limit().get();
                }
                else
                {
                    limit = Long.MAX_VALUE;
                }

                try
                {
                    IOCursor<EntryRecord> newReader = segmentRange.value().get().getReader( currentIndex );
                    if ( reader != null )
                    {
                        reader.close();
                    }
                    reader = newReader;
                }
                catch ( DisposedException e )
                {
                    // TODO Handle
                }
                return true;
            }

            @Override
            public void close() throws IOException
            {
                if ( reader != null )
                {
                    reader.close();
                }
            }

            @Override
            public EntryRecord get()
            {
                return currentRecord.get();
            }
        };
    }
}
