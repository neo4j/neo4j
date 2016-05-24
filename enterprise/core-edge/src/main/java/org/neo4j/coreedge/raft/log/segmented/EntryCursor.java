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
import java.util.Optional;

import org.neo4j.coreedge.raft.log.EntryRecord;
import org.neo4j.coreedge.raft.log.segmented.OpenEndRangeMap.ValueRange;
import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.IOCursor;

/**
 * The entry store allows iterating over RAFT log entries efficiently and handles moving from one
 * segment to the next in a transparent manner. It can thus be mainly viewed as a factory for a
 * smart segment-crossing cursor.
 */
class EntryCursor implements IOCursor<EntryRecord>
{

    private final Segments segments;
    private IOCursor<EntryRecord> reader;
    private ValueRange<Long,SegmentFile> segmentRange = null;
    private long currentIndex;

    private long limit = Long.MAX_VALUE;
    private CursorValue<EntryRecord> currentRecord = new CursorValue<>();

    EntryCursor( Segments segments, long logIndex ) throws IOException
    {
        this.segments = segments;
        this.currentIndex = logIndex - 1;
    }

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
        Optional<Long> limitOptional = segmentRange.limit();
        Optional<SegmentFile> segmentRangeOptional = segmentRange.value();

        boolean hasSegment = segmentRangeOptional.isPresent();
        if ( hasSegment )
        {
            this.limit = limitOptional.orElse( Long.MAX_VALUE );
            try
            {
                IOCursor<EntryRecord> newReader = segmentRangeOptional.get().getReader( currentIndex );
                if ( reader != null )
                {
                    reader.close();
                }
                reader = newReader;
            }
            catch ( DisposedException e )
            {
                //The reader was disposed after we checked for it's existence.
                //todo: log this
                return false;
            }
        }
        else
        {
            currentRecord.invalidate();
        }
        return hasSegment;
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
}
