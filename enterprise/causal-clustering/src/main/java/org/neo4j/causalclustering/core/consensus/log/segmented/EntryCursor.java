/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.causalclustering.core.consensus.log.EntryRecord;
import org.neo4j.causalclustering.core.consensus.log.segmented.OpenEndRangeMap.ValueRange;
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
    private IOCursor<EntryRecord> cursor;
    private ValueRange<Long,SegmentFile> segmentRange;
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

        if ( cursor.next() )
        {
            currentRecord.set( cursor.get() );
            return true;
        }

        currentRecord.invalidate();
        return false;
    }

    private boolean nextSegment() throws IOException
    {
        segmentRange = segments.getForIndex( currentIndex );
        Optional<SegmentFile> optionalFile = segmentRange.value();

        if ( !optionalFile.isPresent() )
        {
            currentRecord.invalidate();
            return false;
        }

        SegmentFile file = optionalFile.get();

        /* Open new reader before closing old, so that pruner cannot overtake us. */
        IOCursor<EntryRecord> oldCursor = cursor;
        try
        {
            cursor = file.getCursor( currentIndex );
        }
        catch ( DisposedException e )
        {
            currentRecord.invalidate();
            return false;
        }

        if ( oldCursor != null )
        {
            oldCursor.close();
        }

        limit = segmentRange.limit().orElse( Long.MAX_VALUE );

        return true;
    }

    @Override
    public void close() throws IOException
    {
        if ( cursor != null )
        {
            cursor.close();
        }
    }

    @Override
    public EntryRecord get()
    {
        return currentRecord.get();
    }
}
