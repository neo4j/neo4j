/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;

class LabelScanValueIterator extends PrimitiveLongCollections.PrimitiveLongBaseIterator
{
    private final int rangeSize;
    private final RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor;
    private long baseNodeId;
    private long bits;

    private int prevLabel = -1;
    private long prevRange = -1;

    LabelScanValueIterator( int rangeSize, RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor )
    {
        this.rangeSize = rangeSize;
        this.cursor = cursor;
    }

    @Override
    protected boolean fetchNext()
    {
        while ( true )
        {
            if ( bits != 0 )
            {
                return nextFromCurrent();
            }

            try
            {
                if ( !cursor.next() )
                {
                    return false;
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }

            Hit<LabelScanKey,LabelScanValue> hit = cursor.get();
            baseNodeId = hit.key().idRange * rangeSize;
            bits = hit.value().bits;

            assert keysInOrder( hit.key() );
        }
    }

    private boolean keysInOrder( LabelScanKey key )
    {
        assert key.labelId >= prevLabel : "Expected to get ordered results, got " + key +
                " where previous label was " + prevLabel;
        assert key.idRange > prevRange : "Expected to get ordered results, got " + key +
                " where previous range was " + prevRange;
        prevLabel = key.labelId;
        prevRange = key.idRange;
        // Made as a method returning boolean so that it can participate in an assert call.
        return true;
    }

    private boolean nextFromCurrent()
    {
        int delta = Long.numberOfTrailingZeros( bits );
        bits &= bits-1;
        return next( baseNodeId + delta );
    }
}
