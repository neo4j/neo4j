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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;

/**
 * {@link PrimitiveLongIterator} which iterate over multiple {@link LabelScanValue} and for each
 * iterate over each set bit, returning actual node ids, i.e. {@code nodeIdRange+bitOffset}.
 *
 * The provided {@link RawCursor} is managed externally, e.g. {@link NativeLabelScanReader},
 * this because implemented interface lacks close-method.
 */
class LabelScanValueIterator extends LabelScanValueIndexAccessor implements PrimitiveLongResourceIterator
{
    private boolean hasNextDecided;
    private boolean hasNext;
    protected long next;

    @Override
    public boolean hasNext()
    {
        if ( !hasNextDecided )
        {
            hasNext = fetchNext();
            hasNextDecided = true;
        }
        return hasNext;
    }

    @Override
    public long next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException( "No more elements in " + this );
        }
        hasNextDecided = false;
        return next;
    }

    LabelScanValueIterator( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor,
            Collection<RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>> toRemoveFromWhenClosed )
    {
        super( toRemoveFromWhenClosed, cursor );
    }

    /**
     * @return next node id in the current {@link LabelScanValue} or, if current value exhausted,
     * goes to next {@link LabelScanValue} from {@link RawCursor}. Returns {@code true} if next node id
     * was found, otherwise {@code false}.
     */
    protected boolean fetchNext()
    {
        while ( true )
        {
            if ( bits != 0 )
            {
                int delta = Long.numberOfTrailingZeros( bits );
                bits &= bits - 1;
                next =  baseNodeId + delta ;
                hasNext = true;
                return true;
            }

            try
            {
                if ( !cursor.next() )
                {
                    close();
                    return false;
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }

            Hit<LabelScanKey,LabelScanValue> hit = cursor.get();
            baseNodeId = hit.key().idRange * LabelScanValue.RANGE_SIZE;
            bits = hit.value().bits;

            //noinspection AssertWithSideEffects
            assert keysInOrder( hit.key() );
        }
    }
}
