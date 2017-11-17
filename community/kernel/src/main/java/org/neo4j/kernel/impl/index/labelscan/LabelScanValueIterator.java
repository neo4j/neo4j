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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
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
class LabelScanValueIterator extends PrimitiveLongCollections.PrimitiveLongBaseIterator implements PrimitiveLongResourceIterator
{
    /**
     * {@link RawCursor} to lazily read new {@link LabelScanValue} from.
     */
    private final RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor;

    /**
     * Current base nodeId, i.e. the {@link LabelScanKey#idRange} of the current {@link LabelScanKey}.
     */
    private long baseNodeId;

    /**
     * Bit set of the current {@link LabelScanValue}.
     */
    private long bits;

    /**
     * LabelId of previously retrieved {@link LabelScanKey}, for debugging and asserting purposes.
     */
    private int prevLabel = -1;

    /**
     * IdRange of previously retrieved {@link LabelScanKey}, for debugging and asserting purposes.
     */
    private long prevRange = -1;

    /**
     * Remove provided cursor from this collection when iterator is exhausted.
     */
    private final Collection<RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>> toRemoveFromWhenClosed;

    /**
     * Indicate provided cursor has been closed.
     */
    private boolean closed;

    LabelScanValueIterator( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor,
            Collection<RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>> toRemoveFromWhenClosed )
    {
        this.cursor = cursor;
        this.toRemoveFromWhenClosed = toRemoveFromWhenClosed;
    }

    /**
     * @return next node id in the current {@link LabelScanValue} or, if current value exhausted,
     * goes to next {@link LabelScanValue} from {@link RawCursor}. Returns {@code true} if next node id
     * was found, otherwise {@code false}.
     */
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
        bits &= bits - 1;
        return next( baseNodeId + delta );
    }

    @Override
    public void close()
    {
        if ( !closed )
        {
            try
            {
                cursor.close();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
            finally
            {
                toRemoveFromWhenClosed.remove( cursor );
                closed = true;
            }
        }
    }
}
