/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.eclipse.collections.api.iterator.LongIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexOrder;

import static org.neo4j.kernel.impl.index.schema.NativeTokenScanWriter.offsetOf;
import static org.neo4j.kernel.impl.index.schema.NativeTokenScanWriter.rangeOf;
import static org.neo4j.kernel.impl.index.schema.TokenScanValue.RANGE_SIZE;

/**
 * {@link LongIterator} which iterate over multiple {@link TokenScanValue} and for each
 * iterate over each set bit, returning actual entity ids, i.e. {@code entityIdRange+bitOffset}.
 *
 * The provided {@link Seeker} is managed externally, e.g. {@link NativeTokenScanReader},
 * this because implemented interface lacks close-method.
 */
class TokenScanValueIterator extends TokenScanValueIndexAccessor implements PrimitiveLongResourceIterator
{
    private long fromId;
    private boolean hasNextDecided;
    private boolean hasNext;
    protected long next;

    /**
     * @param fromId entity to start from (exclusive). The cursor gives entries that are effectively small bit-sets, and the fromId may
     * be somewhere inside a bit-set range.
     */
    TokenScanValueIterator( Seeker<TokenScanKey,TokenScanValue> cursor, long fromId )
    {
        super( cursor );
        this.fromId = fromId;
    }

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

    /**
     * @return next entity id in the current {@link TokenScanValue} or, if current value exhausted,
     * goes to next {@link TokenScanValue} by progressing the {@link Seeker}. Returns {@code true}
     * if it found next entity id, otherwise {@code false}.
     */
    protected boolean fetchNext()
    {
        while ( true )
        {
            if ( bits != 0 )
            {
                int delta = Long.numberOfTrailingZeros( bits );
                bits &= bits - 1;
                next =  baseEntityId + delta ;
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

            TokenScanKey key = cursor.key();
            baseEntityId = key.idRange * RANGE_SIZE;
            bits = cursor.value().bits;

            if ( fromId != TokenScanReader.NO_ID )
            {
                // If we've been told to start at a specific id then trim off ids in this range less than or equal to that id
                long range = rangeOf( fromId );
                if ( range == key.idRange )
                {
                    // Only do this if we're in the idRange that fromId is in, otherwise there were no ids this time in this range
                    long relativeStartId = offsetOf( fromId );
                    long mask = relativeStartId == RANGE_SIZE - 1 ? -1 : (1L << (relativeStartId + 1)) - 1;
                    bits &= ~mask;
                }
                // ... and let's not do that again, only for the first idRange
                fromId = TokenScanReader.NO_ID;
            }

            //noinspection AssertWithSideEffects
            assert keysInOrder( key, IndexOrder.ASCENDING );
        }
    }
}
