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

import org.eclipse.collections.api.list.primitive.MutableLongList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;

import org.neo4j.common.EntityType;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;

import static java.lang.Long.min;
import static org.neo4j.kernel.impl.index.schema.TokenScanValue.RANGE_SIZE;

/**
 * {@link AllEntriesTokenScanReader} for {@link NativeTokenScanStore}.
 * <p>
 * {@link NativeTokenScanStore} uses {@link GBPTree} for storage and it doesn't have means of aggregating
 * results, so the approach this implementation is taking is to create one (lazy) seek cursor per token id
 * and coordinate those simultaneously over the scan. Each {@link EntityTokenRange} returned is a view
 * over all cursors at that same range, giving an aggregation of all tokens in that entity id range.
 */
class NativeAllEntriesTokenScanReader implements AllEntriesTokenScanReader
{
    private final IntFunction<Seeker<TokenScanKey,TokenScanValue>> seekProvider;
    private final List<Seeker<TokenScanKey,TokenScanValue>> cursors = new ArrayList<>();
    private final int highestTokenId;
    private final EntityType entityType;

    NativeAllEntriesTokenScanReader( IntFunction<Seeker<TokenScanKey,TokenScanValue>> seekProvider,
            int highestTokenId, EntityType entityType )
    {
        this.seekProvider = seekProvider;
        this.highestTokenId = highestTokenId;
        this.entityType = entityType;
    }

    @Override
    public long maxCount()
    {
        return BoundedIterable.UNKNOWN_MAX_COUNT;
    }

    @Override
    public int rangeSize()
    {
        return RANGE_SIZE;
    }

    @Override
    public Iterator<EntityTokenRange> iterator()
    {
        try
        {
            long lowestRange = Long.MAX_VALUE;
            closeCursors();
            for ( int tokenId = 0; tokenId <= highestTokenId; tokenId++ )
            {
                Seeker<TokenScanKey,TokenScanValue> cursor = seekProvider.apply( tokenId );

                // Bootstrap the cursor, which also provides a great opportunity to exclude if empty
                if ( cursor.next() )
                {
                    lowestRange = min( lowestRange, cursor.key().idRange );
                    cursors.add( cursor );
                }
            }
            return new EntityTokenRangeIterator( lowestRange, entityType );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void closeCursors() throws IOException
    {
        for ( Seeker<TokenScanKey,TokenScanValue> cursor : cursors )
        {
            cursor.close();
        }
        cursors.clear();
    }

    @Override
    public void close() throws Exception
    {
        closeCursors();
    }

    /**
     * The main iterator over {@link EntityTokenRange ranges}, aggregating all the cursors as it goes.
     */
    private class EntityTokenRangeIterator extends PrefetchingIterator<EntityTokenRange>
    {
        private long currentRange;
        private final EntityType entityType;

        // entityId (relative to lowestRange) --> tokenId[]
        private final MutableLongList[] tokensForEachEntity = new MutableLongList[RANGE_SIZE];

        EntityTokenRangeIterator( long lowestRange, EntityType entityType )
        {
            this.currentRange = lowestRange;
            this.entityType = entityType;
        }

        @Override
        protected EntityTokenRange fetchNextOrNull()
        {
            if ( currentRange == Long.MAX_VALUE )
            {
                return null;
            }

            Arrays.fill( tokensForEachEntity, null );
            long nextLowestRange = Long.MAX_VALUE;
            try
            {
                // One "rangeSize" range at a time
                for ( Seeker<TokenScanKey,TokenScanValue> cursor : cursors )
                {
                    long idRange = cursor.key().idRange;
                    if ( idRange < currentRange )
                    {
                        // This should only happen if the cursor has been exhausted and the iterator have moved on
                        // from the range it returned as its last hit.
                        assert !cursor.next();
                    }
                    else if ( idRange == currentRange )
                    {
                        long bits = cursor.value().bits;
                        long tokenId = cursor.key().tokenId;
                        EntityTokenRange.readBitmap( bits, tokenId, tokensForEachEntity );

                        // Advance cursor and look ahead to the next range
                        if ( cursor.next() )
                        {
                            nextLowestRange = min( nextLowestRange, cursor.key().idRange );
                        }
                    }
                    else
                    {
                        // Excluded from this range
                        nextLowestRange = min( nextLowestRange, cursor.key().idRange );
                    }
                }

                EntityTokenRange range = new EntityTokenRange( currentRange, EntityTokenRange.convertState( tokensForEachEntity ), entityType );
                currentRange = nextLowestRange;

                return range;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
