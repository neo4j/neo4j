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
package org.neo4j.kernel.impl.store.id;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.impl.store.id.IdRangeIterator.VALUE_REPRESENTING_NULL;

public class IdRangeIteratorTest
{
    @Test
    public void shouldReturnValueRepresentingNullIfWeExhaustIdRange() throws Exception
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRange( new long[]{}, 0, rangeLength ).iterator();

        // when
        for ( int i = 0; i < rangeLength; i++ )
        {
            iterator.nextId();
        }

        // then
        assertEquals( IdRangeIterator.VALUE_REPRESENTING_NULL, iterator.nextId() );
    }

    @Test
    public void shouldNotHaveAnyGaps() throws Exception
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRange( new long[]{}, 0, rangeLength ).iterator();

        // when
        Set<Long> seenIds = new HashSet<>();
        for ( int i = 0; i < rangeLength; i++ )
        {
            seenIds.add( iterator.nextId() );
            if ( i > 0 )
            {
                // then
                assertTrue( "Missing id " + (i - 1), seenIds.contains( (long) i - 1 ) );
            }
        }
    }

    @Test
    public void shouldUseDefragIdsFirst() throws Exception
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRange( new long[] {7, 8, 9}, 1024, rangeLength ).iterator();

        // then
        assertEquals( 7, iterator.nextId() );
        assertEquals( 8, iterator.nextId() );
        assertEquals( 9, iterator.nextId() );
        assertEquals( 1024, iterator.nextId() );
    }

    @Test
    public void shouldGetNextIdBatchFromOnlyDefragIds() throws Exception
    {
        // given
        IdRangeIterator iterator = new IdRange( new long[] {1, 2, 3, 4, 5, 6}, 7, 0 ).iterator();

        // when
        IdRangeIterator subRange = iterator.nextIdBatch( 5 ).iterator();

        // then
        assertEquals( 6, iterator.nextId() );
        for ( long i = 0; i < 5; i++ )
        {
            assertEquals( 1 + i, subRange.nextId() );
        }
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId() );
    }

    @Test
    public void shouldGetNextIdBatchFromOnlyDefragIdsWhenSomeDefragIdsHaveAlreadyBeenReturned() throws Exception
    {
        // given
        IdRangeIterator iterator = new IdRange( new long[] {1, 2, 3, 4, 5, 6}, 7, 0 ).iterator();
        iterator.nextId();
        iterator.nextId();

        // when
        IdRangeIterator subRange = iterator.nextIdBatch( 3 ).iterator();

        // then
        assertEquals( 6, iterator.nextId() );
        for ( long i = 0; i < 3; i++ )
        {
            assertEquals( 3 + i, subRange.nextId() );
        }
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId() );
    }

    @Test
    public void shouldGetNextIdBatchFromSomeDefragAndSomeRangeIds() throws Exception
    {
        // given
        IdRangeIterator iterator = new IdRange( new long[] {1, 2, 3}, 10, 5 ).iterator();
        iterator.nextId();

        // when
        IdRangeIterator subRange = iterator.nextIdBatch( 5 ).iterator();

        // then
        assertEquals( 13, iterator.nextId() );
        assertEquals( 2, subRange.nextId() );
        assertEquals( 3, subRange.nextId() );
        assertEquals( 10, subRange.nextId() );
        assertEquals( 11, subRange.nextId() );
        assertEquals( 12, subRange.nextId() );
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId() );
    }

    @Test
    public void shouldGetNextIdBatchFromSomeRangeIds() throws Exception
    {
        // given
        IdRangeIterator iterator = new IdRange( EMPTY_LONG_ARRAY, 0, 20 ).iterator();
        iterator.nextId();

        // when
        IdRangeIterator subRange = iterator.nextIdBatch( 5 ).iterator();

        // then
        assertEquals( 6, iterator.nextId() );
        assertEquals( 1, subRange.nextId() );
        assertEquals( 2, subRange.nextId() );
        assertEquals( 3, subRange.nextId() );
        assertEquals( 4, subRange.nextId() );
        assertEquals( 5, subRange.nextId() );
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId() );

        // when
        subRange = iterator.nextIdBatch( 2 ).iterator();

        // then
        assertEquals( 9, iterator.nextId() );
        assertEquals( 7, subRange.nextId() );
        assertEquals( 8, subRange.nextId() );
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId() );
    }

    @Test
    public void shouldGetNextIdBatchFromSomeRangeIdsWhenThereAreUsedDefragIds() throws Exception
    {
        // given
        IdRangeIterator iterator = new IdRange( new long[] {0, 1, 2}, 3, 10 ).iterator();
        iterator.nextId();
        iterator.nextId();
        iterator.nextId();

        // when
        IdRangeIterator subRange = iterator.nextIdBatch( 3 ).iterator();

        // then
        assertEquals( 6, iterator.nextId() );
        assertEquals( 3, subRange.nextId() );
        assertEquals( 4, subRange.nextId() );
        assertEquals( 5, subRange.nextId() );
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId() );

        // when
        subRange = iterator.nextIdBatch( 3 ).iterator();

        // then
        assertEquals( 10, iterator.nextId() );
        assertEquals( 7, subRange.nextId() );
        assertEquals( 8, subRange.nextId() );
        assertEquals( 9, subRange.nextId() );
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId() );
    }
}
