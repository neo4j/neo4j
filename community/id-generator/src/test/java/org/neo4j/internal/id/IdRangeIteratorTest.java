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
package org.neo4j.internal.id;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.internal.id.IdRangeIterator.VALUE_REPRESENTING_NULL;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@Execution( CONCURRENT )
class IdRangeIteratorTest
{
    @Test
    void shouldReturnValueRepresentingNullIfWeExhaustIdRange()
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRange( new long[]{}, 0, rangeLength ).iterator();

        // when
        for ( int i = 0; i < rangeLength; i++ )
        {
            iterator.nextId( NULL );
        }

        // then
        assertEquals( IdRangeIterator.VALUE_REPRESENTING_NULL, iterator.nextId( NULL ) );
    }

    @Test
    void shouldNotHaveAnyGaps()
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRange( new long[]{}, 0, rangeLength ).iterator();

        // when
        Set<Long> seenIds = new HashSet<>();
        for ( int i = 0; i < rangeLength; i++ )
        {
            seenIds.add( iterator.nextId( NULL ) );
            if ( i > 0 )
            {
                // then
                assertTrue( seenIds.contains( (long) i - 1 ), "Missing id " + (i - 1) );
            }
        }
    }

    @Test
    void shouldUseDefragIdsFirst()
    {
        // given
        int rangeLength = 1024;
        IdRangeIterator iterator = new IdRange( new long[] {7, 8, 9}, 1024, rangeLength ).iterator();

        // then
        assertEquals( 7, iterator.nextId( NULL ) );
        assertEquals( 8, iterator.nextId( NULL ) );
        assertEquals( 9, iterator.nextId( NULL ) );
        assertEquals( 1024, iterator.nextId( NULL ) );
    }

    @Test
    void shouldGetNextIdBatchFromOnlyDefragIds()
    {
        // given
        IdRangeIterator iterator = new IdRange( new long[] {1, 2, 3, 4, 5, 6}, 7, 0 ).iterator();

        // when
        IdRangeIterator subRange = iterator.nextIdBatch( 5, NULL ).iterator();

        // then
        assertEquals( 6, iterator.nextId( NULL ) );
        for ( long i = 0; i < 5; i++ )
        {
            assertEquals( 1 + i, subRange.nextId( NULL ) );
        }
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId( NULL ) );
    }

    @Test
    void shouldGetNextIdBatchFromOnlyDefragIdsWhenSomeDefragIdsHaveAlreadyBeenReturned()
    {
        // given
        IdRangeIterator iterator = new IdRange( new long[] {1, 2, 3, 4, 5, 6}, 7, 0 ).iterator();
        iterator.nextId( NULL );
        iterator.nextId( NULL );

        // when
        IdRangeIterator subRange = iterator.nextIdBatch( 3, NULL ).iterator();

        // then
        assertEquals( 6, iterator.nextId( NULL ) );
        for ( long i = 0; i < 3; i++ )
        {
            assertEquals( 3 + i, subRange.nextId( NULL ) );
        }
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId( NULL ) );
    }

    @Test
    void shouldGetNextIdBatchFromSomeDefragAndSomeRangeIds()
    {
        // given
        IdRangeIterator iterator = new IdRange( new long[] {1, 2, 3}, 10, 5 ).iterator();
        iterator.nextId( NULL );

        // when
        IdRangeIterator subRange = iterator.nextIdBatch( 5, NULL ).iterator();

        // then
        assertEquals( 13, iterator.nextId( NULL ) );
        assertEquals( 2, subRange.nextId( NULL ) );
        assertEquals( 3, subRange.nextId( NULL ) );
        assertEquals( 10, subRange.nextId( NULL ) );
        assertEquals( 11, subRange.nextId( NULL ) );
        assertEquals( 12, subRange.nextId( NULL ) );
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId( NULL ) );
    }

    @Test
    void shouldGetNextIdBatchFromSomeRangeIds()
    {
        // given
        IdRangeIterator iterator = new IdRange( EMPTY_LONG_ARRAY, 0, 20 ).iterator();
        iterator.nextId( NULL );

        // when
        IdRangeIterator subRange = iterator.nextIdBatch( 5, NULL ).iterator();

        // then
        assertEquals( 6, iterator.nextId( NULL ) );
        assertEquals( 1, subRange.nextId( NULL ) );
        assertEquals( 2, subRange.nextId( NULL ) );
        assertEquals( 3, subRange.nextId( NULL ) );
        assertEquals( 4, subRange.nextId( NULL ) );
        assertEquals( 5, subRange.nextId( NULL ) );
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId( NULL ) );

        // when
        subRange = iterator.nextIdBatch( 2, NULL ).iterator();

        // then
        assertEquals( 9, iterator.nextId( NULL ) );
        assertEquals( 7, subRange.nextId( NULL ) );
        assertEquals( 8, subRange.nextId( NULL ) );
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId( NULL ) );
    }

    @Test
    void shouldGetNextIdBatchFromSomeRangeIdsWhenThereAreUsedDefragIds()
    {
        // given
        IdRangeIterator iterator = new IdRange( new long[] {0, 1, 2}, 3, 10 ).iterator();
        iterator.nextId( NULL );
        iterator.nextId( NULL );
        iterator.nextId( NULL );

        // when
        IdRangeIterator subRange = iterator.nextIdBatch( 3, NULL ).iterator();

        // then
        assertEquals( 6, iterator.nextId( NULL ) );
        assertEquals( 3, subRange.nextId( NULL ) );
        assertEquals( 4, subRange.nextId( NULL ) );
        assertEquals( 5, subRange.nextId( NULL ) );
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId( NULL ) );

        // when
        subRange = iterator.nextIdBatch( 3, NULL ).iterator();

        // then
        assertEquals( 10, iterator.nextId( NULL ) );
        assertEquals( 7, subRange.nextId( NULL ) );
        assertEquals( 8, subRange.nextId( NULL ) );
        assertEquals( 9, subRange.nextId( NULL ) );
        assertEquals( VALUE_REPRESENTING_NULL, subRange.nextId( NULL ) );
    }
}
