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
package org.neo4j.collection.primitive;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;

import org.neo4j.graphdb.Resource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrimitiveLongResourceCollectionsTest
{
    private static final LongPredicate EVEN = value -> value % 2 == 0;

    // ITERATOR

    @Test
    void simpleIterator()
    {
        // Given
        CountingResource resource = new CountingResource();
        PrimitiveLongResourceIterator iterator = PrimitiveLongResourceCollections.iterator( resource, 1, 2, 3, 4 );

        // Then
        assertContent( iterator, 1, 2, 3, 4 );

        // When
        iterator.close();

        // Then
        assertEquals( 1, resource.closeCount(), "exactly one call to close" );
    }

    // FILTER

    @Test
    void filterItems()
    {
        // Given
        CountingResource resource = new CountingResource();
        PrimitiveLongResourceIterator iterator = PrimitiveLongResourceCollections.iterator( resource, 1, 2, 3, 4 );

        // When
        PrimitiveLongResourceIterator filtered = PrimitiveLongResourceCollections.filter( iterator, EVEN );

        // Then
        assertContent( filtered, 2, 4 );

        // When
        filtered.close();

        // Then
        assertEquals( 1, resource.closeCount(), "exactly one call to close" );
    }

    // CONCAT

    @Test
    void concatIterators()
    {
        // Given
        CountingResource resource = new CountingResource();
        PrimitiveLongResourceIterator first = PrimitiveLongResourceCollections.iterator( resource, 1, 2 );
        PrimitiveLongResourceIterator second = PrimitiveLongResourceCollections.iterator( resource, 3, 4 );

        // When
        PrimitiveLongResourceIterator concat = PrimitiveLongResourceCollections.concat( first, second );

        // Then
        assertContent( concat, 1, 2, 3, 4 );

        // When
        concat.close();

        // Then
        assertEquals( 2, resource.closeCount(), "all concatenated iterators are closed" );
    }

    private static void assertContent( PrimitiveLongResourceIterator iterator, long... expected )
    {
        int i = 0;
        while ( iterator.hasNext() )
        {
            assertEquals( expected[i++], iterator.next(), "has expected value" );
        }
        assertEquals( expected.length, i, "has all expected values" );
    }

    private static class CountingResource implements Resource
    {
        private AtomicInteger closed = new AtomicInteger();

        @Override
        public void close()
        {
            closed.incrementAndGet();
        }

        int closeCount()
        {
            return closed.get();
        }
    }
}
