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

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;

import org.neo4j.graphdb.Resource;

import static org.junit.Assert.assertEquals;

public class PrimitiveLongResourceCollectionsTest
{
    private static final LongPredicate EVEN = value -> value % 2 == 0;

    // ITERATOR

    @Test
    public void simpleIterator()
    {
        // Given
        CountingResource resource = new CountingResource();
        PrimitiveLongResourceIterator iterator = PrimitiveLongResourceCollections.iterator( resource, 1, 2, 3, 4 );

        // Then
        assertContent( iterator, 1, 2, 3, 4 );

        // When
        iterator.close();

        // Then
        assertEquals( "exactly one call to close", 1, resource.closeCount() );
    }

    // FILTER

    @Test
    public void filterItems()
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
        assertEquals( "exactly one call to close", 1, resource.closeCount() );
    }

    // CONCAT

    @Test
    public void concatIterators()
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
        assertEquals( "all concatenated iterators are closed", 2, resource.closeCount() );
    }

    private void assertContent( PrimitiveLongResourceIterator iterator, long... expected )
    {
        int i = 0;
        while ( iterator.hasNext() )
        {
            assertEquals( "has expected value", expected[i++], iterator.next() );
        }
        assertEquals( "has all expected values", expected.length, i );
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
