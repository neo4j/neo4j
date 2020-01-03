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
package org.neo4j.collection.primitive;

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.memory.GlobalMemoryTracker;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimitiveIntCollectionsTest
{
    @Test
    void arrayOfItemsAsIterator()
    {
        // GIVEN
        int[] items = new int[]{2, 5, 234};

        // WHEN
        PrimitiveIntIterator iterator = PrimitiveIntCollections.iterator( items );

        // THEN
        assertItems( iterator, items );
    }

    @Test
    void convertCollectionToLongArray()
    {
        PrimitiveIntSet heapSet = PrimitiveIntCollections.asSet( new int[]{1, 2, 3} );
        PrimitiveIntSet offHeapIntSet = Primitive.offHeapIntSet( GlobalMemoryTracker.INSTANCE );
        offHeapIntSet.add( 7 );
        offHeapIntSet.add( 8 );
        assertArrayEquals( new long[]{1, 2, 3}, PrimitiveIntCollections.asLongArray( heapSet ) );
        assertArrayEquals( new long[]{7, 8}, PrimitiveIntCollections.asLongArray( offHeapIntSet ) );
    }

    @Test
    void concatenateTwoIterators()
    {
        // GIVEN
        PrimitiveIntIterator firstItems = PrimitiveIntCollections.iterator( 10, 3, 203, 32 );
        PrimitiveIntIterator otherItems = PrimitiveIntCollections.iterator( 1, 2, 5 );

        // WHEN
        PrimitiveIntIterator iterator = PrimitiveIntCollections.concat( asList( firstItems, otherItems ).iterator() );

        // THEN
        assertItems( iterator, 10, 3, 203, 32, 1, 2, 5 );
    }

    @Test
    void filter()
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2, 3 );

        // WHEN
        PrimitiveIntIterator filtered = PrimitiveIntCollections.filter( items, item -> item != 2 );

        // THEN
        assertItems( filtered, 1, 3 );
    }

    @Test
    void deduplicate()
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 1, 2, 3, 2 );

        // WHEN
        PrimitiveIntIterator deduped = PrimitiveIntCollections.deduplicate( items );

        // THEN
        assertItems( deduped, 1, 2, 3 );
    }

    private static final class CountingPrimitiveIntIteratorResource implements PrimitiveIntIterator, AutoCloseable
    {
        private final PrimitiveIntIterator delegate;
        private final AtomicInteger closeCounter;

        private CountingPrimitiveIntIteratorResource( PrimitiveIntIterator delegate, AtomicInteger closeCounter )
        {
            this.delegate = delegate;
            this.closeCounter = closeCounter;
        }

        @Override
        public void close()
        {
            closeCounter.incrementAndGet();
        }

        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        @Override
        public int next()
        {
            return delegate.next();
        }
    }

    @Test
    void iteratorAsSet()
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2, 3 );

        // WHEN
        PrimitiveIntSet set = PrimitiveIntCollections.asSet( items );

        // THEN
        assertTrue( set.contains( 1 ) );
        assertTrue( set.contains( 2 ) );
        assertTrue( set.contains( 3 ) );
        assertFalse( set.contains( 4 ) );
        assertThrows( IllegalStateException.class, () -> PrimitiveIntCollections.asSet( PrimitiveIntCollections.iterator( 1, 2, 1 ) ) );
    }

    @Test
    void shouldNotContinueToCallNextOnHasNextFalse()
    {
        // GIVEN
        AtomicInteger count = new AtomicInteger( 2 );
        PrimitiveIntIterator iterator = new PrimitiveIntCollections.PrimitiveIntBaseIterator()
        {
            @Override
            protected boolean fetchNext()
            {
                return count.decrementAndGet() >= 0 && next( count.get() );
            }
        };

        // WHEN/THEN
        assertTrue( iterator.hasNext() );
        assertTrue( iterator.hasNext() );
        assertEquals( 1L, iterator.next() );
        assertTrue( iterator.hasNext() );
        assertTrue( iterator.hasNext() );
        assertEquals( 0L, iterator.next() );
        assertFalse( iterator.hasNext() );
        assertFalse( iterator.hasNext() );
        assertEquals( -1L, count.get() );
    }

    @Test
    void shouldDeduplicate()
    {
        // GIVEN
        int[] array = new int[]{1, 6, 2, 5, 6, 1, 6};

        // WHEN
        int[] deduped = PrimitiveIntCollections.deduplicate( array );

        // THEN
        assertArrayEquals( new int[]{1, 6, 2, 5}, deduped );
    }

    @Test
    void copyMap()
    {
        PrimitiveIntObjectMap<Object> originalMap = Primitive.intObjectMap();
        originalMap.put( 1, "a" );
        originalMap.put( 2, "b" );
        originalMap.put( 3, "c" );
        PrimitiveIntObjectMap<Object> copyMap = PrimitiveIntCollections.copy( originalMap );
        assertNotSame( originalMap, copyMap );
        assertEquals( 3, copyMap.size() );
        assertEquals( "a", copyMap.get( 1 ) );
        assertEquals( "b", copyMap.get( 2 ) );
        assertEquals( "c", copyMap.get( 3 ) );
    }

    private static void assertNoMoreItems( PrimitiveIntIterator iterator )
    {
        assertFalse( iterator.hasNext(), iterator + " should have no more items" );
        assertThrows( NoSuchElementException.class, iterator::next );
    }

    private static void assertNextEquals( long expected, PrimitiveIntIterator iterator )
    {
        assertTrue( iterator.hasNext(), iterator + " should have had more items" );
        assertEquals( expected, iterator.next() );
    }

    private static void assertItems( PrimitiveIntIterator iterator, int... expectedItems )
    {
        for ( long expectedItem: expectedItems )
        {
            assertNextEquals( expectedItem, iterator );
        }
        assertNoMoreItems( iterator );
    }
}
