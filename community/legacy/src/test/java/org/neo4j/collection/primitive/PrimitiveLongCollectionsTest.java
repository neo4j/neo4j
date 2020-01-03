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

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimitiveLongCollectionsTest
{
    @Test
    void arrayOfItemsAsIterator()
    {
        // GIVEN
        long[] items = new long[]{2, 5, 234};

        // WHEN
        PrimitiveLongIterator iterator = PrimitiveLongCollections.iterator( items );

        // THEN
        assertItems( iterator, items );
    }

    @Test
    void filter()
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        PrimitiveLongIterator filtered = PrimitiveLongCollections.filter( items, item -> item != 2 );

        // THEN
        assertItems( filtered, 1, 3 );
    }

    private static final class CountingPrimitiveLongIteratorResource implements PrimitiveLongIterator, AutoCloseable
    {
        private final PrimitiveLongIterator delegate;
        private final AtomicInteger closeCounter;

        private CountingPrimitiveLongIteratorResource( PrimitiveLongIterator delegate, AtomicInteger closeCounter )
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
        public long next()
        {
            return delegate.next();
        }
    }

    @Test
    void singleWithDefaultMustAutoCloseIterator()
    {
        AtomicInteger counter = new AtomicInteger();
        CountingPrimitiveLongIteratorResource itr = new CountingPrimitiveLongIteratorResource(
                PrimitiveLongCollections.iterator( 13 ), counter );
        assertEquals( PrimitiveLongCollections.single( itr, 2 ), 13 );
        assertEquals( 1, counter.get() );
    }

    @Test
    void singleWithDefaultMustAutoCloseEmptyIterator()
    {
        AtomicInteger counter = new AtomicInteger();
        CountingPrimitiveLongIteratorResource itr = new CountingPrimitiveLongIteratorResource(
                PrimitiveLongCollections.emptyIterator(), counter );
        assertEquals( PrimitiveLongCollections.single( itr, 2 ), 2 );
        assertEquals( 1, counter.get() );
    }

    @Test
    void indexOf()
    {
        // GIVEN
        PrimitiveLongIterable items = () -> PrimitiveLongCollections.iterator( 10, 20, 30 );

        // THEN
        assertEquals( -1, PrimitiveLongCollections.indexOf( items.iterator(), 55 ) );
        assertEquals( 0, PrimitiveLongCollections.indexOf( items.iterator(), 10 ) );
        assertEquals( 1, PrimitiveLongCollections.indexOf( items.iterator(), 20 ) );
        assertEquals( 2, PrimitiveLongCollections.indexOf( items.iterator(), 30 ) );
    }

    @Test
    void iteratorAsSet()
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        PrimitiveLongSet set = PrimitiveLongCollections.asSet( items );

        // THEN
        assertTrue( set.contains( 1 ) );
        assertTrue( set.contains( 2 ) );
        assertTrue( set.contains( 3 ) );
        assertFalse( set.contains( 4 ) );
    }

    @Test
    void count()
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        int count = PrimitiveLongCollections.count( items );

        // THEN
        assertEquals( 3, count );
    }

    @Test
    void asArray()
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        long[] array = PrimitiveLongCollections.asArray( items );

        // THEN
        assertTrue( Arrays.equals( new long[]{1, 2, 3}, array ) );
    }

    @Test
    void shouldDeduplicate()
    {
        // GIVEN
        long[] array = new long[]{1L, 1L, 2L, 5L, 6L, 6L};

        // WHEN
        long[] deduped = PrimitiveLongCollections.deduplicate( array );

        // THEN
        assertArrayEquals( new long[]{1L, 2L, 5L, 6L}, deduped );
    }

    @Test
    void shouldNotContinueToCallNextOnHasNextFalse()
    {
        // GIVEN
        AtomicLong count = new AtomicLong( 2 );
        PrimitiveLongIterator iterator = new PrimitiveLongBaseIterator()
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
    void copyPrimitiveSet()
    {
        PrimitiveLongSet longSet = PrimitiveLongCollections.setOf( 1L, 3L, 5L );
        PrimitiveLongSet copySet = PrimitiveLongCollections.asSet( longSet );
        assertNotSame( copySet, longSet );

        assertTrue( copySet.contains( 1L ) );
        assertTrue( copySet.contains( 3L ) );
        assertTrue( copySet.contains( 5L ) );
        assertEquals( 3, copySet.size() );
    }

    @Test
    void convertJavaCollectionToSetOfPrimitives()
    {
        List<Long> longs = asList( 1L, 4L, 7L );
        PrimitiveLongSet longSet = PrimitiveLongCollections.asSet( longs );
        assertTrue( longSet.contains( 1L ) );
        assertTrue( longSet.contains( 4L ) );
        assertTrue( longSet.contains( 7L ) );
        assertEquals( 3, longSet.size() );
    }

    @Test
    void convertPrimitiveSetToJavaSet()
    {
        PrimitiveLongSet longSet = PrimitiveLongCollections.setOf( 1L, 3L, 5L );
        Set<Long> longs = PrimitiveLongCollections.toSet( longSet );
        assertThat( longs, containsInAnyOrder( 1L, 3L, 5L ) );
    }

    @Test
    void copyMap()
    {
        PrimitiveLongObjectMap<Object> originalMap = Primitive.longObjectMap();
        originalMap.put( 1L, "a" );
        originalMap.put( 2L, "b" );
        originalMap.put( 3L, "c" );
        PrimitiveLongObjectMap<Object> copyMap = PrimitiveLongCollections.copy( originalMap );
        assertNotSame( originalMap, copyMap );
        assertEquals( 3, copyMap.size() );
        assertEquals( "a", copyMap.get( 1L ) );
        assertEquals( "b", copyMap.get( 2L ) );
        assertEquals( "c", copyMap.get( 3L ) );
    }

    private static void assertNoMoreItems( PrimitiveLongIterator iterator )
    {
        assertFalse( iterator.hasNext(), iterator + " should have no more items" );
        assertThrows( NoSuchElementException.class, iterator::next );
    }

    private static void assertNextEquals( long expected, PrimitiveLongIterator iterator )
    {
        assertTrue( iterator.hasNext(), iterator + " should have had more items" );
        assertEquals( expected, iterator.next() );
    }

    private static void assertItems( PrimitiveLongIterator iterator, long... expectedItems )
    {
        for ( long expectedItem: expectedItems )
        {
            assertNextEquals( expectedItem, iterator );
        }
        assertNoMoreItems( iterator );
    }
}
