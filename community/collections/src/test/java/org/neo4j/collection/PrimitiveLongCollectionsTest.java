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
package org.neo4j.collection;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.neo4j.collection.PrimitiveLongCollections.AbstractPrimitiveLongBaseIterator;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;

class PrimitiveLongCollectionsTest
{
    @Test
    void arrayOfItemsAsIterator()
    {
        // GIVEN
        long[] items = new long[] { 2, 5, 234 };

        // WHEN
        LongIterator iterator = PrimitiveLongCollections.iterator( items );

        // THEN
        assertItems( iterator, items );
    }

    @Test
    void filter()
    {
        // GIVEN
        LongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        LongIterator filtered = PrimitiveLongCollections.filter( items, item -> item != 2 );

        // THEN
        assertItems( filtered, 1, 3 );
    }

    @Test
    void indexOf()
    {
        // GIVEN
        Supplier<LongIterator> items = () -> PrimitiveLongCollections.iterator( 10, 20, 30 );

        // THEN
        assertEquals( -1, PrimitiveLongCollections.indexOf( items.get(), 55 ) );
        assertEquals( 0, PrimitiveLongCollections.indexOf( items.get(), 10 ) );
        assertEquals( 1, PrimitiveLongCollections.indexOf( items.get(), 20 ) );
        assertEquals( 2, PrimitiveLongCollections.indexOf( items.get(), 30 ) );
    }

    @Test
    void count()
    {
        // GIVEN
        LongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        int count = PrimitiveLongCollections.count( items );

        // THEN
        assertEquals( 3, count );
    }

    @Test
    void asArray()
    {
        // GIVEN
        LongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        long[] array = PrimitiveLongCollections.asArray( items );

        // THEN
        assertArrayEquals( new long[]{1, 2, 3}, array );
    }

    @Test
    void shouldDeduplicate()
    {
        // GIVEN
        long[] array = new long[] {1L, 1L, 2L, 5L, 6L, 6L};

        // WHEN
        long[] deduped = PrimitiveLongCollections.deduplicate( array );

        // THEN
        assertArrayEquals( new long[] {1L, 2L, 5L, 6L}, deduped );
    }

    @Test
    void shouldDeduplicateWithRandomArrays()
    {
        int arrayLength = 5000;
        int iterations = 10;
        for ( int i = 0; i < iterations; i++ )
        {
            long[] array = ThreadLocalRandom.current().longs( arrayLength, 0, arrayLength ).sorted().toArray();
            long[] dedupedActual = PrimitiveLongCollections.deduplicate( array );
            TreeSet<Long> set = new TreeSet<>();
            for ( long value : array )
            {
                set.add( value );
            }
            long[] dedupedExpected = new long[set.size()];
            Iterator<Long> itr = set.iterator();
            for ( int j = 0; j < dedupedExpected.length; j++ )
            {
                assertTrue( itr.hasNext() );
                dedupedExpected[j] = itr.next();
            }
            assertArrayEquals( dedupedExpected, dedupedActual );
        }
    }

    @Test
    void shouldNotContinueToCallNextOnHasNextFalse()
    {
        // GIVEN
        AtomicLong count = new AtomicLong( 2 );
        LongIterator iterator = new AbstractPrimitiveLongBaseIterator()
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
    void convertJavaCollectionToSetOfPrimitives()
    {
        List<Long> longs = asList( 1L, 4L, 7L );
        LongSet longSet = PrimitiveLongCollections.asSet( longs );
        assertTrue( longSet.contains( 1L ) );
        assertTrue( longSet.contains( 4L ) );
        assertTrue( longSet.contains( 7L ) );
        assertEquals( 3, longSet.size() );
    }

    @Test
    void convertPrimitiveSetToJavaSet()
    {
        LongSet longSet = newSetWith( 1L, 3L, 5L );
        Set<Long> longs = PrimitiveLongCollections.toSet( longSet );
        assertThat( longs ).contains( 1L, 3L, 5L );
    }

    @Test
    void mergeLongIterableToSet()
    {
        assertThat( mergeToSet( new LongHashSet(), new LongHashSet() ) ).isEqualTo( new LongHashSet() );
        assertThat( mergeToSet( newSetWith( 1, 2, 3 ), new LongHashSet() ) ).isEqualTo( newSetWith( 1, 2, 3 ) );
        assertThat( mergeToSet( newSetWith( 1, 2, 3 ), newSetWith( 1, 2, 3, 4, 5, 6 ) ) ).isEqualTo( newSetWith( 1, 2, 3, 4, 5, 6 ) );
        assertThat( mergeToSet( newSetWith( 1, 2, 3 ), newSetWith( 4, 5, 6 ) ) ).isEqualTo( newSetWith( 1, 2, 3, 4, 5, 6 ) );
    }

    private static void assertNoMoreItems( LongIterator iterator )
    {
        assertFalse( iterator.hasNext(), iterator + " should have no more items" );
        assertThrows( NoSuchElementException.class, iterator::next );
    }

    private static void assertNextEquals( long expected, LongIterator iterator )
    {
        assertTrue( iterator.hasNext(), iterator + " should have had more items" );
        assertEquals( expected, iterator.next() );
    }

    private static void assertItems( LongIterator iterator, long... expectedItems )
    {
        for ( long expectedItem : expectedItems )
        {
            assertNextEquals( expectedItem, iterator );
        }
        assertNoMoreItems( iterator );
    }
}
