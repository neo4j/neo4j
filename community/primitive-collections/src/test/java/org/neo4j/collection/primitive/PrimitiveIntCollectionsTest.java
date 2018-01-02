/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.collection.primitive;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.neo4j.function.IntPredicate;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrimitiveIntCollectionsTest
{
    @Test
    public void arrayOfItemsAsIterator() throws Exception
    {
        // GIVEN
        int[] items = new int[] { 2, 5, 234 };

        // WHEN
        PrimitiveIntIterator iterator = PrimitiveIntCollections.iterator( items );

        // THEN
        assertItems( iterator, items );
    }

    @Test
    public void arrayOfReversedItemsAsIterator() throws Exception
    {
        // GIVEN
        int[] items = new int[] { 2, 5, 234 };

        // WHEN
        PrimitiveIntIterator iterator = PrimitiveIntCollections.reversed( items );

        // THEN
        assertItems( iterator, reverse( items ) );
    }

    @Test
    public void concatenateTwoIterators() throws Exception
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
    public void prependItem() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 10, 23 );
        int prepended = 5;

        // WHEN
        PrimitiveIntIterator iterator = PrimitiveIntCollections.prepend( prepended, items );

        // THEN
        assertItems( iterator, prepended, 10, 23 );
    }

    @Test
    public void appendItem() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2 );
        int appended = 3;

        // WHEN
        PrimitiveIntIterator iterator = PrimitiveIntCollections.append( items, appended );

        // THEN
        assertItems( iterator, 1, 2, appended );
    }

    @Test
    public void filter() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2, 3 );

        // WHEN
        PrimitiveIntIterator filtered = PrimitiveIntCollections.filter( items, new IntPredicate()
        {
            @Override
            public boolean test( int item )
            {
                return item != 2;
            }
        } );

        // THEN
        assertItems( filtered, 1, 3 );
    }

    @Test
    public void dedup() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 1, 2, 3, 2 );

        // WHEN
        PrimitiveIntIterator deduped = PrimitiveIntCollections.dedup( items );

        // THEN
        assertItems( deduped, 1, 2, 3 );
    }

    @Test
    public void limit() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2, 3 );

        // WHEN
        PrimitiveIntIterator limited = PrimitiveIntCollections.limit( items, 2 );

        // THEN
        assertItems( limited, 1, 2 );
    }

    @Test
    public void skip() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2, 3, 4 );

        // WHEN
        PrimitiveIntIterator skipped = PrimitiveIntCollections.skip( items, 2 );

        // THEN
        assertItems( skipped, 3, 4 );
    }

    // TODO paging iterator

    @Test
    public void range() throws Exception
    {
        // WHEN
        PrimitiveIntIterator range = PrimitiveIntCollections.range( 5, 15, 3 );

        // THEN
        assertItems( range, 5, 8, 11, 14 );
    }

    @Test
    public void singleton() throws Exception
    {
        // GIVEN
        int item = 15;

        // WHEN
        PrimitiveIntIterator singleton = PrimitiveIntCollections.singleton( item );

        // THEN
        assertItems( singleton, item );
    }

    @Test
    public void reversed() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2, 3 );

        // WHEN
        PrimitiveIntIterator reversed = PrimitiveIntCollections.reversed( items );

        // THEN
        assertItems( reversed, 3, 2, 1 );
    }

    @Test
    public void first() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2 );

        // WHEN
        try
        {
            PrimitiveIntCollections.first(  PrimitiveIntCollections.emptyIterator() );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {   // Good
        }
        long first = PrimitiveIntCollections.first( items );

        // THEN
        assertEquals( 1, first );
    }

    @Test
    public void firstWithDefault() throws Exception
    {
        // GIVEN
        int defaultValue = 5;

        // WHEN
        int firstOnEmpty = PrimitiveIntCollections.first( PrimitiveIntCollections.emptyIterator(), defaultValue );
        int first = PrimitiveIntCollections.first( PrimitiveIntCollections.iterator( 1, 2 ), defaultValue );

        // THEN
        assertEquals( defaultValue, firstOnEmpty );
        assertEquals( 1, first );
    }

    @Test
    public void last() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2 );

        // WHEN
        try
        {
            PrimitiveIntCollections.last( PrimitiveIntCollections.emptyIterator() );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {   // Good
        }
        long last = PrimitiveIntCollections.last( items );

        // THEN
        assertEquals( 2, last );
    }

    @Test
    public void lastWithDefault() throws Exception
    {
        // GIVEN
        int defaultValue = 5;

        // WHEN
        int lastOnEmpty = PrimitiveIntCollections.last( PrimitiveIntCollections.emptyIterator(), defaultValue );
        int last = PrimitiveIntCollections.last( PrimitiveIntCollections.iterator( 1, 2 ), defaultValue );

        // THEN
        assertEquals( defaultValue, lastOnEmpty );
        assertEquals( 2, last );
    }

    @Test
    public void single() throws Exception
    {
        try
        {
            PrimitiveIntCollections.single( PrimitiveIntCollections.emptyIterator() );
        }
        catch ( NoSuchElementException e )
        {
            assertThat( e.getMessage(), containsString( "No" ) );
        }

        assertEquals( 3, PrimitiveIntCollections.single( PrimitiveIntCollections.iterator( 3 ) ) );

        try
        {
            PrimitiveIntCollections.single( PrimitiveIntCollections.iterator( 1, 2 ) );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {
            assertThat( e.getMessage(), containsString( "More than one" ) );
        }
    }

    @Test
    public void singleWithDefault() throws Exception
    {
        assertEquals( 5, PrimitiveIntCollections.single( PrimitiveIntCollections.emptyIterator(), 5 ) );
        assertEquals( 3, PrimitiveIntCollections.single( PrimitiveIntCollections.iterator( 3 ) ) );
        try
        {
            PrimitiveIntCollections.single( PrimitiveIntCollections.iterator( 1, 2 ) );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {   // Good
            assertThat( e.getMessage(), containsString( "More than one" ) );
        }
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
        public void close() throws Exception
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
    public void singleMustAutoCloseIterator()
    {
        AtomicInteger counter = new AtomicInteger();
        CountingPrimitiveIntIteratorResource itr = new CountingPrimitiveIntIteratorResource(
                PrimitiveIntCollections.iterator( 13 ), counter );
        assertEquals( PrimitiveIntCollections.single( itr ), 13 );
        assertEquals( 1, counter.get() );
    }

    @Test
    public void singleWithDefaultMustAutoCloseIterator()
    {
        AtomicInteger counter = new AtomicInteger();
        CountingPrimitiveIntIteratorResource itr = new CountingPrimitiveIntIteratorResource(
                PrimitiveIntCollections.iterator( 13 ), counter );
        assertEquals( PrimitiveIntCollections.single( itr, 2 ), 13 );
        assertEquals( 1, counter.get() );
    }

    @Test
    public void singleMustAutoCloseEmptyIterator()
    {
        AtomicInteger counter = new AtomicInteger();
        CountingPrimitiveIntIteratorResource itr = new CountingPrimitiveIntIteratorResource(
                PrimitiveIntCollections.emptyIterator(), counter );
        try
        {
            PrimitiveIntCollections.single( itr );
            fail( "single() on empty iterator should have thrown" );
        }
        catch ( NoSuchElementException ignore )
        {
        }
        assertEquals( 1, counter.get() );
    }

    @Test
    public void singleWithDefaultMustAutoCloseEmptyIterator()
    {
        AtomicInteger counter = new AtomicInteger();
        CountingPrimitiveIntIteratorResource itr = new CountingPrimitiveIntIteratorResource(
                PrimitiveIntCollections.emptyIterator(), counter );
        assertEquals( PrimitiveIntCollections.single( itr, 2 ), 2 );
        assertEquals( 1, counter.get() );
    }

    @Test
    public void itemAt() throws Exception
    {
        // GIVEN
        PrimitiveIntIterable items = new PrimitiveIntIterable()
        {
            @Override
            public PrimitiveIntIterator iterator()
            {
                return PrimitiveIntCollections.iterator( 10, 20, 30 );
            }
        };

        // THEN
        try
        {
            PrimitiveIntCollections.itemAt( items.iterator(), 3 );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {
            assertThat( e.getMessage(), containsString( "No element" ) );
        }
        try
        {
            PrimitiveIntCollections.itemAt( items.iterator(), -4 );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {
            assertThat( e.getMessage(), containsString( "not found" ) );
        }
        assertEquals( 10, PrimitiveIntCollections.itemAt( items.iterator(), 0 ) );
        assertEquals( 20, PrimitiveIntCollections.itemAt( items.iterator(), 1 ) );
        assertEquals( 30, PrimitiveIntCollections.itemAt( items.iterator(), 2 ) );
        assertEquals( 30, PrimitiveIntCollections.itemAt( items.iterator(), -1 ) );
        assertEquals( 20, PrimitiveIntCollections.itemAt( items.iterator(), -2 ) );
        assertEquals( 10, PrimitiveIntCollections.itemAt( items.iterator(), -3 ) );
    }

    @Test
    public void itemAtWithDefault() throws Exception
    {
        // GIVEN
        PrimitiveIntIterable items = new PrimitiveIntIterable()
        {
            @Override
            public PrimitiveIntIterator iterator()
            {
                return PrimitiveIntCollections.iterator( 10, 20, 30 );
            }
        };
        int defaultValue = 55;

        // THEN
        assertEquals( defaultValue, PrimitiveIntCollections.itemAt( items.iterator(), 3, defaultValue ) );
        assertEquals( defaultValue, PrimitiveIntCollections.itemAt( items.iterator(), -4, defaultValue ) );
        assertEquals( 10, PrimitiveIntCollections.itemAt( items.iterator(), 0 ) );
        assertEquals( 20, PrimitiveIntCollections.itemAt( items.iterator(), 1 ) );
        assertEquals( 30, PrimitiveIntCollections.itemAt( items.iterator(), 2 ) );
        assertEquals( 30, PrimitiveIntCollections.itemAt( items.iterator(), -1 ) );
        assertEquals( 20, PrimitiveIntCollections.itemAt( items.iterator(), -2 ) );
        assertEquals( 10, PrimitiveIntCollections.itemAt( items.iterator(), -3 ) );
    }

    @Test
    public void indexOf() throws Exception
    {
        // GIVEN
        PrimitiveIntIterable items = new PrimitiveIntIterable()
        {
            @Override
            public PrimitiveIntIterator iterator()
            {
                return PrimitiveIntCollections.iterator( 10, 20, 30 );
            }
        };

        // THEN
        assertEquals( -1, PrimitiveIntCollections.indexOf( items.iterator(), 55 ) );
        assertEquals( 0, PrimitiveIntCollections.indexOf( items.iterator(), 10 ) );
        assertEquals( 1, PrimitiveIntCollections.indexOf( items.iterator(), 20 ) );
        assertEquals( 2, PrimitiveIntCollections.indexOf( items.iterator(), 30 ) );
    }

    @Test
    public void iteratorsEqual() throws Exception
    {
        // GIVEN
        PrimitiveIntIterable items1 = new PrimitiveIntIterable()
        {
            @Override
            public PrimitiveIntIterator iterator()
            {
                return PrimitiveIntCollections.iterator( 1, 2, 3 );
            }
        };
        PrimitiveIntIterable items2 = new PrimitiveIntIterable()
        {
            @Override
            public PrimitiveIntIterator iterator()
            {
                return PrimitiveIntCollections.iterator( 1, 20, 3 );
            }
        };
        PrimitiveIntIterable items3 = new PrimitiveIntIterable()
        {
            @Override
            public PrimitiveIntIterator iterator()
            {
                return PrimitiveIntCollections.iterator( 1, 2, 3, 4 );
            }
        };
        PrimitiveIntIterable items4 = new PrimitiveIntIterable()
        {
            @Override
            public PrimitiveIntIterator iterator()
            {
                return PrimitiveIntCollections.iterator( 1, 2, 3 );
            }
        };

        // THEN
        assertFalse( PrimitiveIntCollections.equals( items1.iterator(), items2.iterator() ) );
        assertFalse( PrimitiveIntCollections.equals( items1.iterator(), items3.iterator() ) );
        assertTrue( PrimitiveIntCollections.equals( items1.iterator(), items4.iterator() ) );
    }

    @Test
    public void iteratorAsSet() throws Exception
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
        try
        {
            PrimitiveIntCollections.asSet( PrimitiveIntCollections.iterator( 1, 2, 1 ) );
            fail( "Should fail on duplicates" );
        }
        catch ( IllegalStateException e )
        {   // good
        }
    }

    @Test
    public void iteratorAsSetAllowDuplicates() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2, 1 );

        // WHEN
        PrimitiveIntSet set = PrimitiveIntCollections.asSetAllowDuplicates( items );

        // THEN
        assertTrue( set.contains( 1 ) );
        assertTrue( set.contains( 2 ) );
        assertFalse( set.contains( 3 ) );
    }

    @Test
    public void count() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2, 3 );

        // WHEN
        int count = PrimitiveIntCollections.count( items );

        // THEN
        assertEquals( 3, count );
    }

    @Test
    public void asArray() throws Exception
    {
        // GIVEN
        PrimitiveIntIterator items = PrimitiveIntCollections.iterator( 1, 2, 3 );

        // WHEN
        int[] array = PrimitiveIntCollections.asArray( items );

        // THEN
        assertTrue( Arrays.equals( new int[] { 1, 2, 3 }, array ) );
    }

    private void assertNoMoreItems( PrimitiveIntIterator iterator )
    {
        assertFalse( iterator + " should have no more items", iterator.hasNext() );
        try
        {
            iterator.next();
            fail( "Invoking next() on " + iterator +
                    " which has no items left should have thrown NoSuchElementException" );
        }
        catch ( NoSuchElementException e )
        {   // Good
        }
    }

    private void assertNextEquals( long expected, PrimitiveIntIterator iterator )
    {
        assertTrue( iterator + " should have had more items", iterator.hasNext() );
        assertEquals( expected, iterator.next() );
    }

    private void assertItems( PrimitiveIntIterator iterator, int... expectedItems )
    {
        for ( long expectedItem : expectedItems )
        {
            assertNextEquals( expectedItem, iterator );
        }
        assertNoMoreItems( iterator );
    }

    private int[] reverse( int[] items )
    {
        int[] result = new int[items.length];
        for ( int i = 0; i < items.length; i++ )
        {
            result[i] = items[items.length-i-1];
        }
        return result;
    }
}
