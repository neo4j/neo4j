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

import org.neo4j.function.LongPredicate;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrimitiveLongCollectionsTest
{
    @Test
    public void arrayOfItemsAsIterator() throws Exception
    {
        // GIVEN
        long[] items = new long[] { 2, 5, 234 };

        // WHEN
        PrimitiveLongIterator iterator = PrimitiveLongCollections.iterator( items );

        // THEN
        assertItems( iterator, items );
    }

    @Test
    public void arrayOfReversedItemsAsIterator() throws Exception
    {
        // GIVEN
        long[] items = new long[] { 2, 5, 234 };

        // WHEN
        PrimitiveLongIterator iterator = PrimitiveLongCollections.reversed( items );

        // THEN
        assertItems( iterator, reverse( items ) );
    }

    @Test
    public void concatenateTwoIterators() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator firstItems = PrimitiveLongCollections.iterator( 10, 3, 203, 32 );
        PrimitiveLongIterator otherItems = PrimitiveLongCollections.iterator( 1, 2, 5 );

        // WHEN
        PrimitiveLongIterator iterator = PrimitiveLongCollections.concat( asList( firstItems, otherItems ).iterator() );

        // THEN
        assertItems( iterator, 10, 3, 203, 32, 1, 2, 5 );
    }

    @Test
    public void prependItem() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 10, 23 );
        long prepended = 5;

        // WHEN
        PrimitiveLongIterator iterator = PrimitiveLongCollections.prepend( prepended, items );

        // THEN
        assertItems( iterator, prepended, 10, 23 );
    }

    @Test
    public void appendItem() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2 );
        long appended = 3;

        // WHEN
        PrimitiveLongIterator iterator = PrimitiveLongCollections.append( items, appended );

        // THEN
        assertItems( iterator, 1, 2, appended );
    }

    @Test
    public void filter() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        PrimitiveLongIterator filtered = PrimitiveLongCollections.filter( items, new LongPredicate()
        {
            @Override
            public boolean test( long item )
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
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 1, 2, 3, 2 );

        // WHEN
        PrimitiveLongIterator deduped = PrimitiveLongCollections.dedup( items );

        // THEN
        assertItems( deduped, 1, 2, 3 );
    }

    @Test
    public void limit() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        PrimitiveLongIterator limited = PrimitiveLongCollections.limit( items, 2 );

        // THEN
        assertItems( limited, 1, 2 );
    }

    @Test
    public void skip() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3, 4 );

        // WHEN
        PrimitiveLongIterator skipped = PrimitiveLongCollections.skip( items, 2 );

        // THEN
        assertItems( skipped, 3, 4 );
    }

    // TODO paging iterator

    @Test
    public void range() throws Exception
    {
        // WHEN
        PrimitiveLongIterator range = PrimitiveLongCollections.range( 5, 15, 3 );

        // THEN
        assertItems( range, 5, 8, 11, 14 );
    }

    @Test
    public void singleton() throws Exception
    {
        // GIVEN
        long item = 15;

        // WHEN
        PrimitiveLongIterator singleton = PrimitiveLongCollections.singleton( item );

        // THEN
        assertItems( singleton, item );
    }

    @Test
    public void reversed() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        PrimitiveLongIterator reversed = PrimitiveLongCollections.reversed( items );

        // THEN
        assertItems( reversed, 3, 2, 1 );
    }

    @Test
    public void first() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2 );

        // WHEN
        try
        {
            PrimitiveLongCollections.first(  PrimitiveLongCollections.emptyIterator() );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {   // Good
        }
        long first = PrimitiveLongCollections.first( items );

        // THEN
        assertEquals( 1, first );
    }

    @Test
    public void firstWithDefault() throws Exception
    {
        // GIVEN
        long defaultValue = 5;

        // WHEN
        long firstOnEmpty = PrimitiveLongCollections.first( PrimitiveLongCollections.emptyIterator(), defaultValue );
        long first = PrimitiveLongCollections.first( PrimitiveLongCollections.iterator( 1, 2 ), defaultValue );

        // THEN
        assertEquals( defaultValue, firstOnEmpty );
        assertEquals( 1, first );
    }

    @Test
    public void last() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2 );

        // WHEN
        try
        {
            PrimitiveLongCollections.last( PrimitiveLongCollections.emptyIterator() );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {   // Good
        }
        long last = PrimitiveLongCollections.last( items );

        // THEN
        assertEquals( 2, last );
    }

    @Test
    public void lastWithDefault() throws Exception
    {
        // GIVEN
        long defaultValue = 5;

        // WHEN
        long lastOnEmpty = PrimitiveLongCollections.last( PrimitiveLongCollections.emptyIterator(), defaultValue );
        long last = PrimitiveLongCollections.last( PrimitiveLongCollections.iterator( 1, 2 ), defaultValue );

        // THEN
        assertEquals( defaultValue, lastOnEmpty );
        assertEquals( 2, last );
    }

    @Test
    public void single() throws Exception
    {
        try
        {
            PrimitiveLongCollections.single( PrimitiveLongCollections.emptyIterator() );
        }
        catch ( NoSuchElementException e )
        {
            assertThat( e.getMessage(), containsString( "No" ) );
        }

        assertEquals( 3, PrimitiveLongCollections.single( PrimitiveLongCollections.iterator( 3 ) ) );

        try
        {
            PrimitiveLongCollections.single( PrimitiveLongCollections.iterator( 1, 2 ) );
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
        assertEquals( 5, PrimitiveLongCollections.single( PrimitiveLongCollections.emptyIterator(), 5 ) );
        assertEquals( 3, PrimitiveLongCollections.single( PrimitiveLongCollections.iterator( 3 ) ) );
        try
        {
            PrimitiveLongCollections.single( PrimitiveLongCollections.iterator( 1, 2 ) );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {   // Good
            assertThat( e.getMessage(), containsString( "More than one" ) );
        }
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
        public long next()
        {
            return delegate.next();
        }
    }

    @Test
    public void singleMustAutoCloseIterator()
    {
        AtomicInteger counter = new AtomicInteger();
        CountingPrimitiveLongIteratorResource itr = new CountingPrimitiveLongIteratorResource(
                PrimitiveLongCollections.iterator( 13 ), counter );
        assertEquals( PrimitiveLongCollections.single( itr ), 13 );
        assertEquals( 1, counter.get() );
    }

    @Test
    public void singleWithDefaultMustAutoCloseIterator()
    {
        AtomicInteger counter = new AtomicInteger();
        CountingPrimitiveLongIteratorResource itr = new CountingPrimitiveLongIteratorResource(
                PrimitiveLongCollections.iterator( 13 ), counter );
        assertEquals( PrimitiveLongCollections.single( itr, 2 ), 13 );
        assertEquals( 1, counter.get() );
    }

    @Test
    public void singleMustAutoCloseEmptyIterator()
    {
        AtomicInteger counter = new AtomicInteger();
        CountingPrimitiveLongIteratorResource itr = new CountingPrimitiveLongIteratorResource(
                PrimitiveLongCollections.emptyIterator(), counter );
        try
        {
            PrimitiveLongCollections.single( itr );
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
        CountingPrimitiveLongIteratorResource itr = new CountingPrimitiveLongIteratorResource(
                PrimitiveLongCollections.emptyIterator(), counter );
        assertEquals( PrimitiveLongCollections.single( itr, 2 ), 2 );
        assertEquals( 1, counter.get() );
    }

    @Test
    public void itemAt() throws Exception
    {
        // GIVEN
        PrimitiveLongIterable items = new PrimitiveLongIterable()
        {
            @Override
            public PrimitiveLongIterator iterator()
            {
                return PrimitiveLongCollections.iterator( 10, 20, 30 );
            }
        };

        // THEN
        try
        {
            PrimitiveLongCollections.itemAt( items.iterator(), 3 );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {
            assertThat( e.getMessage(), containsString( "No element" ) );
        }
        try
        {
            PrimitiveLongCollections.itemAt( items.iterator(), -4 );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {
            assertThat( e.getMessage(), containsString( "not found" ) );
        }
        assertEquals( 10, PrimitiveLongCollections.itemAt( items.iterator(), 0 ) );
        assertEquals( 20, PrimitiveLongCollections.itemAt( items.iterator(), 1 ) );
        assertEquals( 30, PrimitiveLongCollections.itemAt( items.iterator(), 2 ) );
        assertEquals( 30, PrimitiveLongCollections.itemAt( items.iterator(), -1 ) );
        assertEquals( 20, PrimitiveLongCollections.itemAt( items.iterator(), -2 ) );
        assertEquals( 10, PrimitiveLongCollections.itemAt( items.iterator(), -3 ) );
    }

    @Test
    public void itemAtWithDefault() throws Exception
    {
        // GIVEN
        PrimitiveLongIterable items = new PrimitiveLongIterable()
        {
            @Override
            public PrimitiveLongIterator iterator()
            {
                return PrimitiveLongCollections.iterator( 10, 20, 30 );
            }
        };
        long defaultValue = 55;

        // THEN
        assertEquals( defaultValue, PrimitiveLongCollections.itemAt( items.iterator(), 3, defaultValue ) );
        assertEquals( defaultValue, PrimitiveLongCollections.itemAt( items.iterator(), -4, defaultValue ) );
        assertEquals( 10, PrimitiveLongCollections.itemAt( items.iterator(), 0 ) );
        assertEquals( 20, PrimitiveLongCollections.itemAt( items.iterator(), 1 ) );
        assertEquals( 30, PrimitiveLongCollections.itemAt( items.iterator(), 2 ) );
        assertEquals( 30, PrimitiveLongCollections.itemAt( items.iterator(), -1 ) );
        assertEquals( 20, PrimitiveLongCollections.itemAt( items.iterator(), -2 ) );
        assertEquals( 10, PrimitiveLongCollections.itemAt( items.iterator(), -3 ) );
    }

    @Test
    public void indexOf() throws Exception
    {
        // GIVEN
        PrimitiveLongIterable items = new PrimitiveLongIterable()
        {
            @Override
            public PrimitiveLongIterator iterator()
            {
                return PrimitiveLongCollections.iterator( 10, 20, 30 );
            }
        };

        // THEN
        assertEquals( -1, PrimitiveLongCollections.indexOf( items.iterator(), 55 ) );
        assertEquals( 0, PrimitiveLongCollections.indexOf( items.iterator(), 10 ) );
        assertEquals( 1, PrimitiveLongCollections.indexOf( items.iterator(), 20 ) );
        assertEquals( 2, PrimitiveLongCollections.indexOf( items.iterator(), 30 ) );
    }

    @Test
    public void iteratorsEqual() throws Exception
    {
        // GIVEN
        PrimitiveLongIterable items1 = new PrimitiveLongIterable()
        {
            @Override
            public PrimitiveLongIterator iterator()
            {
                return PrimitiveLongCollections.iterator( 1, 2, 3 );
            }
        };
        PrimitiveLongIterable items2 = new PrimitiveLongIterable()
        {
            @Override
            public PrimitiveLongIterator iterator()
            {
                return PrimitiveLongCollections.iterator( 1, 20, 3 );
            }
        };
        PrimitiveLongIterable items3 = new PrimitiveLongIterable()
        {
            @Override
            public PrimitiveLongIterator iterator()
            {
                return PrimitiveLongCollections.iterator( 1, 2, 3, 4 );
            }
        };
        PrimitiveLongIterable items4 = new PrimitiveLongIterable()
        {
            @Override
            public PrimitiveLongIterator iterator()
            {
                return PrimitiveLongCollections.iterator( 1, 2, 3 );
            }
        };

        // THEN
        assertFalse( PrimitiveLongCollections.equals( items1.iterator(), items2.iterator() ) );
        assertFalse( PrimitiveLongCollections.equals( items1.iterator(), items3.iterator() ) );
        assertTrue( PrimitiveLongCollections.equals( items1.iterator(), items4.iterator() ) );
    }

    @Test
    public void iteratorAsSet() throws Exception
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
        try
        {
            PrimitiveLongCollections.asSet( PrimitiveLongCollections.iterator( 1, 2, 1 ) );
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
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 1 );

        // WHEN
        PrimitiveLongSet set = PrimitiveLongCollections.asSetAllowDuplicates( items );

        // THEN
        assertTrue( set.contains( 1 ) );
        assertTrue( set.contains( 2 ) );
        assertFalse( set.contains( 3 ) );
    }

    @Test
    public void count() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        int count = PrimitiveLongCollections.count( items );

        // THEN
        assertEquals( 3, count );
    }

    @Test
    public void asArray() throws Exception
    {
        // GIVEN
        PrimitiveLongIterator items = PrimitiveLongCollections.iterator( 1, 2, 3 );

        // WHEN
        long[] array = PrimitiveLongCollections.asArray( items );

        // THEN
        assertTrue( Arrays.equals( new long[] { 1, 2, 3 }, array ) );
    }

    private void assertNoMoreItems( PrimitiveLongIterator iterator )
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

    private void assertNextEquals( long expected, PrimitiveLongIterator iterator )
    {
        assertTrue( iterator + " should have had more items", iterator.hasNext() );
        assertEquals( expected, iterator.next() );
    }

    private void assertItems( PrimitiveLongIterator iterator, long... expectedItems )
    {
        for ( long expectedItem : expectedItems )
        {
            assertNextEquals( expectedItem, iterator );
        }
        assertNoMoreItems( iterator );
    }

    private long[] reverse( long[] items )
    {
        long[] result = new long[items.length];
        for ( int i = 0; i < items.length; i++ )
        {
            result[i] = items[items.length-i-1];
        }
        return result;
    }
}
