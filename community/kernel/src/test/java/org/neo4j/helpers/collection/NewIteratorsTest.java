/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.helpers.collection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.neo4j.helpers.CloneableInPublic;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.NewIterators.DuplicateItemException;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.NewIterators.iterator;

public class NewIteratorsTest
{
    @Test
    public void baseIteratorShouldEndWithNull() throws Exception
    {
        // GIVEN
        final AtomicReference<Integer> ref = new AtomicReference<Integer>();
        Iterator<Integer> iterator = new NewIterators.BaseIterator<Integer>()
        {
            @Override
            protected Integer fetchNextOrNull()
            {
                return ref.get();
            }
        };
        
        // WHEN
        ref.set( 1 );
        assertNextEquals( 1, iterator );
        ref.set( 2 );
        assertNextEquals( 2, iterator );
        
        // THEN
        ref.set( null );
        assertNoMoreItems( iterator );
    }
    
    @Test
    public void arrayOfItemsAsIterator() throws Exception
    {
        // GIVEN
        String[] items = new String[] {
                "First",
                "Second",
                "Third"
        };
        
        // WHEN
        Iterator<String> iterator = NewIterators.iterator( items );
        
        // THEN
        assertItems( iterator, items );
    }

    @Test
    public void arrayOfReversedItemsAsIterator() throws Exception
    {
        // GIVEN
        String[] items = new String[] {
                "First",
                "Second",
                "Third"
        };
        
        // WHEN
        Iterator<String> iterator = NewIterators.reversed( items );
        
        // THEN
        for ( int i = items.length; i --> 0; )
        {
            assertNextEquals( items[i], iterator );
        }
        assertNoMoreItems( iterator );
    }
    
    // TODO test for caching iterator
    // TODO test for catching iterator
    
    @Test
    public void concatenateTwoIterators() throws Exception
    {
        // GIVEN
        Iterator<String> firstItems = iterator( "First", "Second" );
        Iterator<String> otherItems = iterator( "Third", "Fourth" );
        
        // WHEN
        @SuppressWarnings( "unchecked" )
        Iterable<Iterator<String>> iterators = asList( firstItems, otherItems );
        Iterator<String> iterator = NewIterators.concat( iterators.iterator() );
        
        // THEN
        assertItems( iterator, "First", "Second", "Third", "Fourth" );
    }
    
    @Test
    public void concatenateTwoIterables() throws Exception
    {
        // GIVEN
        Iterable<String> firstItems = asList( "First", "Second" );
        Iterable<String> otherItems = asList( "Third", "Fourth" );
        
        // WHEN
        @SuppressWarnings( "unchecked" )
        Iterable<Iterable<String>> iterables = asList( firstItems, otherItems );
        Iterator<String> iterator = NewIterators.concatIterables( iterables.iterator() );
        
        // THEN
        assertItems( iterator, "First", "Second", "Third", "Fourth" );
    }
    
    @Test
    public void prependItem() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second" );
        String prepended = "Zeroth"; // :)
        
        // WHEN
        Iterator<String> iterator = NewIterators.prepend( prepended, items );
        
        // THEN
        assertItems( iterator, prepended, "First", "Second" );
    }
    
    @Test
    public void appendItem() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second" );
        String appended = "Third";
        
        // WHEN
        Iterator<String> iterator = NewIterators.append( items, appended );
        
        // THEN
        assertItems( iterator, "First", "Second", appended );
    }
    
    @Test
    public void interleaveIterators() throws Exception
    {
        // GIVEN
        Iterator<String> firstIterator = iterator( "1-First", "1-Second", "1-Third" );
        Iterator<String> secondIterator = iterator( "2-First", "2-Second" );
        Iterator<String> thirdIterator = iterator( "3-First", "3-Second", "3-Third", "3-Fourth" );
        @SuppressWarnings( "unchecked" )
        Collection<Iterator<String>> allIterators = asList( firstIterator, secondIterator, thirdIterator );
        
        // WHEN
        Iterator<String> interleaved = NewIterators.interleave( allIterators );
        
        // THEN
        assertItems( interleaved,
                "1-First", "2-First", "3-First",
                "1-Second", "2-Second", "3-Second",
                "1-Third", "3-Third",
                "3-Fourth" );
    }
    
    @Test
    public void filter() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second", "Third" );
        
        // WHEN
        Iterator<String> filtered = NewIterators.filter( items, new Predicate<String>()
        {
            @Override
            public boolean accept( String item )
            {
                return !"Second".equals( item );
            }
        } );
        
        // THEN
        assertItems( filtered, "First", "Third" );
    }
    
    @Test
    public void dedup() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "First", "Second", "Third", "Second" );
        
        // WHEN
        Iterator<String> deduped = NewIterators.dedup( items );
        
        // THEN
        assertItems( deduped, "First", "Second", "Third" );
    }
    
    @Test
    public void filterNulls() throws Exception
    {
        // GIVEN
        Iterator<String> items = asList( "First", null, "Second", null ).iterator();
        
        // WHEN
        Iterator<String> nonNull = NewIterators.notNull( items );
        
        // THEN
        assertItems( nonNull, "First", "Second" );
    }
    
    @Test
    public void limit() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second", "Third" );
        
        // WHEN
        Iterator<String> limited = NewIterators.limit( items, 2 );
        
        // THEN
        assertItems( limited, "First", "Second" );
    }
    
    @Test
    public void skip() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second", "Third", "Fourth" );
        
        // WHEN
        Iterator<String> skipped = NewIterators.skip( items, 2 );
        
        // THEN
        assertItems( skipped, "Third", "Fourth" );
    }
    
    @Test
    public void map() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "1", "2", "3", "4" );
        
        // WHEN
        Iterator<Integer> mapped = NewIterators.map( items, new Function<String,Integer>()
        {
            @Override
            public Integer apply( String from )
            {
                return new Integer( from );
            }
        } );
        
        // THEN
        assertItems( mapped, 1, 2, 3, 4 );
    }
    
    @Test
    public void casting() throws Exception
    {
        // GIVEN
        Iterator<Object> strings = NewIterators.<Object>iterator( "First", "Second", "Third" );
        
        // WHEN
        Iterator<String> casted = NewIterators.cast( strings, String.class );
        
        // THEN
        assertItems( casted, "First", "Second", "Third" );
    }
    
    @Test
    public void nested() throws Exception
    {
        // GIVEN
        Iterator<String> surfaceItems = iterator( "1", "2", "3" );
        
        // WHEN
        Iterator<String> nested = NewIterators.nested( surfaceItems, new Function<String,Iterator<String>>()
        {
            @Override
            public Iterator<String> apply( String from )
            {
                return iterator( from + "-1", from + "-2", from + "-3" );
            }
        } );
        
        // THEN
        assertItems( nested,
                "1-1", "1-2", "1-3",
                "2-1", "2-2", "2-3",
                "3-1", "3-2", "3-3" );
    }
    
    // TODO paging iterator
    
    @Test
    public void intRange() throws Exception
    {
        // WHEN
        Iterator<Integer> range = NewIterators.intRange( 5, 15, 3 );
        
        // THEN
        assertItems( range, 5, 8, 11, 14 );
    }
    
    @Test
    public void singleton() throws Exception
    {
        // GIVEN
        String item = "Item";
        
        // WHEN
        Iterator<String> singleton = NewIterators.singleton( item );
        
        // THEN
        assertItems( singleton, item );
    }
    
    @Test
    public void cloning() throws Exception
    {
        // GIVEN
        Iterator<CloneableThing> things = iterator( new CloneableThing( "First" ), new CloneableThing( "Second" ) );
        
        // WHEN
        Iterator<CloneableThing> cloned = NewIterators.cloning( things, CloneableThing.class );
        
        // THEN
        CloneableThing first = cloned.next();
        assertEquals( "First", first.value );
        assertTrue( first.cloned );
        CloneableThing second = cloned.next();
        assertEquals( "Second", second.value );
        assertTrue( second.cloned );
        assertNoMoreItems( cloned );
    }
    
    @Test
    public void reversed() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second", "Third" );
        
        // WHEN
        Iterator<String> reversed = NewIterators.reversed( items );
        
        // THEN
        assertItems( reversed, "Third", "Second", "First" );
    }
    
    @Test
    public void first() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second" );
        
        // WHEN
        try
        {
            NewIterators.first( iterator() );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {   // Good
        }
        String first = NewIterators.first( items );
        
        // THEN
        assertEquals( "First", first );
    }
    
    @Test
    public void firstWithDefault() throws Exception
    {
        // GIVEN
        String defaultValue = "Default";
        
        // WHEN
        String firstOnEmpty = NewIterators.first( NewIterators.<String>iterator(), defaultValue );
        String first = NewIterators.first( iterator( "First", "Second" ), defaultValue );
        
        // THEN
        assertEquals( defaultValue, firstOnEmpty );
        assertEquals( "First", first );
    }
    
    @Test
    public void last() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second" );
        
        // WHEN
        try
        {
            NewIterators.last( iterator() );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {   // Good
        }
        String last = NewIterators.last( items );
        
        // THEN
        assertEquals( "Second", last );
    }
    
    @Test
    public void lastWithDefault() throws Exception
    {
        // GIVEN
        String defaultValue = "Default";
        
        // WHEN
        String lastOnEmpty = NewIterators.last( NewIterators.<String>iterator(), defaultValue );
        String last = NewIterators.last( iterator( "First", "Second" ), defaultValue );
        
        // THEN
        assertEquals( defaultValue, lastOnEmpty );
        assertEquals( "Second", last );
    }
    
    @Test
    public void single() throws Exception
    {
        try
        {
            NewIterators.single( iterator() );
        }
        catch ( NoSuchElementException e )
        {
            assertThat( e.getMessage(), containsString( "No" ) );
        }
        
        assertEquals( "Single", NewIterators.single( iterator( "Single" ) ) );
        
        try
        {
            NewIterators.single( iterator( "First", "Second" ) );
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
        assertEquals( "Default", NewIterators.single( iterator(), "Default" ) );
        assertEquals( "Single", NewIterators.single( iterator( "Single" ) ) );
        try
        {
            NewIterators.single( iterator( "First", "Second" ) );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {   // Good
            assertThat( e.getMessage(), containsString( "More than one" ) );
        }
    }
    
    @Test
    public void itemAt() throws Exception
    {
        
        // GIVEN
        Iterable<String> items = asList( "First", "Second", "Third" );
        
        // THEN
        try
        {
            NewIterators.itemAt( items.iterator(), 3 );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {
            assertThat( e.getMessage(), containsString( "No element" ) );
        }
        try
        {
            NewIterators.itemAt( items.iterator(), -4 );
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        {
            assertThat( e.getMessage(), containsString( "No element" ) );
        }
        assertEquals( "First", NewIterators.itemAt( items.iterator(), 0 ) );
        assertEquals( "Second", NewIterators.itemAt( items.iterator(), 1 ) );
        assertEquals( "Third", NewIterators.itemAt( items.iterator(), 2 ) );
        assertEquals( "Third", NewIterators.itemAt( items.iterator(), -1 ) );
        assertEquals( "Second", NewIterators.itemAt( items.iterator(), -2 ) );
        assertEquals( "First", NewIterators.itemAt( items.iterator(), -3 ) );
    }
    
    @Test
    public void itemAtWithDefault() throws Exception
    {
        // GIVEN
        Iterable<String> items = asList( "First", "Second", "Third" );
        String defaultValue = "Default";
        
        // THEN
        assertEquals( defaultValue, NewIterators.itemAt( items.iterator(), 3, defaultValue ) );
        assertEquals( defaultValue, NewIterators.itemAt( items.iterator(), -4, defaultValue ) );
        assertEquals( "First", NewIterators.itemAt( items.iterator(), 0 ) );
        assertEquals( "Second", NewIterators.itemAt( items.iterator(), 1 ) );
        assertEquals( "Third", NewIterators.itemAt( items.iterator(), 2 ) );
        assertEquals( "Third", NewIterators.itemAt( items.iterator(), -1 ) );
        assertEquals( "Second", NewIterators.itemAt( items.iterator(), -2 ) );
        assertEquals( "First", NewIterators.itemAt( items.iterator(), -3 ) );
    }
    
    @Test
    public void indexOf() throws Exception
    {
        // GIVEN
        Iterable<String> items = asList( "First", "Second", "Third" );
        
        // THEN
        assertEquals( -1, NewIterators.indexOf( "Something", items.iterator() ) );
        assertEquals( 0, NewIterators.indexOf( "First", items.iterator() ) );
        assertEquals( 1, NewIterators.indexOf( "Second", items.iterator() ) );
        assertEquals( 2, NewIterators.indexOf( "Third", items.iterator() ) );
    }
    
    @Test
    public void iteratorsEqual() throws Exception
    {
        // GIVEN
        List<String> items1 = asList( "First", "Second", "Third" );
        List<String> items2 = asList( "First", "Bacond", "Third" );
        List<String> items3 = asList( "First", "Second", "Third", "Fourth" );
        List<String> items4 = asList( "First", "Second", "Third" );
        
        // THEN
        assertFalse( NewIterators.equals( items1.iterator(), items2.iterator() ) );
        assertFalse( NewIterators.equals( items1.iterator(), items3.iterator() ) );
        assertTrue( NewIterators.equals( items1.iterator(), items4.iterator() ) );
    }
    
    @Test
    public void addToCollection() throws Exception
    {
        // GIVEN
        Collection<String> collection = new ArrayList<String>( asList( "First", "Second" ) );
        
        // WHEN
        collection = NewIterators.addToCollection( iterator( "Third", "Fourth" ), collection );
        
        // THEN
        assertItems( collection.iterator(), "First", "Second", "Third", "Fourth" );
    }
    
    @Test
    public void addToCollectionUnique() throws Exception
    {
        // GIVEN
        Collection<String> collection = new HashSet<String>( asList( "First", "Second" ) );
        
        // WHEN
        collection = NewIterators.addToCollection( iterator( "Third", "Fourth" ), collection );
        
        // THEN
        assertEquals( collection, new HashSet<String>( asList( "First", "Second", "Third", "Fourth" ) ) );
        try
        {
            NewIterators.addToCollectionAssertChanged( iterator( "Second" ), collection );
            fail( "Should not be able to add that one again" );
        }
        catch ( DuplicateItemException e )
        {   // good
        }
    }
    
    @Test
    public void iteratorAsList() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second", "Third" );
        
        // WHEN
        List<String> list = NewIterators.asList( items );
        
        // THEN
        assertItems( list.iterator(), "First", "Second", "Third" );
    }
    
    @Test
    public void iteratorAsSet() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second", "Third" );
        
        // WHEN
        Set<String> set = NewIterators.asSet( items );
        
        // THEN
        assertEquals( new HashSet<String>( asList( "First", "Second", "Third" ) ), set );
        try
        {
            NewIterators.asSet( iterator( "First", "Second", "First" ) );
            fail( "Should fail on duplicates" );
        }
        catch ( IllegalStateException e )
        {   // good
        }
    }
    
    @Test
    public void iteratorAsSetALlowDuplicates() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second", "First" );
        
        // WHEN
        Set<String> set = NewIterators.asSetAllowDuplicates( items );
        
        // THEN
        assertEquals( new HashSet<String>( asList( "First", "Second" ) ), set );
    }
    
    @Test
    public void loop() throws Exception
    {
        // GIVEN
        String[] items = new String[] { "First", "Second", "Third" };
        Iterator<String> iterator = iterator( items );
        
        // WHEN
        int i = 0;
        for ( String item : NewIterators.loop( iterator ) )
        {
            // THEN
            assertEquals( items[i++], item );
        }
        assertEquals( items.length, i );
    }
    
    @Test
    public void count() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second", "Third" );
        
        // WHEN
        int count = NewIterators.count( items );
        
        // THEN
        assertEquals( 3, count );
    }
    
    @Test
    public void asArray() throws Exception
    {
        // GIVEN
        Iterator<String> items = iterator( "First", "Second", "Third" );
        
        // WHEN
        String[] array = NewIterators.asArray( items, String.class );
        
        // THEN
        assertTrue( Arrays.equals( new String[] { "First", "Second", "Third" }, array ) );
    }
    
    private static class CloneableThing implements CloneableInPublic
    {
        private final String value;
        private boolean cloned;

        CloneableThing( String value )
        {
            this.value = value;
            this.cloned = false;
        }
        
        @Override
        public CloneableThing clone()
        {
            CloneableThing clone = new CloneableThing( value );
            clone.cloned = true;
            return clone;
        }
    }
    
    private void assertNoMoreItems( Iterator<?> iterator )
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

    private <T> void assertNextEquals( T expected, Iterator<T> iterator )
    {
        assertTrue( iterator + " should have had more items", iterator.hasNext() );
        assertEquals( expected, iterator.next() );
    }
    
    private <T> void assertItems( Iterator<T> iterator, T... expectedItems )
    {
        for ( T expectedItem : expectedItems )
        {
            assertNextEquals( expectedItem, iterator );
        }
        assertNoMoreItems( iterator );
    }
}
