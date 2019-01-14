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
package org.neo4j.helpers.collection;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.stream.Stream;

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestCommonIterators
{
    @Test
    public void testNoDuplicatesFilteringIterator()
    {
        List<Integer> ints = Arrays.asList( 1, 2, 2, 40, 100, 40, 101, 2, 3 );
        Iterator<Integer> iterator = FilteringIterator.noDuplicates( ints.iterator() );
        assertEquals( (Integer) 1, iterator.next() );
        assertEquals( (Integer) 2, iterator.next() );
        assertEquals( (Integer) 40, iterator.next() );
        assertEquals( (Integer) 100, iterator.next() );
        assertEquals( (Integer) 101, iterator.next() );
        assertEquals( (Integer) 3, iterator.next() );
    }

    @Test
    public void testCachingIterator()
    {
        Iterator<Integer> source = new RangeIterator( 8 );
        CachingIterator<Integer> caching = new CachingIterator<>( source );

        try
        {
            caching.previous();
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        { /* Good */ }

        try
        {
            caching.current();
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e )
        { /* Good */ }

        // Next and previous
        assertEquals( 0, caching.position() );
        assertTrue( caching.hasNext() );
        assertEquals( 0, caching.position() );
        assertFalse( caching.hasPrevious() );
        assertEquals( (Integer) 0, caching.next() );
        assertTrue( caching.hasNext() );
        assertTrue( caching.hasPrevious() );
        assertEquals( (Integer) 1, caching.next() );
        assertTrue( caching.hasPrevious() );
        assertEquals( (Integer) 1, caching.current() );
        assertEquals( (Integer) 2, caching.next() );
        assertEquals( (Integer) 2, caching.current() );
        assertEquals( (Integer) 3, (Integer) caching.position() );
        assertEquals( (Integer) 2, caching.current() );
        assertTrue( caching.hasPrevious() );
        assertEquals( (Integer) 2, caching.previous() );
        assertEquals( (Integer) 2, caching.current() );
        assertEquals( (Integer) 2, (Integer) caching.position() );
        assertEquals( (Integer) 1, caching.previous() );
        assertEquals( (Integer) 1, caching.current() );
        assertEquals( (Integer) 1, (Integer) caching.position() );
        assertEquals( (Integer) 0, caching.previous() );
        assertEquals( (Integer) 0, (Integer) caching.position() );
        assertFalse( caching.hasPrevious() );

        // Positioning
        try
        {
            caching.position( -1 );
            fail( "Shouldn't be able to set a lower value than 0" );
        }
        catch ( IllegalArgumentException e )
        { /* Good */ }
        assertEquals( (Integer) 0, caching.current() );
        assertEquals( 0, caching.position( 3 ) );
        try
        {
            caching.current();
            fail( "Shouldn't be able to call current() after a call to position(int)" );
        }
        catch ( NoSuchElementException e )
        { /* Good */ }
        assertTrue( caching.hasNext() );
        assertEquals( (Integer) 3, caching.next() );
        assertEquals( (Integer) 3, caching.current() );
        assertTrue( caching.hasPrevious() );
        assertEquals( (Integer) 4, caching.next() );
        assertEquals( 5, caching.position() );
        assertEquals( (Integer) 4, caching.previous() );
        assertEquals( (Integer) 4, caching.current() );
        assertEquals( (Integer) 4, caching.current() );
        assertEquals( 4, caching.position() );
        assertEquals( (Integer) 3, caching.previous() );
        assertEquals( 3, caching.position() );
        try
        {
            caching.position( 9 );
            fail( "Shouldn't be able to set a position which is too big" );
        }
        catch ( NoSuchElementException e )
        { /* Good */ }
        assertEquals( 3, caching.position( 8 ) );
        assertTrue( caching.hasPrevious() );
        assertFalse( caching.hasNext() );
        try
        {
            caching.next();
            fail( "Shouldn't be able to go beyond last item" );
        }
        catch ( NoSuchElementException e )
        { /* Good */ }
        assertEquals( 8, caching.position() );
        assertEquals( (Integer) 7, caching.previous() );
        assertEquals( (Integer) 6, caching.previous() );
        assertEquals( 6, caching.position( 0 ) );
        assertEquals( (Integer) 0, caching.next() );
    }

    @Test
    public void testPagingIterator()
    {
        Iterator<Integer> source = new RangeIterator( 24 );
        PagingIterator<Integer> pager = new PagingIterator<>( source, 10 );
        assertEquals( 0, pager.page() );
        assertTrue( pager.hasNext() );
        assertPage( pager.nextPage(), 10, 0 );
        assertTrue( pager.hasNext() );

        assertEquals( 1, pager.page() );
        assertTrue( pager.hasNext() );
        assertPage( pager.nextPage(), 10, 10 );
        assertTrue( pager.hasNext() );

        assertEquals( 2, pager.page() );
        assertTrue( pager.hasNext() );
        assertPage( pager.nextPage(), 4, 20 );
        assertFalse( pager.hasNext() );

        pager.page( 1 );
        assertEquals( 1, pager.page() );
        assertTrue( pager.hasNext() );
        assertPage( pager.nextPage(), 10, 10 );
        assertTrue( pager.hasNext() );
    }

    private void assertPage( Iterator<Integer> page, int size, int plus )
    {
        for ( int i = 0; i < size; i++ )
        {
            assertTrue( page.hasNext() );
            assertEquals( (Integer) (i + plus), page.next() );
        }
        assertFalse( page.hasNext() );
    }

    @Test
    public void testFirstElement()
    {
        Object object = new Object();
        Object object2 = new Object();

        // first Iterable
        assertEquals( object, Iterables.first( Arrays.asList( object, object2 ) ) );
        assertEquals( object, Iterables.first( Arrays.asList( object ) ) );
        try
        {
            Iterables.first( Arrays.asList() );
            fail( "Should fail" );
        }
        catch ( NoSuchElementException e )
        { /* Good */ }

        // first Iterator
        assertEquals( object, Iterators.first( Arrays.asList( object, object2 ).iterator() ) );
        assertEquals( object, Iterators.first( Arrays.asList( object ).iterator() ) );
        try
        {
            Iterators.first( Arrays.asList().iterator() );
            fail( "Should fail" );
        }
        catch ( NoSuchElementException e )
        { /* Good */ }

        // firstOrNull Iterable
        assertEquals( object, Iterables.firstOrNull( Arrays.asList( object, object2 ) ) );
        assertEquals( object, Iterables.firstOrNull( Arrays.asList( object ) ) );
        assertNull( Iterables.firstOrNull( Arrays.asList() ) );

        // firstOrNull Iterator
        assertEquals( object, Iterators.firstOrNull( Arrays.asList( object, object2 ).iterator() ) );
        assertEquals( object, Iterators.firstOrNull( Arrays.asList( object ).iterator() ) );
        assertNull( Iterators.firstOrNull( Arrays.asList().iterator() ) );
    }

    @Test
    public void testLastElement()
    {
        Object object = new Object();
        Object object2 = new Object();

        // last Iterable
        assertEquals( object2, Iterables.last( Arrays.asList( object, object2 ) ) );
        assertEquals( object, Iterables.last( Arrays.asList( object ) ) );
        try
        {
            Iterables.last( Arrays.asList() );
            fail( "Should fail" );
        }
        catch ( NoSuchElementException e )
        { /* Good */ }

        // last Iterator
        assertEquals( object2, Iterators.last( Arrays.asList( object, object2 ).iterator() ) );
        assertEquals( object, Iterators.last( Arrays.asList( object ).iterator() ) );
        try
        {
            Iterators.last( Arrays.asList().iterator() );
            fail( "Should fail" );
        }
        catch ( NoSuchElementException e )
        { /* Good */ }

        // lastOrNull Iterable
        assertEquals( object2, Iterables.lastOrNull( Arrays.asList( object, object2 ) ) );
        assertEquals( object, Iterables.lastOrNull( Arrays.asList( object ) ) );
        assertNull( Iterables.lastOrNull( Arrays.asList() ) );

        // lastOrNull Iterator
        assertEquals( object2, Iterators.lastOrNull( Arrays.asList( object, object2 ).iterator() ) );
        assertEquals( object, Iterators.lastOrNull( Arrays.asList( object ).iterator() ) );
        assertNull( Iterators.lastOrNull( Arrays.asList().iterator() ) );
    }

    @Test
    public void testSingleElement()
    {
        Object object = new Object();
        Object object2 = new Object();

        // single Iterable
        assertEquals( object, Iterables.single( Arrays.asList( object ) ) );
        try
        {
            Iterables.single( Arrays.asList() );
            fail( "Should fail" );
        }
        catch ( Exception e )
        { /* Good */ }
        try
        {
            Iterables.single( Arrays.asList( object, object2 ) );
            fail( "Should fail" );
        }
        catch ( Exception e )
        { /* Good */ }

        // single Iterator
        assertEquals( object, Iterators.single( Arrays.asList( object ).iterator() ) );
        try
        {
            Iterators.single( Arrays.asList().iterator() );
            fail( "Should fail" );
        }
        catch ( Exception e )
        { /* Good */ }
        try
        {
            Iterators.single( Arrays.asList( object, object2 ).iterator() );
            fail( "Should fail" );
        }
        catch ( Exception e )
        { /* Good */ }

        // singleOrNull Iterable
        assertEquals( object, Iterables.singleOrNull( Arrays.asList( object ) ) );
        assertNull( Iterables.singleOrNull( Arrays.asList() ) );
        try
        {
            Iterables.singleOrNull( Arrays.asList( object, object2 ) );
            fail( "Should fail" );
        }
        catch ( Exception e )
        { /* Good */ }

        // singleOrNull Iterator
        assertEquals( object, Iterators.singleOrNull( Arrays.asList( object ).iterator() ) );
        assertNull( Iterators.singleOrNull( Arrays.asList().iterator() ) );
        try
        {
            Iterators.singleOrNull( Arrays.asList( object, object2 ).iterator() );
            fail( "Should fail" );
        }
        catch ( Exception e )
        { /* Good */ }
    }

    @Test
    public void getItemFromEnd()
    {
        Iterable<Integer> ints = Arrays.asList( 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 );
        assertEquals( (Integer) 9, Iterables.fromEnd( ints, 0 ) );
        assertEquals( (Integer) 8, Iterables.fromEnd( ints, 1 ) );
        assertEquals( (Integer) 7, Iterables.fromEnd( ints, 2 ) );
    }

    @Test( expected = NullPointerException.class )
    public void iteratorsStreamForNull()
    {
        Iterators.stream( null );
    }

    @Test
    public void iteratorsStream()
    {
        List<Object> list = Arrays.asList( 1, 2, "3", '4', null, "abc", "56789" );

        Iterator<Object> iterator = list.iterator();

        assertEquals( list, Iterators.stream( iterator ).collect( toList() ) );
    }

    @Test
    public void iteratorsStreamClosesResourceIterator()
    {
        List<Object> list = Arrays.asList( "a", "b", "c", "def" );

        Resource resource = mock( Resource.class );
        ResourceIterator<Object> iterator = Iterators.resourceIterator( list.iterator(), resource );

        try ( Stream<Object> stream = Iterators.stream( iterator ) )
        {
            assertEquals( list, stream.collect( toList() ) );
        }
        verify( resource ).close();
    }

    @Test
    public void iteratorsStreamCharacteristics()
    {
        Iterator<Integer> iterator = Arrays.asList( 1, 2, 3 ).iterator();
        int characteristics = Spliterator.DISTINCT | Spliterator.ORDERED | Spliterator.SORTED;

        Stream<Integer> stream = Iterators.stream( iterator, characteristics );

        assertEquals( characteristics, stream.spliterator().characteristics() );
    }

    @Test( expected = NullPointerException.class )
    public void iterablesStreamForNull()
    {
        Iterables.stream( null );
    }

    @Test
    public void iterablesStream()
    {
        List<Object> list = Arrays.asList( 1, 2, "3", '4', null, "abc", "56789" );

        assertEquals( list, Iterables.stream( list ).collect( toList() ) );
    }

    @Test
    public void iterablesStreamClosesResourceIterator()
    {
        List<Object> list = Arrays.asList( "a", "b", "c", "def" );

        Resource resource = mock( Resource.class );
        ResourceIterable<Object> iterable = () -> Iterators.resourceIterator( list.iterator(), resource );

        try ( Stream<Object> stream = Iterables.stream( iterable ) )
        {
            assertEquals( list, stream.collect( toList() ) );
        }
        verify( resource ).close();
    }

    @Test
    public void iterablesStreamCharacteristics()
    {
        Iterable<Integer> iterable = Arrays.asList( 1, 2, 3 );
        int characteristics = Spliterator.DISTINCT | Spliterator.ORDERED | Spliterator.NONNULL;

        Stream<Integer> stream = Iterables.stream( iterable, characteristics );

        assertEquals( characteristics, stream.spliterator().characteristics() );
    }
}
