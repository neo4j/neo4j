package org.neo4j.commons.iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;
import org.neo4j.helpers.collection.CachingIterator;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PagingIterator;
import org.neo4j.helpers.collection.RangeIterator;

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
        CachingIterator<Integer> caching = new CachingIterator<Integer>( source );
        
        try
        {
            caching.previous();
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e ) { /* Good */ }
        
        try
        {
            caching.current();
            fail( "Should throw exception" );
        }
        catch ( NoSuchElementException e ) { /* Good */ }
        
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
        catch ( IllegalArgumentException e ) { /* Good */ }
        assertEquals( (Integer) 0, caching.current() );
        assertEquals( 0, caching.position( 3 ) );
        try
        {
            caching.current();
            fail( "Shouldn't be able to call current() after a call to position(int)" );
        }
        catch ( NoSuchElementException e ) { /* Good */ }
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
        catch ( NoSuchElementException e ) { /* Good */ }
        assertEquals( 3, caching.position( 8 ) );
        assertTrue( caching.hasPrevious() );
        assertFalse( caching.hasNext() );
        try
        {
            caching.next();
            fail( "Shouldn't be able to go beyond last item" );
        }
        catch ( NoSuchElementException e ) { /* Good */ }
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
        PagingIterator<Integer> pager = new PagingIterator<Integer>( source, 10 );
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
            assertEquals( (Integer) (i+plus), page.next() );
        }
        assertFalse( page.hasNext() );
    }
    
    @Test
    public void testFirstElement()
    {
        Object object = new Object();
        Object object2 = new Object();
        assertEquals( object, IteratorUtil.first( Arrays.asList( object, object2 ).iterator() ) );
        assertEquals( object, IteratorUtil.first( Arrays.asList( object ).iterator() ) );
        try
        {
            IteratorUtil.first( Arrays.asList().iterator() );
            fail( "Should fail" );
        }
        catch ( NoSuchElementException e ) { /* Good */ }

        assertEquals( object, IteratorUtil.firstOrNull( Arrays.asList( object, object2 ).iterator() ) );
        assertEquals( object, IteratorUtil.firstOrNull( Arrays.asList( object ).iterator() ) );
        assertNull( IteratorUtil.firstOrNull( Arrays.asList().iterator() ) );
    }
    
    @Test
    public void testLastElement()
    {
        Object object = new Object();
        Object object2 = new Object();
        assertEquals( object2, IteratorUtil.last( Arrays.asList( object, object2 ).iterator() ) );
        assertEquals( object, IteratorUtil.last( Arrays.asList( object ).iterator() ) );
        try
        {
            IteratorUtil.last( Arrays.asList().iterator() );
            fail( "Should fail" );
        }
        catch ( NoSuchElementException e ) { /* Good */ }

        assertEquals( object2, IteratorUtil.lastOrNull( Arrays.asList( object, object2 ).iterator() ) );
        assertEquals( object, IteratorUtil.lastOrNull( Arrays.asList( object ).iterator() ) );
        assertNull( IteratorUtil.lastOrNull( Arrays.asList().iterator() ) );
    }
    
    @Test
    public void testSingleElement()
    {
        Object object = new Object();
        Object object2 = new Object();
        assertEquals( object, IteratorUtil.single( Arrays.asList( object ).iterator() ) );
        try
        {
            IteratorUtil.single( Arrays.asList().iterator() );
            fail( "Should fail" );
        }
        catch ( Exception e ) { /* Good */ }
        try
        {
            IteratorUtil.single( Arrays.asList( object, object2 ).iterator() );
            fail( "Should fail" );
        }
        catch ( Exception e ) { /* Good */ }
        
        assertEquals( object, IteratorUtil.singleOrNull( Arrays.asList( object ).iterator() ) );
        assertNull( IteratorUtil.singleOrNull( Arrays.asList().iterator() ) );
        try
        {
            IteratorUtil.singleOrNull( Arrays.asList( object, object2 ).iterator() );
            fail( "Should fail" );
        }
        catch ( Exception e ) { /* Good */ }
    }
}
