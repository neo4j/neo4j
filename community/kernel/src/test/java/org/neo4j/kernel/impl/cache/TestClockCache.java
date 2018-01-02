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
package org.neo4j.kernel.impl.cache;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class TestClockCache
{
    @Test
    public void testCreate()
    {
        try
        {
            new ClockCache<Object, Object>( "TestCache", 0 );
            fail( "Illegal maxSize should throw exception" );
        }
        catch ( IllegalArgumentException e )
        { // good
        }
        ClockCache<Object, Object> cache = new ClockCache<Object, Object>( "TestCache", 70 );
        try
        {
            cache.put( null, new Object() );
            fail( "Null key should throw exception" );
        }
        catch ( IllegalArgumentException e )
        { // good
        }
        try
        {
            cache.put( new Object(), null );
            fail( "Null element should throw exception" );
        }
        catch ( IllegalArgumentException e )
        { // good
        }
        try
        {
            cache.get( null );
            fail( "Null key should throw exception" );
        }
        catch ( IllegalArgumentException e )
        { // good
        }
        try
        {
            cache.remove( null );
            fail( "Null key should throw exception" );
        }
        catch ( IllegalArgumentException e )
        { // good
        }
        cache.put( new Object(), new Object() );
        cache.clear();
    }

    private static class ClockCacheTest<K, E> extends ClockCache<K, E>
    {
        private E cleanedElement = null;

        ClockCacheTest( String name, int maxSize )
        {
            super( name, maxSize );
        }

        @Override
        public void elementCleaned( E element )
        {
            cleanedElement = element;
        }

        E getLastCleanedElement()
        {
            return cleanedElement;
        }
    }

    @Test
    public void testSimple()
    {
        ClockCacheTest<Integer, String> cache = new ClockCacheTest<Integer, String>( "TestCache", 3 );
        Map<String, Integer> valueToKey = new HashMap<String, Integer>();
        Map<Integer, String> keyToValue = new HashMap<Integer, String>();

        String s1 = new String( "1" );
        Integer key1 = new Integer( 1 );
        valueToKey.put( s1, key1 );
        keyToValue.put( key1, s1 );

        String s2 = new String( "2" );
        Integer key2 = new Integer( 2 );
        valueToKey.put( s2, key2 );
        keyToValue.put( key2, s2 );

        String s3 = new String( "3" );
        Integer key3 = new Integer( 3 );
        valueToKey.put( s3, key3 );
        keyToValue.put( key3, s3 );

        String s4 = new String( "4" );
        Integer key4 = new Integer( 4 );
        valueToKey.put( s4, key4 );
        keyToValue.put( key4, s4 );

        String s5 = new String( "5" );
        Integer key5 = new Integer( 5 );
        valueToKey.put( s5, key5 );
        keyToValue.put( key5, s5 );

        List<Integer> cleanedElements = new LinkedList<Integer>();
        List<Integer> existingElements = new LinkedList<Integer>();

        cache.put( key1, s1 );
        cache.put( key2, s2 );
        cache.put( key3, s3 );
        assertEquals( null, cache.getLastCleanedElement() );

        String fromKey2 = cache.get( key2 );
        assertEquals( s2, fromKey2 );
        String fromKey1 = cache.get( key1 );
        assertEquals( s1, fromKey1 );
        String fromKey3 = cache.get( key3 );
        assertEquals( s3, fromKey3 );

        cache.put( key4, s4 );
        assertFalse( s4.equals( cache.getLastCleanedElement() ) );
        cleanedElements.add( valueToKey.get( cache.getLastCleanedElement() ) );
        existingElements.remove( valueToKey.get( cache.getLastCleanedElement() ) );

        cache.put( key5, s5 );
        assertFalse( s4.equals( cache.getLastCleanedElement() ) );
        assertFalse( s5.equals( cache.getLastCleanedElement() ) );
        cleanedElements.add( valueToKey.get( cache.getLastCleanedElement() ) );
        existingElements.remove( valueToKey.get( cache.getLastCleanedElement() ) );

        int size = cache.size();
        assertEquals( 3, size );
        for ( Integer key : cleanedElements )
        {
            assertEquals( null, cache.get( key ) );
        }
        for ( Integer key : existingElements )
        {
            assertEquals( keyToValue.get( key ), cache.get( key ) );
        }
        cache.clear();
        assertEquals( 0, cache.size() );
        for ( Integer key : keyToValue.keySet() )
        {
            assertEquals( null, cache.get( key ) );
        }
    }

    @Test
    public void shouldUpdateSizeWhenRemoving() throws Exception
    {
        ClockCache<String, Integer> cache = new ClockCache<String, Integer>( "foo", 3 );
        cache.put( "bar", 42 );
        cache.put( "baz", 87 );

        cache.remove( "bar" );

        assertThat( cache.size(), is( 1 ) );
    }
}