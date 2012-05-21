/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
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
        ClockCache<Object, Object> cache = new ClockCache<Object, Object>(
            "TestCache", 70 );
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
        ClockCacheTest<Integer, String> cache = new ClockCacheTest<Integer, String>(
            "TestCache", 3 );
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
        assertNotNull( cache.getLastCleanedElement() );
        String lastCleaned = cache.getLastCleanedElement();
        cleanedElements.add( valueToKey.get( lastCleaned ) );
        existingElements.remove( valueToKey.get( lastCleaned ) );

        cache.put( key5, s5 );
        assertNotNull( cache.getLastCleanedElement() );
        assertFalse( lastCleaned.equals( cache.getLastCleanedElement() ) );
        lastCleaned = cache.getLastCleanedElement();
        assertFalse( s4.equals( lastCleaned ) );
        assertFalse( s5.equals( lastCleaned ) );
        cleanedElements.add( valueToKey.get( lastCleaned ) );
        existingElements.remove( valueToKey.get( lastCleaned ) );

        int size = cache.size();
        assertEquals( 3, size );
        for ( Integer key : cleanedElements )
        {
            assertEquals( "for key " + key, null, cache.get( key ) );
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
    public void testMoreSimple()
    {
        ClockCacheTest<Integer, String> cache = new ClockCacheTest<Integer, String>( "TestCacheSingle", 2 );
        cache.put( 1, "1" );
        cache.put( 2, "2" );
        cache.put( 3, "3" );
        assertNull( cache.get( 1 ) );
        assertEquals( "2", cache.get( 2 ) );
        assertEquals( "3", cache.get( 3 ) );
        assertEquals( "1", cache.getLastCleanedElement() );
        cache.put( 1, "1-1" );
        assertEquals( "3", cache.getLastCleanedElement() );
        assertEquals( "1-1", cache.get( 1 ) );
        cache.put( 1, "1" );

        int entryCounter = 0;
        for ( Map.Entry<Integer, String> entry : cache.entrySet() )
        {
            entryCounter++;
            assertEquals( entry.getKey().toString(), entry.getValue() );
        }
        assertEquals( 2, entryCounter );
        assertEquals( entryCounter, cache.size() );
    }

    @Test
    @Ignore( "Takes a lot of time and the cache is non hard on guarantees. Run by hand instead" )
    public void testMultiThreaded() throws InterruptedException
    {
        final int cacheSize = 10;
        Map<String, Long> theControl = new ConcurrentHashMap<String, Long>();
        for ( int i = 0; i < 100; i++ )
        {
            theControl.put( "" + i, System.currentTimeMillis() );
        }
        ExecutorService executor = Executors.newFixedThreadPool( 20 );

        // key is an integer, value is the integer with a '-value' appended
        ClockCache<String, String> theCache = new ClockCache<String, String>( "under test", 10 );
        Random r = new Random();
        for ( int i = 0; i < 1000000; i++ )
        {
            executor.execute( new ClockCacheWorker( theCache, theControl, r.nextInt( 100 ) ) );
        }
        executor.shutdown();
        while ( !executor.awaitTermination( 5, TimeUnit.SECONDS ) )
        {
            System.out.println( "waiting more" );
        }
        assertEquals( cacheSize, theCache.size() );
        long now = System.currentTimeMillis();
        int entryCounter = 0;
        for ( Map.Entry<String, String> entry : theCache.entrySet() )
        {
            System.out.println( "Entry " + entry.getKey() + " is in cache, last messed with "
                                + ( now - theControl.get( entry.getKey() ) ) + " ms" );
            assertNotNull( "null for key " + entry.getKey(), entry.getValue() );
            assertEquals( "wrong value for key " + entry.getKey(), entry.getKey() + "-value", entry.getValue() );
            entryCounter++;
        }
        assertEquals( "Entry counting was wrong", theCache.size(), entryCounter );
        /*
        for ( Map.Entry<String, Long> entry : theControl.entrySet() )
        {
            System.out.println( "Entry " + entry.getKey() + " is in control, " + ( now - entry.getValue() )
                                + " since last messed with" );
        }
        */
    }

    private static class ClockCacheWorker implements Runnable
    {
        private final ClockCache<String, String> theCache;
        private final Map<String, Long> theControl;
        private final int startAt;

        public ClockCacheWorker( ClockCache<String, String> cache, Map<String, Long> control, int startAt )
        {
            this.theCache = cache;
            this.theControl = control;
            this.startAt = startAt;
        }

        @Override
        public void run()
        {
            for ( int i = 0; i < 50; i++ )
            {
                String toMessWith = ( ( startAt + i ) % 100 ) + "";
                String toMessWithValue = toMessWith + "-value";
                theCache.put( toMessWith, toMessWithValue );
                theCache.get( toMessWith );
                theControl.put( toMessWith, System.currentTimeMillis() );
            }
        }
    }
}