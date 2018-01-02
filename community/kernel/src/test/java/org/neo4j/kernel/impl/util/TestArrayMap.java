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
package org.neo4j.kernel.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.Ignore;
import org.junit.Test;

public class TestArrayMap
{
    @Test
	public void testArrayMap()
	{
		ArrayMap<String,Integer> map = new ArrayMap<String,Integer>();

		assertTrue( map.get( "key1" ) == null );
		map.put( "key1", 0 );
		assertEquals( new Integer(0), map.get( "key1" ) );
		assertEquals( new Integer(0), map.get( "key1" ) );
		map.put( "key1", 1 );
		assertEquals( new Integer(1), map.get( "key1" ) );
		map.put( "key2", 0 );
		assertEquals( new Integer(0), map.get( "key2" ) );
		map.put( "key2", 2 );
		assertEquals( new Integer(2), map.get( "key2" ) );
		assertEquals( new Integer(2), map.remove( "key2" ) );
		assertTrue( map.get( "key2" ) == null );
		assertEquals( new Integer(1), map.get( "key1" ) );
		assertEquals( new Integer(1), map.remove( "key1" ) );
		assertTrue( map.get( "key1" ) == null );

		map.put( "key1", 1 );
		map.put( "key2", 2 );
		map.put( "key3", 3 );
		map.put( "key4", 4 );
		map.put( "key5", 5 );
		assertEquals( new Integer(5), map.get( "key5" ) );
		assertEquals( new Integer(4), map.get( "key4" ) );
		assertEquals( new Integer(3), map.get( "key3" ) );
		assertEquals( new Integer(2), map.get( "key2" ) );
		assertEquals( new Integer(1), map.get( "key1" ) );
		assertEquals( new Integer(5), map.remove( "key5" ) );
		assertEquals( new Integer(1), map.get( "key1" ) );
		assertEquals( new Integer(4), map.get( "key4" ) );
		assertEquals( new Integer(3), map.get( "key3" ) );
		assertEquals( new Integer(2), map.get( "key2" ) );
		assertEquals( new Integer(3), map.remove( "key3" ) );
		assertEquals( new Integer(1), map.remove( "key1" ) );
		assertEquals( new Integer(2), map.remove( "key2" ) );

		for ( int i = 0; i < 100; i++ )
		{
			map.put( "key" + i, i );
		}
		for ( int i = 0; i < 100; i++ )
		{
			assertEquals( new Integer(i), map.get( "key" + i) );
		}
		for ( int i = 0; i < 100; i++ )
		{
			assertEquals( new Integer(i), map.remove( "key" + i) );
		}
		for ( int i = 0; i < 100; i++ )
		{
			assertTrue( map.get( "key" + i ) == null );
		}
    }

    @Test
    public void arraymapIsClearedWhenExpandingToHashMapIfNonShrinkable() throws Exception
    {
        assertDataRepresentationSwitchesWhenAboveThreshold( new ArrayMap<String, Integer>( (byte)3, false,
                false ), false );
    }

    @Test
    public void arraymapIsClearedWhenExpandingToHashMapIfShrinkable() throws Exception
    {
        assertDataRepresentationSwitchesWhenAboveThreshold( new ArrayMap<String, Integer>( (byte)3, false,
                true ), true );
    }

    @Test
    public void arraymapIsClearedWhenExpandingToHashMapIfNonShrinkableAndSynchronized()
            throws Exception
    {
        assertDataRepresentationSwitchesWhenAboveThreshold( new ArrayMap<String, Integer>( (byte)3, true,
                false ), false );
    }

    @Test
    public void arraymapIsClearedWhenExpandingToHashMapIfShrinkableAndSynchronized()
            throws Exception
    {
        assertDataRepresentationSwitchesWhenAboveThreshold(
                new ArrayMap<String, Integer>( (byte)3, true, true ), true );
    }

    @SuppressWarnings( "rawtypes" )
    private void assertDataRepresentationSwitchesWhenAboveThreshold( ArrayMap<String, Integer> map,
            boolean shrinkable ) throws Exception
    {
        // Perhaps not the pretties solution... quite brittle...
        Field mapThresholdField = ArrayMap.class.getDeclaredField( "toMapThreshold" );
        mapThresholdField.setAccessible( true );
        int arraySize = mapThresholdField.getInt( map );
        Field dataField = ArrayMap.class.getDeclaredField( "data" );
        dataField.setAccessible( true );
        assertTrue( dataField.get( map ) instanceof Object[] );
        
        for ( int i = 0; i < arraySize; i++ )
        {
            map.put( "key" + i, i );
            assertTrue( dataField.get( map ) instanceof Object[] );
        }

        map.put( "next key", 999 );
        Map dataAsMap = (Map) dataField.get( map );
        assertEquals( arraySize+1, dataAsMap.size() );

        map.remove( "key1" );
        map.remove( "key2" );
        map.remove( "key3" );

        if ( shrinkable )
        {
            // It should've shrinked back into an array
            assertTrue( dataField.get( map ) instanceof Object[] );
        }
        else
        {
            // It should stay as Map representation
            assertTrue( dataField.get( map ) instanceof Map );
        }
    }

    @Test
    public void canOverwriteThenRemoveElementAcrossDeflation() throws Exception
    {
        ArrayMap<String, Integer> map = new ArrayMap<String, Integer>( (byte)3, false, true );

        map.put( "key1", 1 );
        map.put( "key2", 2 );
        map.put( "key3", 3 );
        map.put( "key4", 4 );
        map.put( "key5", 5 );

        map.put( "key1", 6 );

        map.remove( "key1" );
        assertNull( "removed element still found", map.get( "key1" ) );
        map.remove( "key2" );
        assertNull( "removed element still found", map.get( "key1" ) );
        map.remove( "key3" );
        
        assertNull( "removed element still found", map.get( "key1" ) );
        map.remove( "key4" );
        assertNull( "removed element still found", map.get( "key1" ) );
    }

    @Ignore("Takes 30mins to run on Windows. Ignoring")
    @Test
    public void testThreadSafeSize() throws InterruptedException
    {
        ArrayMap<Integer,Object> map = new ArrayMap<Integer,Object>((byte)5, true, true );
        map.put( 1, new Object() );
        map.put( 2, new Object() );
        map.put( 3, new Object() );
        map.put( 4, new Object() );
        map.put( 5, new Object() );
        final int NUM_THREADS = 100;
        CountDownLatch done = new CountDownLatch( NUM_THREADS );
        List<WorkerThread> threads = new ArrayList<WorkerThread>( NUM_THREADS );
        for ( int i = 0; i < NUM_THREADS; i++ )
        {
            WorkerThread thread = new WorkerThread( map, done );
            threads.add( thread );
            thread.start();
        }
        done.await();
        for ( WorkerThread thread : threads )
        {
            assertTrue( "Synchronized ArrayMap concurrent size invoke failed: " + thread.getCause(), thread.wasSuccessful() );
        }
    }
    
    private static class WorkerThread extends Thread
    {
        private final ArrayMap<Integer,Object> map;
        
        private volatile boolean success = false;
        private volatile Throwable t = null;

        private final CountDownLatch done;
        
        WorkerThread( ArrayMap<Integer,Object> map, CountDownLatch done )
        {
            this.map = map;
            this.done = done;
        }
        
        @Override
        public void run()
        {
            try
            {
                for ( int i = 0; i < 10000; i++ )
                {
                    if ( map.size() > 5 )
                    {
                        for ( int j = i; j < (i+10); j++ )
                        {
                            if ( map.remove( j % 10 ) != null )
                            {
                                break;
                            }
                        }
                    }
                    // calling size again to increase chance to hit CCE
                    else if ( map.size() <= 5 )
                    {
                        map.put( i % 10, new Object() );
                    }
                    yield();
                }
                success = true;
            }
            catch ( Throwable t )
            {
                this.t = t;
            }
            finally
            {
                done.countDown();
            }
        }
        
        boolean wasSuccessful()
        {
            return success;
        }
        
        Throwable getCause()
        {
            return t;
        }
    }
}