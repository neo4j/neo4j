/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static java.lang.System.currentTimeMillis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TestDirectArrayMap
{
    @Test
    public void singleThreaded() throws Exception
    {
        int size = 10;
        int count = 100000000;
        
        // ConcurrentHashMap
        {
            Map<Integer, String> map = new ConcurrentHashMap<Integer, String>();
            for ( int i = 0; i < size; i++ )
            {
                map.put( i, "yo " + i );
            }
            
            long t = currentTimeMillis();
            for ( int i = 0; i < count; i++ )
            {
                map.get( i%size );
            }
            System.out.println( "map " + (currentTimeMillis()-t) );
        }
        
        // DirectArrayMap
        {
            DirectArrayMap<String> map = new DirectArrayMap<String>( 10 );
            for ( int i = 0; i < size; i++ )
            {
                map.put( i, "yo " + i );
            }
            
            long t = currentTimeMillis();
            for ( int i = 0; i < count; i++ )
            {
                map.get( i%size );
            }
            System.out.println( "dmap " + (currentTimeMillis()-t) );
        }
    }
    
    @Test
    public void multiThreaded() throws Exception
    {
        final int size = 15;
        final int count = 10000000;
        
        // ConcurrentHashMap
        {
            final Map<Integer, String> map = new ConcurrentHashMap<Integer, String>();
            for ( int i = 0; i < 3; i++ ) map.put( i, "yo " + i );
            List<Thread> threads = new ArrayList<Thread>();
            long t = currentTimeMillis();
            for ( int i = 0; i < 3; i++ )
            {
                Thread thread = new Thread()
                {
                    @Override
                    public void run()
                    {
                        for ( int i = 0; i < count; i++ )
                        {
                            map.get( i%size );
                            if ( count%100000 == 0 && i > 0 )
                            {
                                int key = (int)(currentTimeMillis()%size);
                                map.put( key, "yo " + key );
                            }
//                            else if ( count%150000 == 0 && i > 0 )
//                            {
//                                int key = (int)(currentTimeMillis()%size);
//                                map.remove( key );
//                            }
                        }
                    }
                };
                thread.start();
                threads.add( thread );
            }
            for ( Thread thread : threads )
            {
                thread.join();
            }
            System.out.println( "map " + (currentTimeMillis()-t) );
        }

        // DirectArrayMap
        {
            final DirectArrayMap<String> map = new DirectArrayMap<String>( size );
            for ( int i = 0; i < 3; i++ ) map.put( i, "yo " + i );
            List<Thread> threads = new ArrayList<Thread>();
            long t = currentTimeMillis();
            for ( int i = 0; i < 3; i++ )
            {
                Thread thread = new Thread()
                {
                    @Override
                    public void run()
                    {
                        for ( int i = 0; i < count; i++ )
                        {
                            map.get( i%size );
                            if ( count%100000 == 0 && i > 0 )
                            {
                                int key = (int)(currentTimeMillis()%size);
                                map.put( key, "yo " + key );
                            }
//                            else if ( count%150000 == 0 && i > 0 )
//                            {
//                                int key = (int)(currentTimeMillis()%size);
//                                map.remove( key );
//                            }
                        }
                    }
                };
                thread.start();
                threads.add( thread );
            }
            for ( Thread thread : threads )
            {
                thread.join();
            }
            System.out.println( "dmap " + (currentTimeMillis()-t) );
        }
    }
}
