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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.junit.Assert.assertNull;

public class SpatialIndexCacheTest
{
    @Test
    public void stressCache() throws Exception
    {
        StringFactory factory = new StringFactory();
        SpatialIndexCache<String,Exception> cache = new SpatialIndexCache<>( factory );

        CacheStresser[] stressers = new CacheStresser[100];
        for ( int i = 0; i < stressers.length; i++ )
        {
            stressers[i] = new CacheStresser( cache );
            stressers[i].start();
        }

        Thread.sleep( 5_000 );

        for ( CacheStresser stresser : stressers )
        {
            stresser.interrupt();
        }

        for ( CacheStresser stresser : stressers )
        {
            stresser.join();
        }

        for ( CacheStresser stresser : stressers )
        {
            assertNull( stresser.failed );
        }
    }

    static private final CoordinateReferenceSystem[] coordinateReferenceSystems =
            Iterators.stream( CoordinateReferenceSystem.all() ).toArray( CoordinateReferenceSystem[]::new );

    static class CacheStresser extends Thread
    {
        SpatialIndexCache<String,Exception> cache;
        Random r = new Random();
        Exception failed = null;

        CacheStresser( SpatialIndexCache<String,Exception> cache )
        {
            this.cache = cache;
        }

        @Override
        public void run()
        {
            while ( !Thread.interrupted() && failed == null )
            {
                try
                {
                    stress();
                }
                catch ( InterruptedException e )
                {
                    return;
                }
                catch ( Exception e )
                {
                    failed = e;
                }
            }
        }

        private void stress() throws Exception
        {
            // select
            cache.select( coordinateReferenceSystems[r.nextInt( coordinateReferenceSystems.length )] );
            // iterate
            for ( String s : cache )
            {
                if ( s == null )
                {
                    throw new IllegalStateException( "iterated over null" );
                }
            }
        }
    }

    static class StringFactory implements SpatialIndexCache.Factory<String, Exception>
    {
        AtomicInteger[] counters = new AtomicInteger[coordinateReferenceSystems.length];

        StringFactory()
        {
            for ( int i = 0; i < counters.length; i++ )
            {
                counters[i] = new AtomicInteger( 0 );
            }
        }

        @Override
        public String newSpatial( CoordinateReferenceSystem crs ) throws Exception
        {
            for ( int i = 0; i < coordinateReferenceSystems.length; i++ )
            {
                if (coordinateReferenceSystems[i].equals( crs ))
                {
                    int count = counters[i].incrementAndGet();
                    if ( count > 1 )
                    {
                        throw new IllegalStateException( "called new on same crs multiple times" );
                    }
                    break;
                }
            }
            return crs.toString();
        }
    }
}
