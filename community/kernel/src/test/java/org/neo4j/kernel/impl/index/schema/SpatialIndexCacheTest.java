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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.Race;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterables.count;

public class SpatialIndexCacheTest
{
    @SuppressWarnings( "Duplicates" )
    @Test
    public void stressCache() throws Exception
    {
        StringFactory factory = new StringFactory();
        SpatialIndexCache<String> cache = new SpatialIndexCache<>( factory );

        ExecutorService pool = Executors.newFixedThreadPool( 20 );
        Future<?>[] futures = new Future[100];
        AtomicBoolean shouldContinue = new AtomicBoolean( true );

        try
        {
            for ( int i = 0; i < futures.length; i++ )
            {
                futures[i] = pool.submit( new CacheStresser( cache, shouldContinue ) );
            }

            Thread.sleep( 5_000 );

            shouldContinue.set( false );

            for ( Future<?> future : futures )
            {
                future.get( 10, TimeUnit.SECONDS );
            }
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void stressInstantiationWithClose() throws Throwable
    {
        // given
        StringFactory factory = new StringFactory();
        SpatialIndexCache<String> cache = new SpatialIndexCache<>( factory );
        Race race = new Race().withRandomStartDelays();
        MutableInt instantiatedAtClose = new MutableInt();
        race.addContestant( () ->
        {
            try
            {
                cache.uncheckedSelect( CoordinateReferenceSystem.WGS84 );
                cache.uncheckedSelect( CoordinateReferenceSystem.Cartesian_3D );
            }
            catch ( IllegalStateException e )
            {
                // This exception is OK since it may have been closed
            }
        }, 1 );
        race.addContestant( () ->
        {
            cache.closeInstantiateCloseLock();
            instantiatedAtClose.setValue( count( cache ) );
        }, 1 );

        // when
        race.go();

        // then
        try
        {
            cache.uncheckedSelect( CoordinateReferenceSystem.Cartesian );
            fail( "No instantiation after closed" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
        assertEquals( instantiatedAtClose.intValue(), count( cache ) );
    }

    private static final CoordinateReferenceSystem[] coordinateReferenceSystems =
            Iterators.stream( CoordinateReferenceSystem.all().iterator() ).toArray( CoordinateReferenceSystem[]::new );

    static class CacheStresser implements Runnable
    {
        private final SpatialIndexCache<String> cache;
        private final AtomicBoolean shouldContinue;
        private final Random random = new Random();

        CacheStresser( SpatialIndexCache<String> cache, AtomicBoolean shouldContinue )
        {
            this.cache = cache;
            this.shouldContinue = shouldContinue;
        }

        @Override
        public void run()
        {
            while ( shouldContinue.get() )
            {
                stress();
            }
        }

        private void stress()
        {
            try
            {
                // select
                cache.select( coordinateReferenceSystems[random.nextInt( coordinateReferenceSystems.length )] );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
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

    private static class StringFactory implements SpatialIndexCache.Factory<String>
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
        public String newSpatial( CoordinateReferenceSystem crs )
        {
            for ( int i = 0; i < coordinateReferenceSystems.length; i++ )
            {
                if ( coordinateReferenceSystems[i].equals( crs ) )
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
