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

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.Race;
import org.neo4j.values.storable.ValueGroup;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.values.storable.ValueGroup.DATE;
import static org.neo4j.values.storable.ValueGroup.DURATION;
import static org.neo4j.values.storable.ValueGroup.LOCAL_DATE_TIME;
import static org.neo4j.values.storable.ValueGroup.LOCAL_TIME;
import static org.neo4j.values.storable.ValueGroup.ZONED_DATE_TIME;
import static org.neo4j.values.storable.ValueGroup.ZONED_TIME;

public class TemporalIndexCacheTest
{
    @Test
    public void shouldIterateOverCreatedParts() throws Exception
    {
        StringFactory factory = new StringFactory();
        TemporalIndexCache<String> cache = new TemporalIndexCache<>( factory );

        assertEquals( Iterables.count( cache ), 0 );

        cache.select( LOCAL_DATE_TIME );
        cache.select( ZONED_TIME );

        assertThat( cache, containsInAnyOrder( "LocalDateTime", "ZonedTime" ) );

        cache.select( DATE );
        cache.select( LOCAL_TIME );
        cache.select( LOCAL_DATE_TIME );
        cache.select( ZONED_TIME );
        cache.select( ZONED_DATE_TIME );
        cache.select( DURATION );

        assertThat( cache, containsInAnyOrder( "Date", "LocalDateTime", "ZonedDateTime", "LocalTime", "ZonedTime", "Duration" ) );
    }

    @SuppressWarnings( "Duplicates" )
    @Test
    public void stressCache() throws Exception
    {
        StringFactory factory = new StringFactory();
        TemporalIndexCache<String> cache = new TemporalIndexCache<>( factory );

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
        TemporalIndexCache<String> cache = new TemporalIndexCache<>( factory );
        Race race = new Race().withRandomStartDelays();
        MutableInt instantiatedAtClose = new MutableInt();
        race.addContestant( () ->
        {
            try
            {
                cache.uncheckedSelect( valueGroups[0] );
                cache.uncheckedSelect( valueGroups[1] );
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
            cache.uncheckedSelect( valueGroups[2] );
            fail( "No instantiation after closed" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
        assertEquals( instantiatedAtClose.intValue(), count( cache ) );
    }

    private static final ValueGroup[] valueGroups = {ZONED_DATE_TIME, LOCAL_DATE_TIME, DATE, ZONED_TIME, LOCAL_TIME, DURATION};

    static class CacheStresser extends Thread
    {
        private final TemporalIndexCache<String> cache;
        private final AtomicBoolean shouldContinue;
        private final Random r = new Random();

        CacheStresser( TemporalIndexCache<String> cache, AtomicBoolean shouldContinue )
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
                cache.select( valueGroups[r.nextInt( valueGroups.length )] );
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

    private static class StringFactory implements TemporalIndexCache.Factory<String>
    {
        AtomicInteger dateCounter = new AtomicInteger( 0 );
        AtomicInteger localDateTimeCounter = new AtomicInteger( 0 );
        AtomicInteger zonedDateTimeCounter = new AtomicInteger( 0 );
        AtomicInteger localTimeCounter = new AtomicInteger( 0 );
        AtomicInteger zonedTimeCounter = new AtomicInteger( 0 );
        AtomicInteger durationCounter = new AtomicInteger( 0 );

        @Override
        public String newDate()
        {
            updateCounterAndAssertSingleUpdate( dateCounter );
            return "Date";
        }

        @Override
        public String newLocalDateTime()
        {
            updateCounterAndAssertSingleUpdate( localDateTimeCounter );
            return "LocalDateTime";
        }

        @Override
        public String newZonedDateTime()
        {
            updateCounterAndAssertSingleUpdate( zonedDateTimeCounter );
            return "ZonedDateTime";
        }

        @Override
        public String newLocalTime()
        {
            updateCounterAndAssertSingleUpdate( localTimeCounter );
            return "LocalTime";
        }

        @Override
        public String newZonedTime()
        {
            updateCounterAndAssertSingleUpdate( zonedTimeCounter );
            return "ZonedTime";
        }

        @Override
        public String newDuration()
        {
            updateCounterAndAssertSingleUpdate( durationCounter );
            return "Duration";
        }

        private void updateCounterAndAssertSingleUpdate( AtomicInteger counter )
        {
            int count = counter.incrementAndGet();
            if ( count > 1 )
            {
                throw new IllegalStateException( "called new on same factory method multiple times" );
            }
        }
    }
}
