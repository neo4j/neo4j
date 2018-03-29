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

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.values.storable.ValueGroup;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
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

    @Test
    public void stressCache() throws Exception
    {
        StringFactory factory = new StringFactory();
        TemporalIndexCache<String> cache = new TemporalIndexCache<>( factory );

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

    private static final ValueGroup[] valueGroups = {
            ZONED_DATE_TIME,
            LOCAL_DATE_TIME,
            DATE,
            ZONED_TIME,
            LOCAL_TIME,
            DURATION};

    static class CacheStresser extends Thread
    {
        TemporalIndexCache<String> cache;
        Random r = new Random();
        Exception failed;

        CacheStresser( TemporalIndexCache<String> cache )
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
            cache.select( valueGroups[r.nextInt( valueGroups.length )] );
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

    static class StringFactory implements TemporalIndexCache.Factory<String>
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
