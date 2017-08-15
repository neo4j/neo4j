/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.junit.Test;

import java.io.Flushable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjLongConsumer;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.configuration.Config;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ConfigurableIOLimiterTest
{
    private ConfigurableIOLimiter limiter;
    private AtomicLong pauseNanosCounter;
    private static final Flushable FLUSHABLE = () -> {};

    private void createIOLimiter( Config config )
    {
        pauseNanosCounter = new AtomicLong();
        ObjLongConsumer<Object> pauseNanos = ( blocker, nanos ) -> pauseNanosCounter.getAndAdd( nanos );
        limiter = new ConfigurableIOLimiter( config, pauseNanos );
    }

    private void createIOLimiter( int limit )
    {
        Map<String, String> settings = stringMap(
                GraphDatabaseSettings.check_point_iops_limit.name(), "" + limit );
        createIOLimiter( Config.defaults( settings ) );
    }

    @Test
    public void mustPutDefaultLimitOnIOWhenNoLimitIsConfigured() throws Exception
    {
        createIOLimiter( Config.defaults() );

        // Do 100*100 = 10000 IOs real quick, when we're limited to 1000 IOPS.
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 100 );

        // This should have led to about 10 seconds of pause, minus the time we spent in the loop.
        // So let's say 9 seconds - experiments indicate this gives us about a 10x margin.
        assertThat( pauseNanosCounter.get(), greaterThan( TimeUnit.SECONDS.toNanos( 9 ) ) );
    }

    @Test
    public void mustNotPutLimitOnIOWhenConfiguredToBeUnlimited() throws Exception
    {
        createIOLimiter( -1 );
        assertUnlimited();
    }

    private void assertUnlimited() throws IOException
    {
        long pauseTime = pauseNanosCounter.get();
        repeatedlyCallMaybeLimitIO( limiter, IOLimiter.INITIAL_STAMP, 1000000 );
        assertThat( pauseNanosCounter.get(), is( pauseTime ) );
    }

    @Test
    public void mustNotPutLimitOnIOWhenLimitingIsDisabledAndNoLimitIsConfigured() throws Exception
    {
        createIOLimiter( Config.defaults() );
        limiter.disableLimit();
        try
        {
            assertUnlimited();
            limiter.disableLimit();
            try
            {
                assertUnlimited();
            }
            finally
            {
                limiter.enableLimit();
            }
        }
        finally
        {
            limiter.enableLimit();
        }
    }

    @Test
    public void mustRestrictIORateToConfiguredLimit() throws Exception
    {
        createIOLimiter( 100 );

        // Do 10*100 = 1000 IOs real quick, when we're limited to 100 IOPS.
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );

        // This should have led to about 10 seconds of pause, minus the time we spent in the loop.
        // So let's say 9 seconds - experiments indicate this gives us about a 10x margin.
        assertThat( pauseNanosCounter.get(), greaterThan( TimeUnit.SECONDS.toNanos( 9 ) ) );
    }

    private long repeatedlyCallMaybeLimitIO( IOLimiter ioLimiter, long stamp, int iosPerIteration ) throws IOException
    {
        for ( int i = 0; i < 100; i++ )
        {
            stamp = ioLimiter.maybeLimitIO( stamp, iosPerIteration, FLUSHABLE );
        }
        return stamp;
    }

    @Test
    public void mustNotRestrictIOToConfiguredRateWhenLimitIsDisabled() throws Exception
    {
        createIOLimiter( 100 );

        long stamp = IOLimiter.INITIAL_STAMP;
        limiter.disableLimit();
        try
        {
            stamp = repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
            limiter.disableLimit();
            try
            {
                stamp = repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
            }
            finally
            {
                limiter.enableLimit();
            }
            repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
        }
        finally
        {
            limiter.enableLimit();
        }

        // We should've spent no time rushing
        assertThat( pauseNanosCounter.get(), is( 0L ) );
    }
}
