/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
        createIOLimiter( Config.embeddedDefaults( settings ) );
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

    @Test
    public void configuredLimitMustReflectCurrentState()
    {
        createIOLimiter( 100 );

        assertThat( limiter.isLimited(), is( true ) );
        multipleDisableShouldReportUnlimited( limiter );
        assertThat( limiter.isLimited(), is( true ) );
    }

    @Test
    public void configuredDisabledLimitShouldBeUnlimited()
    {
        createIOLimiter( -1 );

        assertThat( limiter.isLimited(), is( false ) );
        multipleDisableShouldReportUnlimited( limiter );
        assertThat( limiter.isLimited(), is( false ) );
    }

    @Test
    public void unlimitedShouldAlwaysBeUnlimited()
    {
        IOLimiter limiter = IOLimiter.unlimited();

        assertThat( limiter.isLimited(), is( false ) );
        multipleDisableShouldReportUnlimited( limiter );
        assertThat( limiter.isLimited(), is( false ) );
    }

    private static void multipleDisableShouldReportUnlimited( IOLimiter limiter )
    {
        limiter.disableLimit();
        try
        {
            assertThat( limiter.isLimited(), is( false ) );
            limiter.disableLimit();
            try
            {
                assertThat( limiter.isLimited(), is( false ) );
            }
            finally
            {
                limiter.enableLimit();
            }
            assertThat( limiter.isLimited(), is( false ) );
        }
        finally
        {
            limiter.enableLimit();
        }
    }
}
