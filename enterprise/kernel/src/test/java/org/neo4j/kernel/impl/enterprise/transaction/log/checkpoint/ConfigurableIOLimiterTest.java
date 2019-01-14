/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
    private static final String ORIGIN = "test";
    private ConfigurableIOLimiter limiter;
    private Config config;
    private AtomicLong pauseNanosCounter;
    private static final Flushable FLUSHABLE = () -> {};

    private void createIOLimiter( Config config )
    {
        this.config = config;
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
    public void mustPutDefaultLimitOnIOWhenNoLimitIsConfigured()
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
    public void mustNotPutLimitOnIOWhenConfiguredToBeUnlimited()
    {
        createIOLimiter( -1 );
        assertUnlimited();
    }

    private void assertUnlimited()
    {
        long pauseTime = pauseNanosCounter.get();
        repeatedlyCallMaybeLimitIO( limiter, IOLimiter.INITIAL_STAMP, 1000000 );
        assertThat( pauseNanosCounter.get(), is( pauseTime ) );
    }

    @Test
    public void mustNotPutLimitOnIOWhenLimitingIsDisabledAndNoLimitIsConfigured()
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
    public void mustRestrictIORateToConfiguredLimit()
    {
        createIOLimiter( 100 );

        // Do 10*100 = 1000 IOs real quick, when we're limited to 100 IOPS.
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );

        // This should have led to about 10 seconds of pause, minus the time we spent in the loop.
        // So let's say 9 seconds - experiments indicate this gives us about a 10x margin.
        assertThat( pauseNanosCounter.get(), greaterThan( TimeUnit.SECONDS.toNanos( 9 ) ) );
    }

    private long repeatedlyCallMaybeLimitIO( IOLimiter ioLimiter, long stamp, int iosPerIteration )
    {
        for ( int i = 0; i < 100; i++ )
        {
            stamp = ioLimiter.maybeLimitIO( stamp, iosPerIteration, FLUSHABLE );
        }
        return stamp;
    }

    @Test
    public void mustNotRestrictIOToConfiguredRateWhenLimitIsDisabled()
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
    public void dynamicConfigurationUpdateMustBecomeVisible()
    {
        // Create initially unlimited
        createIOLimiter( 0 );

        // Then set a limit of 100 IOPS
        config.updateDynamicSetting( GraphDatabaseSettings.check_point_iops_limit.name(), "100", ORIGIN );

        // Do 10*100 = 1000 IOs real quick, when we're limited to 100 IOPS.
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );

        // Then assert that the updated limit is respected
        assertThat( pauseNanosCounter.get(), greaterThan( TimeUnit.SECONDS.toNanos( 9 ) ) );

        // Change back to unlimited
        config.updateDynamicSetting( GraphDatabaseSettings.check_point_iops_limit.name(), "-1", ORIGIN );

        // And verify it's respected
        assertUnlimited();
    }

    @Test
    public void dynamicConfigurationUpdateEnablingLimiterMustNotDisableLimiter()
    {
        // Create initially unlimited
        createIOLimiter( 0 );
        // Disable the limiter...
        limiter.disableLimit();
        // ...while a dynamic configuration update happens
        config.updateDynamicSetting( GraphDatabaseSettings.check_point_iops_limit.name(), "100", ORIGIN );
        // The limiter must still be disabled...
        assertUnlimited();
        // ...and re-enabling it...
        limiter.enableLimit();
        // ...must make the limiter limit.
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
        assertThat( pauseNanosCounter.get(), greaterThan( TimeUnit.SECONDS.toNanos( 9 ) ) );
    }

    @Test
    public void dynamicConfigurationUpdateDisablingLimiterMustNotDisableLimiter()
    {
        // Create initially limited
        createIOLimiter( 100 );
        // Disable the limiter...
        limiter.disableLimit();
        // ...while a dynamic configuration update happens
        config.updateDynamicSetting( GraphDatabaseSettings.check_point_iops_limit.name(), "-1", ORIGIN );
        // The limiter must still be disabled...
        assertUnlimited();
        // ...and re-enabling it...
        limiter.enableLimit();
        // ...must maintain the limiter disabled.
        assertUnlimited();
        // Until it is re-enabled.
        config.updateDynamicSetting( GraphDatabaseSettings.check_point_iops_limit.name(), "100", ORIGIN );
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( limiter, stamp, 10 );
        assertThat( pauseNanosCounter.get(), greaterThan( TimeUnit.SECONDS.toNanos( 9 ) ) );
    }
}
