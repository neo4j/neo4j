/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjLongConsumer;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.checkpoint.Rush;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ConfigurableCheckPointFlushControlTest
{
    private ConfigurableCheckPointFlushControl control;
    private AtomicLong pauseNanosCounter;

    private void createFlushControl( Config config )
    {
        pauseNanosCounter = new AtomicLong();
        ObjLongConsumer<Object> pauseNanos = ( blocker, nanos ) -> pauseNanosCounter.getAndAdd( nanos );
        control = new ConfigurableCheckPointFlushControl( config, pauseNanos );
    }

    private void createFlushControl( int limit )
    {
        Map<String, String> settings = stringMap(
                GraphDatabaseSettings.check_point_iops_limit.name(), "" + limit );
        createFlushControl( new Config( settings ) );
    }

    @Test
    public void mustNotPutLimitOnIOWhenNoLimitIsConfigured() throws Exception
    {
        createFlushControl( Config.defaults() );
        assertThat( control.getIOLimiter(), sameInstance( IOLimiter.unlimited() ) );
    }

    @Test
    public void mustNotPutLimitOnIODuringTemporaryRushWhenNoLimitIsConfigured() throws Exception
    {
        createFlushControl( Config.defaults() );
        try ( Rush ignore = control.beginTemporaryRush() )
        {
            assertThat( control.getIOLimiter(), sameInstance( IOLimiter.unlimited() ) );
            //noinspection unused
            try ( Rush ignore2 = control.beginTemporaryRush() )
            {
                assertThat( control.getIOLimiter(), sameInstance( IOLimiter.unlimited() ) );
            }
        }
    }

    @Test
    public void mustRestrictIORateToConfiguredLimit() throws Exception
    {
        createFlushControl( 100 );

        // Do 10*100 = 1000 IOs real quick, when we're limited to 100 IOPS.
        IOLimiter ioLimiter = control.getIOLimiter();
        long stamp = IOLimiter.INITIAL_STAMP;
        repeatedlyCallMaybeLimitIO( ioLimiter, stamp );

        // This should have led to about 10 seconds of pause, minus the time we spent in the loop.
        // So let's say 9 seconds - experiments indicate this gives us about a 10x margin.
        assertThat( pauseNanosCounter.get(), greaterThan( TimeUnit.SECONDS.toNanos( 9 ) ) );
    }

    private long repeatedlyCallMaybeLimitIO( IOLimiter ioLimiter, long stamp ) throws IOException
    {
        for ( int i = 0; i < 100; i++ )
        {
            stamp = ioLimiter.maybeLimitIO( stamp, 10, () -> {} );
        }
        return stamp;
    }

    @Test
    public void mustNotRestrictIOToConfiguredRateWhenRushing() throws Exception
    {
        createFlushControl( 100 );

        IOLimiter ioLimiter = control.getIOLimiter();
        long stamp = IOLimiter.INITIAL_STAMP;
        try ( Rush ignore = control.beginTemporaryRush() )
        {
            stamp = repeatedlyCallMaybeLimitIO( ioLimiter, stamp );
            try ( Rush ignore2 = control.beginTemporaryRush() )
            {
                stamp = repeatedlyCallMaybeLimitIO( ioLimiter, stamp );
            }
            repeatedlyCallMaybeLimitIO( ioLimiter, stamp );
        }

        // We should've spent no time rushing
        assertThat( pauseNanosCounter.get(), is( 0L ) );
    }
}
