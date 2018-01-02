/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.metrics.source;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.metrics.MetricsSettings;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( "=== Java Virtual Machine Metrics\n\n" +
             "These metrics are environment dependent and they may vary on different hardware and with JVM configurations.\n" +
             "Typically these metrics will show information about garbage collections " +
             "(for example the number of events and time spent collecting), memory pools and buffers, and " +
             "finally the number of active threads running." )
public class JvmMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "vm";
    private static final String GC_PREFIX = name( NAME_PREFIX, "gc" );
    private static final String GC_TIME = name( GC_PREFIX, "time" );
    private static final String GC_COUNT = name( GC_PREFIX, "count" );
    private static final String MEMORY_POOL = name( NAME_PREFIX, "memory.pool" );
    private static final String MEMORY_BUFFER = name( NAME_PREFIX, "memory.buffer" );
    private static final String THREAD = name( NAME_PREFIX, "thread" );
    private final Config config;
    private final MetricRegistry registry;

    public JvmMetrics( Config config, MetricRegistry registry )
    {
        this.config = config;
        this.registry = registry;
    }

    @Override
    public void start() throws Throwable
    {
        // VM stats
        // Garbage collection
        if ( config.get( MetricsSettings.jvmGcEnabled ) )
        {
            for ( final GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans() )
            {
                registry.register( name( GC_TIME, prettifyName( gcBean.getName() ) ), new Gauge<Long>()
                {
                    @Override
                    public Long getValue()
                    {
                        return gcBean.getCollectionTime();
                    }
                } );

                registry.register( name( GC_COUNT, prettifyName( gcBean.getName() ) ), new Gauge<Long>()
                {
                    @Override
                    public Long getValue()
                    {
                        return gcBean.getCollectionCount();
                    }
                } );
            }
        }

        // Memory metrics
        if ( config.get( MetricsSettings.jvmMemoryEnabled ) )
        {
            for ( final MemoryPoolMXBean memPool : ManagementFactory.getMemoryPoolMXBeans() )
            {
                registry.register( name( MEMORY_POOL, prettifyName( memPool.getName() ) ), new Gauge<Long>()
                {
                    @Override
                    public Long getValue()
                    {
                        return memPool.getUsage().getUsed();
                    }
                } );
            }
        }

        // Buffer pools
        if ( config.get( MetricsSettings.jvmBuffersEnabled ) )
        {
            for ( final BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans( BufferPoolMXBean.class ) )
            {
                registry.register( name( MEMORY_BUFFER, prettifyName( pool.getName() ), "count" ), new Gauge<Long>()
                {
                    @Override
                    public Long getValue()
                    {
                        return pool.getCount();
                    }
                } );

                registry.register( name( MEMORY_BUFFER, prettifyName( pool.getName() ), "used" ), new Gauge<Long>()
                {
                    @Override
                    public Long getValue()
                    {
                        return pool.getMemoryUsed();
                    }
                } );

                registry.register( name( MEMORY_BUFFER, prettifyName( pool.getName() ), "capacity" ), new Gauge<Long>()
                {
                    @Override
                    public Long getValue()
                    {
                        return pool.getTotalCapacity();
                    }
                } );
            }
        }

        // Threads
        if ( config.get( MetricsSettings.jvmThreadsEnabled ) )
        {
            registry.register( name( THREAD, "count" ), new Gauge<Integer>()
            {
                @Override
                public Integer getValue()
                {
                    return Thread.activeCount();
                }
            } );
        }
    }

    @Override
    public void stop() throws IOException
    {
        registry.removeMatching( new MetricFilter()
        {
            @Override
            public boolean matches( String name, Metric metric )
            {
                return name.startsWith( NAME_PREFIX );
            }
        } );
    }

    private static String prettifyName( String name )
    {
        return name.toLowerCase().replace( ' ', '_' );
    }
}
