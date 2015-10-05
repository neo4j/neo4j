/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import com.sun.management.GarbageCollectionNotificationInfo;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.metrics.MetricsSettings;

import static com.codahale.metrics.MetricRegistry.name;
import static com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION;

public class JvmMetrics implements Closeable
{
    private static final String NAME_PREFIX = "neo4j.vm";
    private static final String GC = name( NAME_PREFIX, "gc" );
    private static final String MEMORY_POOL = name( NAME_PREFIX, "memory.pool" );
    private static final String MEMORY_BUFFER = name( NAME_PREFIX, "memory.buffer" );
    private static final String THREAD = name( NAME_PREFIX, "thread" );
    private final MetricRegistry registry;
    private final List<Runnable> removeListenerHandlers = new ArrayList<>();

    public JvmMetrics( LogService logService, Config config, MetricRegistry registry )
    {
        this.registry = registry;

        // VM stats
        // Garbage collection
        if ( config.get( MetricsSettings.jvmGcEnabled ) )
        {
            for ( GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans() )
            {
                final AtomicLong periodGcDuration = new AtomicLong();
                registry.register( name( GC, prettifyName( gcBean.getName() ) ), new Gauge<Long>()
                {
                    public Long getValue()
                    {
                        return periodGcDuration.get();
                    }
                } );

                if ( gcBean instanceof NotificationEmitter )
                {
                    final NotificationEmitter emitter = (NotificationEmitter) gcBean;
                    final NotificationListener listener = new NotificationListener()
                    {
                        public void handleNotification( Notification notification, Object handback )
                        {
                            if ( notification.getType().equals( GARBAGE_COLLECTION_NOTIFICATION ) )
                            {
                                GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(
                                        (CompositeData) notification.getUserData() );
                                long duration = info.getGcInfo().getDuration();
                                periodGcDuration.addAndGet( duration );
                            }
                        }
                    };

                    emitter.addNotificationListener( listener, null, null );
                    removeListenerHandlers.add( new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            try
                            {
                                emitter.removeNotificationListener( listener );
                            }
                            catch ( ListenerNotFoundException e )
                            {
                                throw new RuntimeException( e );
                            }
                        }
                    } );
                }
                else
                {
                    logService.getInternalLog( getClass() ).warn(
                            "Failed to add listener for gc notifications on %s", gcBean );
                }
            }
        }

        // Memory metrics
        if ( config.get( MetricsSettings.jvmMemoryEnabled ) )
        {
            for ( final MemoryPoolMXBean memPool : ManagementFactory.getMemoryPoolMXBeans() )
            {
                registry.register( name( MEMORY_POOL, prettifyName( memPool.getName() ) ), new Gauge<Long>()
                {
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
                    public Long getValue()
                    {
                        return pool.getCount();
                    }
                } );

                registry.register( name( MEMORY_BUFFER, prettifyName( pool.getName() ), "used" ), new Gauge<Long>()
                {
                    public Long getValue()
                    {
                        return pool.getMemoryUsed();
                    }
                } );

                registry.register( name( MEMORY_BUFFER, prettifyName( pool.getName() ), "capacity" ),
                        new Gauge<Long>()
                        {
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
                public Integer getValue()
                {
                    return Thread.activeCount();
                }
            } );
        }
    }

    @Override
    public void close() throws IOException
    {
        for ( Runnable handle : removeListenerHandlers )
        {
            handle.run();
        }

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
