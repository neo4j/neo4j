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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;
import static org.neo4j.metrics.source.JvmMetrics.prettifyName;

public class MemoryBuffersMetrics extends LifecycleAdapter implements Lifecycle
{
    public static final String MEMORY_BUFFER = name( JvmMetrics.NAME_PREFIX, "memory.buffer" );

    private final MetricRegistry registry;

    public MemoryBuffersMetrics( MetricRegistry registry )
    {
        this.registry = registry;
    }

    @Override
    public void start()
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

    @Override
    public void stop()
    {
        registry.removeMatching( new MetricFilter()
        {
            @Override
            public boolean matches( String name, Metric metric )
            {
                return name.startsWith( MEMORY_BUFFER );
            }
        } );
    }
}
