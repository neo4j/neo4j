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
package org.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;

import static com.codahale.metrics.MetricRegistry.name;

public class MemoryBuffersMetrics extends JvmMetrics
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
            registry.register(
                    name( MEMORY_BUFFER, prettifyName( pool.getName() ), "count" ), (Gauge<Long>) pool::getCount );
            registry.register(
                    name( MEMORY_BUFFER, prettifyName( pool.getName() ), "used" ), (Gauge<Long>) pool::getMemoryUsed );
            registry.register(
                    name( MEMORY_BUFFER, prettifyName( pool.getName() ), "capacity" ),
                    (Gauge<Long>) pool::getTotalCapacity );
        }
    }

    @Override
    public void stop()
    {
        registry.removeMatching( ( name, metric ) -> name.startsWith( MEMORY_BUFFER ) );
    }
}
