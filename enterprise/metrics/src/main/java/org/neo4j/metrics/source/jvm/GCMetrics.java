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
package org.neo4j.metrics.source.jvm;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;
import static org.neo4j.metrics.source.jvm.JvmMetrics.NAME_PREFIX;
import static org.neo4j.metrics.source.jvm.JvmMetrics.prettifyName;

public class GCMetrics extends LifecycleAdapter implements Lifecycle
{
    public static final String GC_PREFIX = name( NAME_PREFIX, "gc" );
    public static final String GC_TIME = name( GC_PREFIX, "time" );
    public static final String GC_COUNT = name( GC_PREFIX, "count" );

    private final MetricRegistry registry;

    public GCMetrics( MetricRegistry registry )
    {
        this.registry = registry;
    }

    @Override
    public void start()
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

    @Override
    public void stop()
    {
        registry.removeMatching( new MetricFilter()
        {
            @Override
            public boolean matches( String name, Metric metric )
            {
                return name.startsWith( GC_PREFIX );
            }
        } );
    }
}
