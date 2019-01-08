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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import static com.codahale.metrics.MetricRegistry.name;

public class ThreadMetrics extends JvmMetrics
{
    public static final String THREAD_COUNT = name( JvmMetrics.NAME_PREFIX, "thread.count" );
    public static final String THREAD_TOTAL = name( JvmMetrics.NAME_PREFIX, "thread.total" );

    private final MetricRegistry registry;
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    public ThreadMetrics( MetricRegistry registry )
    {
        this.registry = registry;
    }

    @Override
    public void start()
    {
        registry.register( THREAD_COUNT, (Gauge<Integer>) Thread::activeCount );
        registry.register( THREAD_TOTAL, (Gauge<Integer>) threadMXBean::getThreadCount );
    }

    @Override
    public void stop()
    {
        registry.remove( THREAD_COUNT );
        registry.remove( THREAD_TOTAL );
    }
}

