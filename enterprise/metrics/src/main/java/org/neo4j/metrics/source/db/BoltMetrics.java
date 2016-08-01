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
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.bolt.v1.runtime.MonitoredWorkerFactory;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Bolt Metrics" )
public class BoltMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "neo4j.bolt";

    @Documented( "The total number of messages received via Bolt since this instance started." )
    public static final String MESSAGES_RECIEVED = name( NAME_PREFIX, "messages_received" );
    @Documented( "The total number of messages work has started on since this instance started. This is different " +
                 "from messages received in that this counter tracks how many of the received messages have" +
                 "been taken on by a worker thread." )
    public static final String MESSAGES_STARTED = name( NAME_PREFIX, "messages_started" );
    @Documented( "The total number of messages work has completed on since this instance started. This includes " +
                 "successful, failed and ignored Bolt messages." )
    public static final String MESSAGES_DONE = name( NAME_PREFIX, "messages_done" );

    @Documented( "The accumulated time messages have spent waiting for a worker thread." )
    public static final String TOTAL_QUEUE_TIME = name( NAME_PREFIX, "accumulated_queue_time" );
    @Documented( "The accumulated time worker threads have spent processing messages." )
    public static final String TOTAL_PROCESSING_TIME = name( NAME_PREFIX, "accumulated_processing_time" );

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final BoltMetricsMonitor boltMonitor = new BoltMetricsMonitor();

    public BoltMetrics( MetricRegistry registry, Monitors monitors )
    {
        this.registry = registry;
        this.monitors = monitors;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( boltMonitor );
        registry.register( MESSAGES_RECIEVED, (Gauge<Long>) boltMonitor.recieved::get );
        registry.register( MESSAGES_STARTED, (Gauge<Long>) boltMonitor.started::get );
        registry.register( MESSAGES_DONE, (Gauge<Long>) boltMonitor.done::get );
        registry.register( TOTAL_QUEUE_TIME, (Gauge<Long>) boltMonitor.queueTime::get );
        registry.register( TOTAL_PROCESSING_TIME, (Gauge<Long>) boltMonitor.processingTime::get );
    }

    @Override
    public void stop()
    {
        registry.remove( MESSAGES_RECIEVED );
        registry.remove( MESSAGES_STARTED );
        registry.remove( MESSAGES_DONE );
        registry.remove( TOTAL_QUEUE_TIME );
        registry.remove( TOTAL_PROCESSING_TIME );
        monitors.removeMonitorListener( boltMonitor );
    }

    private class BoltMetricsMonitor implements MonitoredWorkerFactory.SessionMonitor
    {
        public final AtomicLong recieved = new AtomicLong();
        public final AtomicLong started = new AtomicLong();
        public final AtomicLong done = new AtomicLong();

        // It will take about 300 million years of queue/processing time to overflow these
        // Even if we run a million processors concurrently, the instance would need to
        // run uninterrupted for three hundred years before the monitoring had a hiccup.
        public final AtomicLong queueTime = new AtomicLong();
        public final AtomicLong processingTime = new AtomicLong();

        @Override
        public void messageReceived()
        {
            recieved.incrementAndGet();
        }

        @Override
        public void processingStarted( long queueTime )
        {
            this.queueTime.addAndGet( queueTime );
            started.incrementAndGet();
        }

        @Override
        public void processingDone( long processingTime )
        {
            this.processingTime.addAndGet( processingTime );
            done.incrementAndGet();
        }
    }
}
