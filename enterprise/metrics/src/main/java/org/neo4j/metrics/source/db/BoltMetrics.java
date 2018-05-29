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

    @Documented( "The total number of Bolt sessions started since this instance started. This includes both " +
                 "succeeded and failed sessions." )
    public static final String SESSIONS_STARTED = name( NAME_PREFIX, "sessions_started" );
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
        registry.register( SESSIONS_STARTED, (Gauge<Long>) boltMonitor.sessionsStarted::get );
        registry.register( MESSAGES_RECIEVED, (Gauge<Long>) boltMonitor.messagesReceived::get );
        registry.register( MESSAGES_STARTED, (Gauge<Long>) boltMonitor.messagesStarted::get );
        registry.register( MESSAGES_DONE, (Gauge<Long>) boltMonitor.messagesDone::get );
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
        final AtomicLong sessionsStarted = new AtomicLong();

        final AtomicLong messagesReceived = new AtomicLong();
        final AtomicLong messagesStarted = new AtomicLong();
        final AtomicLong messagesDone = new AtomicLong();

        // It will take about 300 million years of queue/processing time to overflow these
        // Even if we run a million processors concurrently, the instance would need to
        // run uninterrupted for three hundred years before the monitoring had a hiccup.
        final AtomicLong queueTime = new AtomicLong();
        final AtomicLong processingTime = new AtomicLong();

        @Override
        public void sessionStarted()
        {
            sessionsStarted.incrementAndGet();
        }

        @Override
        public void messageReceived()
        {
            messagesReceived.incrementAndGet();
        }

        @Override
        public void processingStarted( long queueTime )
        {
            this.queueTime.addAndGet( queueTime );
            messagesStarted.incrementAndGet();
        }

        @Override
        public void processingDone( long processingTime )
        {
            this.processingTime.addAndGet( processingTime );
            messagesDone.incrementAndGet();
        }
    }
}
