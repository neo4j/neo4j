/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.bolt.runtime.BoltConnectionMetricsMonitor;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Bolt metrics" )
public class BoltMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "neo4j.bolt";

    @Documented( "The total number of Bolt sessions started since this instance started. This includes both " +
                 "succeeded and failed sessions (deprecated, use connections_opened instead)." )
    public static final String SESSIONS_STARTED = name( NAME_PREFIX, "sessions_started" );

    @Documented( "The total number of Bolt connections opened since this instance started. This includes both " +
            "succeeded and failed connections." )
    public static final String CONNECTIONS_OPENED = name( NAME_PREFIX, "connections_opened" );

    @Documented( "The total number of Bolt connections closed since this instance started. This includes both " +
            "properly and abnormally ended connections." )
    public static final String CONNECTIONS_CLOSED = name( NAME_PREFIX, "connections_closed" );

    @Documented( "The total number of Bolt connections currently being executed." )
    public static final String CONNECTIONS_RUNNING = name( NAME_PREFIX, "connections_running" );

    @Documented( "The total number of Bolt connections sitting idle." )
    public static final String CONNECTIONS_IDLE = name( NAME_PREFIX, "connections_idle" );

    @Documented( "The total number of messages received via Bolt since this instance started." )
    public static final String MESSAGES_RECIEVED = name( NAME_PREFIX, "messages_received" );

    @Documented( "The total number of messages that began processing since this instance started. This is different " +
                 "from messages received in that this counter tracks how many of the received messages have" +
                 "been taken on by a worker thread." )
    public static final String MESSAGES_STARTED = name( NAME_PREFIX, "messages_started" );

    @Documented( "The total number of messages that completed processing since this instance started. This includes " +
                 "successful, failed and ignored Bolt messages." )
    public static final String MESSAGES_DONE = name( NAME_PREFIX, "messages_done" );

    @Documented( "The total number of messages that failed processing since this instance started." )
    public static final String MESSAGES_FAILED = name( NAME_PREFIX, "messages_failed" );

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
        registry.register( SESSIONS_STARTED, (Gauge<Long>) boltMonitor.connectionsOpened::get );
        registry.register( CONNECTIONS_OPENED, (Gauge<Long>) boltMonitor.connectionsOpened::get );
        registry.register( CONNECTIONS_CLOSED, (Gauge<Long>) boltMonitor.connectionsClosed::get );
        registry.register( CONNECTIONS_RUNNING, (Gauge<Long>) boltMonitor.connectionsActive::get );
        registry.register( CONNECTIONS_IDLE, (Gauge<Long>) boltMonitor.connectionsIdle::get );
        registry.register( MESSAGES_RECIEVED, (Gauge<Long>) boltMonitor.messagesReceived::get );
        registry.register( MESSAGES_STARTED, (Gauge<Long>) boltMonitor.messagesStarted::get );
        registry.register( MESSAGES_DONE, (Gauge<Long>) boltMonitor.messagesDone::get );
        registry.register( MESSAGES_FAILED, (Gauge<Long>) boltMonitor.messagesFailed::get );
        registry.register( TOTAL_QUEUE_TIME, (Gauge<Long>) boltMonitor.queueTime::get );
        registry.register( TOTAL_PROCESSING_TIME, (Gauge<Long>) boltMonitor.processingTime::get );
    }

    @Override
    public void stop()
    {
        registry.remove( SESSIONS_STARTED );
        registry.remove( CONNECTIONS_OPENED );
        registry.remove( CONNECTIONS_CLOSED );
        registry.remove( CONNECTIONS_IDLE );
        registry.remove( CONNECTIONS_RUNNING );
        registry.remove( MESSAGES_RECIEVED );
        registry.remove( MESSAGES_STARTED );
        registry.remove( MESSAGES_DONE );
        registry.remove( MESSAGES_FAILED );
        registry.remove( TOTAL_QUEUE_TIME );
        registry.remove( TOTAL_PROCESSING_TIME );
        monitors.removeMonitorListener( boltMonitor );
    }

    private class BoltMetricsMonitor implements BoltConnectionMetricsMonitor
    {
        final AtomicLong connectionsOpened = new AtomicLong();
        final AtomicLong connectionsClosed = new AtomicLong();

        final AtomicLong connectionsActive = new AtomicLong();
        final AtomicLong connectionsIdle = new AtomicLong();

        final AtomicLong messagesReceived = new AtomicLong();
        final AtomicLong messagesStarted = new AtomicLong();
        final AtomicLong messagesDone = new AtomicLong();
        final AtomicLong messagesFailed = new AtomicLong();

        // It will take about 300 million years of queue/processing time to overflow these
        // Even if we run a million processors concurrently, the instance would need to
        // run uninterrupted for three hundred years before the monitoring had a hiccup.
        final AtomicLong queueTime = new AtomicLong();
        final AtomicLong processingTime = new AtomicLong();

        @Override
        public void connectionOpened()
        {
            connectionsOpened.incrementAndGet();
            connectionsIdle.incrementAndGet();
        }

        @Override
        public void connectionActivated()
        {
            connectionsActive.incrementAndGet();
            connectionsIdle.decrementAndGet();
        }

        @Override
        public void connectionWaiting()
        {
            connectionsIdle.incrementAndGet();
            connectionsActive.decrementAndGet();
        }

        @Override
        public void messageReceived()
        {
            messagesReceived.incrementAndGet();
        }

        @Override
        public void messageProcessingStarted( long queueTime )
        {
            this.queueTime.addAndGet( queueTime );
            messagesStarted.incrementAndGet();
        }

        @Override
        public void messageProcessingCompleted( long processingTime )
        {
            this.processingTime.addAndGet( processingTime );
            messagesDone.incrementAndGet();
        }

        @Override
        public void messageProcessingFailed()
        {
            messagesFailed.incrementAndGet();
        }

        @Override
        public void connectionClosed()
        {
            connectionsClosed.incrementAndGet();
            connectionsIdle.decrementAndGet();
        }
    }
}
