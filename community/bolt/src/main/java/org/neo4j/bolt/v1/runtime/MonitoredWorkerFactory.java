/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltConnectionDescriptor;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Thin wrapper around {@link WorkerFactory} that adds monitoring capabilities, which
 * means Bolt can be introspected at runtime by adding Monitor listeners.
 *
 * This adds no overhead if no listeners are registered.
 */
public class MonitoredWorkerFactory implements WorkerFactory
{
    private final SessionMonitor monitor;
    private final WorkerFactory delegate;
    private final Clock clock;
    private final Monitors monitors;

    public MonitoredWorkerFactory( Monitors monitors, WorkerFactory delegate, Clock clock )
    {
        this.delegate = delegate;
        this.clock = clock;
        this.monitors = monitors;
        this.monitor = this.monitors.newMonitor( SessionMonitor.class );
    }

    @Override
    public BoltWorker newWorker( BoltChannel boltChannel )
    {
        if ( monitors.hasListeners( SessionMonitor.class ) )
        {
            return new MonitoredBoltWorker( monitor, delegate.newWorker( boltChannel ), clock );
        }
        return delegate.newWorker( boltChannel );
    }

    static class MonitoredBoltWorker implements BoltWorker
    {
        private final SessionMonitor monitor;
        private final BoltWorker delegate;
        private final Clock clock;

        MonitoredBoltWorker( SessionMonitor monitor, BoltWorker delegate, Clock clock )
        {
            this.monitor = monitor;
            this.delegate = delegate;
            this.clock = clock;

            this.monitor.sessionStarted();
        }

        @Override
        public void enqueue( Job job )
        {
            monitor.messageReceived();
            long start = clock.millis();
            delegate.enqueue( session ->
            {
                long queueTime = clock.millis() - start;
                monitor.processingStarted( queueTime );
                job.perform( session );
                monitor.processingDone( (clock.millis() - start) - queueTime );
            } );
        }

        @Override
        public void interrupt()
        {
            delegate.interrupt();
        }

        @Override
        public void halt()
        {
            delegate.halt();
        }
    }

    /**
     * For monitoring the Bolt protocol, implementing and registering this monitor allows
     * tracking requests arriving via the Bolt protocol and the queuing and processing times
     * of those requests.
     */
    public interface SessionMonitor
    {
        /**
         * Called when a new Bolt session (backed by a {@link BoltWorker}) is started.
         */
        void sessionStarted();

        /**
         * Called whenever a request is received. This happens after a request is
         * deserialized, but before it is queued pending processing.
         */
        void messageReceived();

        /**
         * Called after a request is done queueing, right before the worker thread takes on the request
         * @param queueTime time between {@link #messageReceived()} and this call, in milliseconds
         */
        void processingStarted( long queueTime );

        /**
         * Called after a request has been processed by the worker thread - this will
         * be called independent of if the request is successful, failed or ignored.
         * @param processingTime time between {@link #processingStarted(long)} and this call, in milliseconds
         */
        void processingDone( long processingTime );
    }
}
