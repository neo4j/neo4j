/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.Map;

import org.neo4j.bolt.v1.runtime.internal.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Thin wrapper around {@link Sessions} that adds monitoring capabilities, which
 * means Bolt can be introspected at runtime by adding Monitor listeners.
 *
 * This adds no overhead if no listeners are registered.
 */
public class MonitoredSessions implements Sessions
{
    private final SessionMonitor monitor;
    private final Sessions delegate;
    private final Clock clock;
    private final Monitors monitors;

    public MonitoredSessions( Monitors monitors, Sessions delegate, Clock clock )
    {
        this.delegate = delegate;
        this.clock = clock;
        this.monitors = monitors;
        this.monitor = this.monitors.newMonitor( SessionMonitor.class );
    }

    @Override
    public Session newSession( String connectionDescriptor, boolean isEncrypted )
    {
        if( monitors.hasListeners( SessionMonitor.class ) )
        {
            return new MonitoredSession( monitor, delegate.newSession( connectionDescriptor, isEncrypted ), clock );
        }
        return delegate.newSession( connectionDescriptor, isEncrypted );
    }

    static class MonitoredSession implements Session
    {
        private final SessionMonitor monitor;
        private final Session delegate;
        private final Clock clock;

        public MonitoredSession( SessionMonitor monitor, Session delegate, Clock clock )
        {
            this.monitor = monitor;
            this.delegate = delegate;
            this.clock = clock;
        }

        @Override
        public String key()
        {
            return delegate.key();
        }

        public String connectionDescriptor()
        {
            return delegate.connectionDescriptor();
        }

        @Override
        public <A> void init( String clientName, Map<String,Object> authToken, A attachment, Callback<Boolean,A> callback )
        {
            monitor.messageReceived();
            delegate.init( clientName, authToken, attachment, withMonitor( callback ) );
        }

        @Override
        public <A> void run( String statement, Map<String,Object> params, A attachment,
                Callback<StatementMetadata,A> callback )
        {
            monitor.messageReceived();
            delegate.run( statement, params, attachment, withMonitor( callback ) );
        }

        @Override
        public <A> void pullAll( A attachment, Callback<RecordStream,A> callback )
        {
            monitor.messageReceived();
            delegate.pullAll( attachment, withMonitor( callback ) );
        }

        @Override
        public <A> void discardAll( A attachment, Callback<Void,A> callback )
        {
            monitor.messageReceived();
            delegate.discardAll( attachment, withMonitor( callback ) );
        }

        @Override
        public <A> void reset( A attachment, Callback<Void,A> callback )
        {
            monitor.messageReceived();
            delegate.reset( attachment, withMonitor( callback ) );
        }

        @Override
        public <A> void ackFailure( A attachment, Callback<Void,A> callback )
        {
            monitor.messageReceived();
            delegate.ackFailure( attachment, withMonitor( callback ) );
        }

        @Override
        public void interrupt()
        {
            delegate.interrupt();
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        private <R, A> Callback<R,A> withMonitor( Callback<R,A> callback )
        {
            return new Callback<R,A>()
            {
                private final long start = clock.millis();
                private long queueTime = 0;

                @Override
                public void started( A attachment )
                {
                    queueTime = clock.millis() - start;
                    monitor.processingStarted( queueTime );
                    callback.started( attachment );
                }

                @Override
                public void result( R result, A attachment ) throws Exception
                {
                    callback.result( result, attachment );
                }

                @Override
                public void failure( Neo4jError err, A attachment )
                {
                    callback.failure( err, attachment );
                    callMonitorDone();
                }

                @Override
                public void completed( A attachment )
                {
                    callback.completed( attachment );
                    callMonitorDone();
                }

                @Override
                public void ignored( A attachment )
                {
                    callback.ignored( attachment );
                    callMonitorDone();
                }

                private void callMonitorDone()
                {
                    monitor.processingDone( (clock.millis() - start) - queueTime );
                }
            };
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
