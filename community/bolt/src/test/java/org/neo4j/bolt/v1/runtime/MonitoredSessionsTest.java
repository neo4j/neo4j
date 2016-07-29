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
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import org.junit.Test;

import org.neo4j.bolt.v1.runtime.MonitoredSessions.MonitoredSession;
import org.neo4j.bolt.v1.runtime.internal.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.kernel.api.bolt.HaltableUserSession;
import org.neo4j.kernel.monitoring.Monitors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.bolt.v1.runtime.Session.Callback.noOp;

public class MonitoredSessionsTest
{
    @Test
    public void shouldSignalReceivedStartAndComplete() throws Throwable
    {
        // given
        Sessions delegate = mock( Sessions.class );
        ControlledCompletionSession innerSession = new ControlledCompletionSession();
        when( delegate.newSession( anyString(), anyBoolean() ) ).thenReturn( innerSession );

        Monitors monitors = new Monitors();
        CountingSessionMonitor monitor = new CountingSessionMonitor();
        monitors.addMonitorListener( monitor );

        FakeClock clock = new FakeClock();

        MonitoredSessions sessions = new MonitoredSessions( monitors, delegate, clock );
        Session session = sessions.newSession( "<test>" );

        // when
        session.run( "hello", null, noOp() );
        clock.forward( 1337 );
        innerSession.callback.started();
        clock.forward( 1338 );
        innerSession.callback.completed();

        // then
        assertEquals( 1, monitor.messagesRecieved );
        assertEquals( 1337, monitor.queueTime );
        assertEquals( 1338, monitor.processingTime );
    }

    @Test
    public void shouldNotWrapWithMonitoredSessionIfNobodyIsListening() throws Throwable
    {
        // Given
        // Monitoring adds GC overhead, so we only want to do the work involved
        // if someone has actually registered a listener. We still allow plugging
        // monitoring in at runtime, but it will only apply to sessions started
        // after monitor listeners are added
        Sessions innerSessions = mock( Sessions.class );
        Session innerSession = mock( Session.class );
        when(innerSessions.newSession( anyString(), anyBoolean() ) ).thenReturn( innerSession );

        Monitors monitors = new Monitors();
        MonitoredSessions sessions = new MonitoredSessions( monitors, innerSessions, new FakeClock() );

        // When
        Session session = sessions.newSession( "<test>" );

        // Then
        assertEquals( innerSession, session );

        // But when I register a listener
        monitors.addMonitorListener( new CountingSessionMonitor() );

        // Then new sessions should be monitored
        assertThat( sessions.newSession( "<test>" ), instanceOf( MonitoredSession.class ) );
    }

    private static class CountingSessionMonitor implements MonitoredSessions.SessionMonitor
    {
       long messagesRecieved = 0;
       long queueTime = 0;
       long processingTime = 0;

        @Override
        public void messageReceived()
        {
            messagesRecieved++;
        }

        @Override
        public void processingStarted( long queueTime )
        {
            this.queueTime += queueTime;
        }

        @Override
        public void processingDone( long processingTime )
        {
            this.processingTime += processingTime;
        }
    }

    private static class ControlledCompletionSession extends HaltableUserSession.Adapter implements Session
    {
        Callback callback;

        @Override
        public String key()
        {
            return null;
        }

        public String connectionDescriptor()
        {
            return "<test>";
        }

        @Override
        public void init( String clientName, Map<String, Object> authToken, long currentHighestTransactionId,
                          Callback<Boolean> callback )
        {
            this.callback = callback;
        }

        @Override
        public void run( String statement, Map<String, Object> params,
                         Callback<StatementMetadata> callback )
        {
            this.callback = callback;
        }

        @Override
        public void pullAll( Callback<RecordStream> callback )
        {
            this.callback = callback;
        }

        @Override
        public void discardAll( Callback<Void> callback )
        {
            this.callback = callback;
        }

        @Override
        public void reset( Callback<Void> callback )
        {
            this.callback = callback;
        }

        @Override
        public void externalError( Neo4jError error, Callback<Void> callback )
        {

        }

        @Override
        public void ackFailure( Callback<Void> callback )
        {

        }

        @Override
        public void interrupt()
        {

        }

        @Override
        public void close()
        {

        }
    }

    // Note that this is java.time.clock, not Neo4j clock.
    // However, still duplicate of FakeClock class in `neo4j-common` PR,
    // resolve once that PR is merged.
    private class FakeClock extends Clock
    {
        private long millis = 0;

        @Override
        public ZoneId getZone()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Clock withZone( ZoneId zone )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant()
        {
            return Instant.ofEpochMilli( millis );
        }

        void forward( long delta )
        {
            this.millis += delta;
        }
    }
}
