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

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.v1.runtime.MonitoredWorkerFactory.MonitoredBoltWorker;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.time.Clocks.systemClock;

public class MonitoredBoltWorkerFactoryTest
{
    private static final BoltChannel boltChannel = mock( BoltChannel.class );

    @Test
    public void shouldSignalReceivedStartAndComplete() throws Throwable
    {
        // given
        FakeClock clock = Clocks.fakeClock();

        WorkerFactory delegate = mock( WorkerFactory.class );
        BoltStateMachine machine = mock( BoltStateMachine.class );
        when( delegate.newWorker( boltChannel ) )
                .thenReturn( new BoltWorker()
                {
                    @Override
                    public void enqueue( Job job )
                    {
                        clock.forward( 1337, TimeUnit.MILLISECONDS );
                        try
                        {
                            job.perform( machine );
                        }
                        catch ( BoltConnectionFatality connectionFatality )
                        {
                            throw new RuntimeException( connectionFatality );
                        }
                    }

                    @Override
                    public void interrupt()
                    {
                        throw new RuntimeException();
                    }

                    @Override
                    public void halt()
                    {
                        throw new RuntimeException();
                    }

                } );

        Monitors monitors = new Monitors();
        CountingSessionMonitor monitor = new CountingSessionMonitor();
        monitors.addMonitorListener( monitor );

        MonitoredWorkerFactory workerFactory = new MonitoredWorkerFactory( monitors, delegate, clock );
        BoltWorker worker = workerFactory.newWorker( boltChannel );

        // when
        worker.enqueue( stateMachine ->
        {
            stateMachine.run( "hello", null, nullResponseHandler() );
            clock.forward( 1338, TimeUnit.MILLISECONDS );
        } );

        // then
        assertEquals( 1, monitor.messagesReceived );
        assertEquals( 1337, monitor.queueTime );
        assertEquals( 1338, monitor.processingTime );
    }

    @Test
    public void shouldReportStartedSessions()
    {
        int workersCount = 42;

        Monitors monitors = new Monitors();
        CountingSessionMonitor monitor = new CountingSessionMonitor();
        monitors.addMonitorListener( monitor );

        WorkerFactory mockWorkers = mock( WorkerFactory.class );
        when( mockWorkers.newWorker( boltChannel ) ).thenReturn( mock( BoltWorker.class ) );

        MonitoredWorkerFactory workerFactory = new MonitoredWorkerFactory( monitors, mockWorkers, systemClock() );

        for ( int i = 0; i < workersCount; i++ )
        {
            workerFactory.newWorker( boltChannel );
        }

        assertEquals( workersCount, monitor.sessionsStarted );
    }

    @Test
    public void shouldNotWrapWithMonitoredSessionIfNobodyIsListening() throws Throwable
    {
        // Given
        // Monitoring adds GC overhead, so we only want to do the work involved
        // if someone has actually registered a listener. We still allow plugging
        // monitoring in at runtime, but it will only apply to sessions started
        // after monitor listeners are added
        WorkerFactory workerFactory = mock( WorkerFactory.class );
        BoltWorker boltWorker = mock( BoltWorker.class );
        when( workerFactory.newWorker( boltChannel ) ).thenReturn( boltWorker );

        Monitors monitors = new Monitors();
        MonitoredWorkerFactory monitoredWorkerFactory = new MonitoredWorkerFactory( monitors, workerFactory, Clocks.fakeClock() );

        // When

        BoltWorker worker = monitoredWorkerFactory.newWorker( boltChannel );

        // Then
        assertEquals( boltWorker, worker );

        // But when I register a listener
        monitors.addMonitorListener( new CountingSessionMonitor() );

        // Then new sessions should be monitored
        assertThat( monitoredWorkerFactory.newWorker( boltChannel ), instanceOf( MonitoredBoltWorker.class ) );
    }

    private static class CountingSessionMonitor implements MonitoredWorkerFactory.SessionMonitor
    {
       long sessionsStarted;
       long messagesReceived;
       long queueTime;
       long processingTime;

        @Override
        public void sessionStarted()
        {
            sessionsStarted++;
        }

        @Override
        public void messageReceived()
        {
            messagesReceived++;
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
}
