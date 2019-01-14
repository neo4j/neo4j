/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.runtime;

import org.junit.Test;

import java.util.UUID;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.v1.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.time.Clocks;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetricsReportingBoltConnectionTest
{

    @Test
    public void shouldNotifyConnectionOpened()
    {
        BoltConnectionMetricsMonitor metricsMonitor = mock( BoltConnectionMetricsMonitor.class );
        BoltConnection connection = newConnection( metricsMonitor );

        connection.start();

        verify( metricsMonitor ).connectionOpened();
    }

    @Test
    public void shouldNotifyConnectionClosed()
    {
        BoltConnectionMetricsMonitor metricsMonitor = mock( BoltConnectionMetricsMonitor.class );
        BoltConnection connection = newConnection( metricsMonitor );

        connection.start();
        connection.stop();
        connection.processNextBatch();

        verify( metricsMonitor ).connectionClosed();
    }

    @Test
    public void shouldNotifyConnectionClosedOnBoltConnectionAuthFatality()
    {
        verifyConnectionClosed( machine ->
        {
            throw new BoltConnectionAuthFatality( "auth failure", new RuntimeException() );
        } );
    }

    @Test
    public void shouldNotifyConnectionClosedOnBoltProtocolBreachFatality()
    {
        verifyConnectionClosed( machine ->
        {
            throw new BoltProtocolBreachFatality( "protocol failure" );
        } );
    }

    @Test
    public void shouldNotifyConnectionClosedOnUncheckedException()
    {
        verifyConnectionClosed( machine ->
        {
            throw new RuntimeException( "unexpected error" );
        } );
    }

    @Test
    public void shouldNotifyMessageReceived()
    {
        BoltConnectionMetricsMonitor metricsMonitor = mock( BoltConnectionMetricsMonitor.class );
        BoltConnection connection = newConnection( metricsMonitor );

        connection.start();
        connection.enqueue( machine ->
        {

        } );

        verify( metricsMonitor ).messageReceived();
    }

    @Test
    public void shouldNotifyMessageProcessingStartedAndCompleted()
    {
        BoltConnectionMetricsMonitor metricsMonitor = mock( BoltConnectionMetricsMonitor.class );
        BoltConnection connection = newConnection( metricsMonitor );

        connection.start();
        connection.enqueue( machine ->
        {

        } );
        connection.processNextBatch();

        verify( metricsMonitor ).messageProcessingStarted( anyLong() );
        verify( metricsMonitor ).messageProcessingCompleted( anyLong() );
    }

    @Test
    public void shouldNotifyConnectionActivatedAndDeactivated()
    {
        BoltConnectionMetricsMonitor metricsMonitor = mock( BoltConnectionMetricsMonitor.class );
        BoltConnection connection = newConnection( metricsMonitor );

        connection.start();
        connection.enqueue( machine ->
        {

        } );
        connection.processNextBatch();

        verify( metricsMonitor ).connectionActivated();
        verify( metricsMonitor ).connectionWaiting();
    }

    @Test
    public void shouldNotifyMessageProcessingFailed()
    {
        BoltConnectionMetricsMonitor metricsMonitor = mock( BoltConnectionMetricsMonitor.class );
        BoltConnection connection = newConnection( metricsMonitor );

        connection.start();
        connection.enqueue( machine ->
        {
            throw new BoltConnectionAuthFatality( "some error", new RuntimeException() );
        } );
        connection.processNextBatch();

        verify( metricsMonitor ).messageProcessingFailed();
    }

    private static void verifyConnectionClosed( Job throwingJob )
    {
        BoltConnectionMetricsMonitor metricsMonitor = mock( BoltConnectionMetricsMonitor.class );
        BoltConnection connection = newConnection( metricsMonitor );

        connection.start();
        connection.enqueue( throwingJob );
        connection.processNextBatch();

        verify( metricsMonitor ).connectionClosed();
    }

    private static BoltConnection newConnection( BoltConnectionMetricsMonitor metricsMonitor )
    {
        BoltChannel channel = mock( BoltChannel.class );
        when( channel.id() ).thenReturn( UUID.randomUUID().toString() );

        return new MetricsReportingBoltConnection( channel, mock( PackOutput.class ), mock( BoltStateMachine.class ), NullLogService.getInstance(),
                mock( BoltConnectionLifetimeListener.class ), mock( BoltConnectionQueueMonitor.class ), metricsMonitor, Clocks.systemClock() );
    }
}
