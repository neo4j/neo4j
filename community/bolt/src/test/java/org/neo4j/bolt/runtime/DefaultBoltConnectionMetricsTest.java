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

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.scheduling.BoltConnectionLifetimeListener;
import org.neo4j.bolt.runtime.scheduling.BoltConnectionQueueMonitor;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.packstream.PackOutput;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.time.Clocks;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.runtime.DefaultBoltConnection.DEFAULT_MAX_BATCH_SIZE;

class DefaultBoltConnectionMetricsTest
{

    @Test
    void notifyConnectionOpened()
    {
        BoltConnectionMetricsMonitor metricsMonitor = mock( BoltConnectionMetricsMonitor.class );
        BoltConnection connection = newConnection( metricsMonitor );

        connection.start();

        verify( metricsMonitor ).connectionOpened();
    }

    @Test
    void notifyConnectionClosed()
    {
        BoltConnectionMetricsMonitor metricsMonitor = mock( BoltConnectionMetricsMonitor.class );
        BoltConnection connection = newConnection( metricsMonitor );

        connection.start();
        connection.stop();
        connection.processNextBatch();

        verify( metricsMonitor ).connectionClosed();
    }

    @Test
    void notifyConnectionClosedOnBoltConnectionAuthFatality()
    {
        verifyConnectionClosed( machine ->
        {
            throw new BoltConnectionAuthFatality( "auth failure", new RuntimeException() );
        } );
    }

    @Test
    void notifyConnectionClosedOnBoltProtocolBreachFatality()
    {
        verifyConnectionClosed( machine ->
        {
            throw new BoltProtocolBreachFatality( "protocol failure" );
        } );
    }

    @Test
    void notifyConnectionClosedOnUncheckedException()
    {
        verifyConnectionClosed( machine ->
        {
            throw new RuntimeException( "unexpected error" );
        } );
    }

    @Test
    void notifyMessageReceived()
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
    void notifyMessageProcessingStartedAndCompleted()
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
    void notifyConnectionActivatedAndDeactivated()
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
    void notifyMessageProcessingFailed()
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
        BoltChannel channel = BoltTestUtil.newTestBoltChannel();
        return new DefaultBoltConnection( channel, mock( PackOutput.class ), mock( BoltStateMachine.class ), NullLogService.getInstance(),
                mock( BoltConnectionLifetimeListener.class ), mock( BoltConnectionQueueMonitor.class ), DEFAULT_MAX_BATCH_SIZE, metricsMonitor,
                Clocks.systemClock() );
    }
}
