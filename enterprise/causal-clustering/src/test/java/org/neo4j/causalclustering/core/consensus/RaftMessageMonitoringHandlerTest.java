/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.time.Clocks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RaftMessageMonitoringHandlerTest
{
    private Instant now = Instant.now();
    private Monitors monitors = new Monitors();
    private RaftMessageProcessingMonitor monitor = mock( RaftMessageProcessingMonitor.class );
    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> downstream = mock( LifecycleMessageHandler.class );

    private Duration messageQueueDelay = Duration.ofMillis( 5 );
    private Duration messageProcessingDelay = Duration.ofMillis( 7 );
    private RaftMessages.ReceivedInstantClusterIdAwareMessage message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of(
            now.minus( messageQueueDelay ), new ClusterId( UUID.randomUUID() ), new RaftMessages.Heartbeat( new MemberId( UUID.randomUUID() ), 0, 0, 0 )
    );
    private Clock clock = Clocks.tickOnAccessClock( now, messageProcessingDelay );

    private RaftMessageMonitoringHandler handler = new RaftMessageMonitoringHandler( downstream, clock, monitors );

    @Before
    public void setUp()
    {
        monitors.addMonitorListener( monitor );
    }

    @Test
    public void shouldSendMessagesToDelegate() throws Exception
    {
        // when
        handler.handle( message );

        // then
        verify( downstream ).handle( message );
    }

    @Test
    public void shouldUpdateDelayMonitor() throws Exception
    {
        // when
        handler.handle( message );

        // then
        verify( monitor ).setDelay( messageQueueDelay );
    }

    @Test
    public void shouldTimeDelegate() throws Exception
    {
        // when
        handler.handle( message );

        // then
        verify( monitor ).updateTimer( RaftMessages.Type.HEARTBEAT, messageProcessingDelay );
    }

    @Test
    public void shouldDelegateStart() throws Throwable
    {
        // given
        ClusterId clusterId = new ClusterId( UUID.randomUUID() );

        // when
        handler.start( clusterId );

        // then
        Mockito.verify( downstream ).start( clusterId );
    }

    @Test
    public void shouldDelegateStop() throws Throwable
    {
        // when
        handler.stop();

        // then
        Mockito.verify( downstream ).stop();
    }
}
