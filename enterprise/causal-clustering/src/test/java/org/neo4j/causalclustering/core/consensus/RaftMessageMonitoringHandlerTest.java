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
    private LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> downstream = mock( LifecycleMessageHandler.class );

    private Duration messageQueueDelay = Duration.ofMillis( 5 );
    private Duration messageProcessingDelay = Duration.ofMillis( 7 );
    private RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of(
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
    public void shouldSendMessagesToDelegate()
    {
        // when
        handler.handle( message );

        // then
        verify( downstream ).handle( message );
    }

    @Test
    public void shouldUpdateDelayMonitor()
    {
        // when
        handler.handle( message );

        // then
        verify( monitor ).setDelay( messageQueueDelay );
    }

    @Test
    public void shouldTimeDelegate()
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
