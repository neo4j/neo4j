/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.verify;

public class ClusterBindingHandlerTest
{
    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );

    private RaftMessages.ReceivedInstantClusterIdAwareMessage<?> heartbeat =
            RaftMessages.ReceivedInstantClusterIdAwareMessage.of( Instant.now(), clusterId,
                    new RaftMessages.Heartbeat( new MemberId( UUID.randomUUID() ), 0L, 0, 0 ) );

    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> delegate = Mockito.mock( LifecycleMessageHandler.class );

    private ClusterBindingHandler handler = new ClusterBindingHandler( delegate , NullLogProvider.getInstance() );

    @Test
    public void shouldDropMessagesIfHasNotBeenStarted()
    {
        // when
        handler.handle( heartbeat );

        // then
        verify( delegate, Mockito.never() ).handle( heartbeat );
    }

    @Test
    public void shouldDropMessagesIfHasBeenStopped() throws Throwable
    {
        // given
        handler.start( clusterId );
        handler.stop();

        // when
        handler.handle( heartbeat );

        // then
        verify( delegate, Mockito.never() ).handle( heartbeat );
    }

    @Test
    public void shouldDropMessagesIfForDifferentClusterId() throws Throwable
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( RaftMessages.ReceivedInstantClusterIdAwareMessage.of(
                Instant.now(), new ClusterId( UUID.randomUUID() ),
                new RaftMessages.Heartbeat( new MemberId( UUID.randomUUID() ), 0L, 0, 0 )
        ) );

        // then
        verify( delegate, Mockito.never() ).handle( ArgumentMatchers.any( RaftMessages.ReceivedInstantClusterIdAwareMessage.class ) );
    }

    @Test
    public void shouldDelegateMessages() throws Throwable
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( heartbeat );

        // then
        verify( delegate ).handle( heartbeat );
    }

    @Test
    public void shouldDelegateStartCalls() throws Throwable
    {
        // when
        handler.start( clusterId );

        // then
        verify( delegate ).start( clusterId );
    }

    @Test
    public void shouldDelegateStopCalls() throws Throwable
    {
        // when
        handler.stop();

        // then
        verify( delegate ).stop();
    }
}
