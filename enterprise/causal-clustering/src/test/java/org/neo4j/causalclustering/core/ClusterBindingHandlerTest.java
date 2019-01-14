/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
