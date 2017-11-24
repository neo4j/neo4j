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

import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.function.LongSupplier;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.logging.NullLogProvider;

public class LeaderAvailabilityHandlerTest
{
    @SuppressWarnings( "unchecked" )
    private Inbound.MessageHandler<RaftMessages.ClusterIdAwareMessage> delegate = Mockito.mock( Inbound.MessageHandler.class );
    private LeaderAvailabilityTimers leaderAvailabilityTimers = Mockito.mock( LeaderAvailabilityTimers.class );
    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );
    private LongSupplier term = () -> 3;

    private LeaderAvailabilityHandler handler = new LeaderAvailabilityHandler( delegate, leaderAvailabilityTimers, term, NullLogProvider.getInstance() );

    private MemberId leader = new MemberId( UUID.randomUUID() );
    private RaftMessages.ClusterIdAwareMessage heartbeat =
            new RaftMessages.ClusterIdAwareMessage( clusterId, new RaftMessages.Heartbeat( leader, term.getAsLong(), 0, 0 ) );
    private RaftMessages.ClusterIdAwareMessage appendEntries =
            new RaftMessages.ClusterIdAwareMessage( clusterId,
                    new RaftMessages.AppendEntries.Request( leader, term.getAsLong(), 0, 0, RaftLogEntry.empty, 0 )
            );
    private RaftMessages.ClusterIdAwareMessage voteResponse =
            new RaftMessages.ClusterIdAwareMessage( clusterId, new RaftMessages.Vote.Response( leader, term.getAsLong(), false ) );

    @Test
    public void shouldDropMessagesIfHasNotBeenStarted() throws Exception
    {
        // when
        handler.handle( heartbeat );

        // then
        Mockito.verify( delegate, Mockito.never() ).handle( heartbeat );
    }

    @Test
    public void shouldDropMessagesIfHasBeenStopped() throws Exception
    {
        // given
        handler.start( clusterId );
        handler.stop();

        // when
        handler.handle( heartbeat );

        // then
        Mockito.verify( delegate, Mockito.never() ).handle( heartbeat );
    }

    @Test
    public void shouldDropMessagesIfForDifferentClusterId() throws Exception
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( new RaftMessages.ClusterIdAwareMessage(
                new ClusterId( UUID.randomUUID() ), new RaftMessages.Heartbeat( leader, term.getAsLong(), 0, 0 )
        ) );

        // then
        Mockito.verify( delegate, Mockito.never() ).handle( heartbeat );
    }

    @Test
    public void shouldDelegateMessages() throws Exception
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( heartbeat );

        // then
        Mockito.verify( delegate ).handle( heartbeat );
    }

    @Test
    public void shouldRenewElectionForHeartbeats() throws Exception
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( heartbeat );

        // then
        Mockito.verify( leaderAvailabilityTimers ).renewElection();
    }

    @Test
    public void shouldRenewElectionForAppendEntriesRequests() throws Exception
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( appendEntries );

        // then
        Mockito.verify( leaderAvailabilityTimers ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionForOtherMessages() throws Exception
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( voteResponse );

        // then
        Mockito.verify( leaderAvailabilityTimers, Mockito.never() ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionTimeoutsForHeartbeatsFromEarlierTerm() throws Exception
    {
        // given
        RaftMessages.ClusterIdAwareMessage heartbeat =
                new RaftMessages.ClusterIdAwareMessage( clusterId, new RaftMessages.Heartbeat( leader, term.getAsLong() - 1, 0, 0 ) );

        handler.start( clusterId );

        // when
        handler.handle( heartbeat );

        // then
        Mockito.verify( leaderAvailabilityTimers, Mockito.never() ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionTimeoutsForAppendEntriesRequestsFromEarlierTerms() throws Exception
    {
        RaftMessages.ClusterIdAwareMessage appendEntries =
                new RaftMessages.ClusterIdAwareMessage( clusterId,
                        new RaftMessages.AppendEntries.Request( leader, term.getAsLong() - 1, 0, 0, RaftLogEntry.empty, 0 )
                );

        handler.start( clusterId );

        // when
        handler.handle( appendEntries );

        // then
        Mockito.verify( leaderAvailabilityTimers, Mockito.never() ).renewElection();
    }
}
