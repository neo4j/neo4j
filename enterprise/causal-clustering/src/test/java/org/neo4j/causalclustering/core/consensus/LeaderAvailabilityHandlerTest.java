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
package org.neo4j.causalclustering.core.consensus;

import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.UUID;
import java.util.function.LongSupplier;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import static org.mockito.Mockito.verify;

public class LeaderAvailabilityHandlerTest
{
    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> delegate = Mockito.mock( LifecycleMessageHandler.class );
    private LeaderAvailabilityTimers leaderAvailabilityTimers = Mockito.mock( LeaderAvailabilityTimers.class );
    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );
    private LongSupplier term = () -> 3;

    private LeaderAvailabilityHandler handler = new LeaderAvailabilityHandler( delegate, leaderAvailabilityTimers, term );

    private MemberId leader = new MemberId( UUID.randomUUID() );
    private RaftMessages.ReceivedInstantClusterIdAwareMessage<?> heartbeat =
            RaftMessages.ReceivedInstantClusterIdAwareMessage.of( Instant.now(), clusterId, new RaftMessages.Heartbeat( leader, term.getAsLong(), 0, 0 ) );
    private RaftMessages.ReceivedInstantClusterIdAwareMessage<?> appendEntries =
            RaftMessages.ReceivedInstantClusterIdAwareMessage.of( Instant.now(), clusterId,
                    new RaftMessages.AppendEntries.Request( leader, term.getAsLong(), 0, 0, RaftLogEntry.empty, 0 )
            );
    private RaftMessages.ReceivedInstantClusterIdAwareMessage<?> voteResponse =
            RaftMessages.ReceivedInstantClusterIdAwareMessage.of( Instant.now(), clusterId, new RaftMessages.Vote.Response( leader, term.getAsLong(), false ) );

    @Test
    public void shouldRenewElectionForHeartbeats() throws Throwable
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( heartbeat );

        // then
        verify( leaderAvailabilityTimers ).renewElection();
    }

    @Test
    public void shouldRenewElectionForAppendEntriesRequests() throws Throwable
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( appendEntries );

        // then
        verify( leaderAvailabilityTimers ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionForOtherMessages() throws Throwable
    {
        // given
        handler.start( clusterId );

        // when
        handler.handle( voteResponse );

        // then
        verify( leaderAvailabilityTimers, Mockito.never() ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionTimeoutsForHeartbeatsFromEarlierTerm() throws Throwable
    {
        // given
        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> heartbeat =  RaftMessages.ReceivedInstantClusterIdAwareMessage.of(
                Instant.now(), clusterId, new RaftMessages.Heartbeat( leader, term.getAsLong() - 1, 0, 0 ) );

        handler.start( clusterId );

        // when
        handler.handle( heartbeat );

        // then
        verify( leaderAvailabilityTimers, Mockito.never() ).renewElection();
    }

    @Test
    public void shouldNotRenewElectionTimeoutsForAppendEntriesRequestsFromEarlierTerms() throws Throwable
    {
        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> appendEntries = RaftMessages.ReceivedInstantClusterIdAwareMessage.of(
                Instant.now(), clusterId,
                new RaftMessages.AppendEntries.Request(
                        leader, term.getAsLong() - 1, 0, 0, RaftLogEntry.empty, 0 )
        );

        handler.start( clusterId );

        // when
        handler.handle( appendEntries );

        // then
        verify( leaderAvailabilityTimers, Mockito.never() ).renewElection();
    }

    @Test
    public void shouldDelegateStart() throws Throwable
    {
        // when
        handler.start( clusterId );

        // then
        verify( delegate ).start( clusterId );
    }

    @Test
    public void shouldDelegateStop() throws Throwable
    {
        // when
        handler.stop();

        // then
        verify( delegate ).stop();
    }
}
