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
