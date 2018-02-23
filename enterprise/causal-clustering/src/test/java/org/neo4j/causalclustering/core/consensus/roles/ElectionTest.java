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
package org.neo4j.causalclustering.core.consensus.roles;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.RaftMachineBuilder;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.consensus.schedule.OnDemandTimerService;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.test.extension.MockitoExtension;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteRequest;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteResponse;
import static org.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static org.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

@ExtendWith( MockitoExtension.class )
public class ElectionTest
{
    private MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );

    @Mock
    private Inbound inbound;
    @Mock
    private Outbound<MemberId, RaftMessages.RaftMessage> outbound;

    @Test
    public void candidateShouldWinElectionAndBecomeLeader() throws Exception
    {
        // given
        FakeClock fakeClock = Clocks.fakeClock();
        TimerService timeouts = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .outbound( outbound )
                .timerService( timeouts )
                .clock( fakeClock )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );
        raft.postRecoveryActions();

        timeouts.invoke( RaftMachine.Timeouts.ELECTION );

        // when
        raft.handle( voteResponse().from( member1 ).term( 1 ).grant().build() );
        raft.handle( voteResponse().from( member2 ).term( 1 ).grant().build() );

        // then
        assertEquals( 1, raft.term() );
        assertEquals( LEADER, raft.currentRole() );

        /*
         * We require atLeast here because RaftMachine has its own scheduled service, which can spuriously wake up and
         * send empty entries. These are fine and have no bearing on the correctness of this test, but can cause it
         * fail if we expect exactly 2 of these messages
         */
        verify( outbound, atLeast( 1 ) ).send( eq( member1 ), isA( RaftMessages.AppendEntries.Request.class ) );
        verify( outbound, atLeast( 1 ) ).send( eq( member2 ), isA( RaftMessages.AppendEntries.Request.class ) );
    }

    @Test
    public void candidateShouldLoseElectionAndRemainCandidate() throws Exception
    {
        // Note the etcd implementation seems to diverge from the paper here, since the paper suggests that it should
        // remain as a candidate

        // given
        FakeClock fakeClock = Clocks.fakeClock();
        TimerService timeouts = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .outbound( outbound )
                .timerService( timeouts )
                .clock( fakeClock )
                .build();

        raft.installCoreState(
                new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 ) ) ));
        raft.postRecoveryActions();

        timeouts.invoke( RaftMachine.Timeouts.ELECTION );

        // when
        raft.handle( voteResponse().from( member1 ).term( 1 ).deny().build() );
        raft.handle( voteResponse().from( member2 ).term( 1 ).deny().build() );

        // then
        assertEquals( 1, raft.term() );
        assertEquals( CANDIDATE, raft.currentRole() );

        verify( outbound, never() ).send( eq( member1 ), isA( RaftMessages.AppendEntries.Request.class ) );
        verify( outbound, never() ).send( eq( member2 ), isA( RaftMessages.AppendEntries.Request.class ) );
    }

    @Test
    public void candidateShouldVoteForTheSameCandidateInTheSameTerm() throws Exception
    {
        // given
        FakeClock fakeClock = Clocks.fakeClock();
        TimerService timeouts = new OnDemandTimerService( fakeClock );
        RaftMachine raft = new RaftMachineBuilder( myself, 3, RaftTestMemberSetBuilder.INSTANCE )
                .outbound( outbound )
                .timerService( timeouts )
                .clock( fakeClock )
                .build();

        raft.installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( myself, member1, member2 )  ) ) );

        // when
        raft.handle( voteRequest().from( member1 ).candidate( member1 ).term( 1 ).build() );
        raft.handle( voteRequest().from( member1 ).candidate( member1 ).term( 1 ).build() );

        // then
        verify( outbound, times( 2 ) ).send( member1, voteResponse().term( 1 ).grant().build() );
    }
}
