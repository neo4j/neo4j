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
package org.neo4j.causalclustering.core.consensus.roles;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.TestMessageBuilders;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.preVoteRequest;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.preVoteResponse;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteRequest;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteResponse;
import static org.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static org.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@RunWith( MockitoJUnitRunner.class )
public class CandidateTest
{
    private MemberId myself = member( 0 );
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );

    private LogProvider logProvider = NullLogProvider.getInstance();

    @Test
    public void shouldBeElectedLeaderOnReceivingGrantedVoteResponseWithCurrentTerm() throws Exception
    {
        // given
        RaftState state = RaftStateBuilder.raftState()
                .term( 1 )
                .myself( myself )
                .votingMembers( member1, member2 )
                .replicationMembers( member1, member2 )
                .build();

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteResponse()
                .term( state.term() )
                .from( member1 )
                .grant()
                .build(), state, log() );

        // then
        assertEquals( LEADER, outcome.getRole() );
        assertEquals( true, outcome.electionTimeoutRenewed() );
        assertThat( outcome.getLogCommands(), hasItem( new AppendLogEntry( 0,
                new RaftLogEntry( state.term(), new NewLeaderBarrier() ) ) ) );
        assertThat( outcome.getOutgoingMessages(), hasItems(
                new RaftMessages.Directed( member1, new RaftMessages.Heartbeat( myself, state.term(), -1, -1 ) ),
                new RaftMessages.Directed( member2, new RaftMessages.Heartbeat( myself, state.term(), -1, -1 ) ) )
        );
    }

    @Test
    public void shouldStayAsCandidateOnReceivingDeniedVoteResponseWithCurrentTerm() throws Exception
    {
        // given
        RaftState state = newState();

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteResponse()
                .term( state.term() )
                .from( member1 )
                .deny()
                .build(), state, log() );

        // then
        assertEquals( CANDIDATE, outcome.getRole() );
    }

    @Test
    public void shouldUpdateTermOnReceivingVoteResponseWithLaterTerm() throws Exception
    {
        // given
        RaftState state = newState();

        final long voterTerm = state.term() + 1;

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteResponse()
                .term( voterTerm )
                .from( member1 )
                .grant()
                .build(), state, log() );

        // then
        assertEquals( FOLLOWER, outcome.getRole() );
        assertEquals( voterTerm, outcome.getTerm() );
    }

    @Test
    public void shouldRejectVoteResponseWithOldTerm() throws Exception
    {
        // given
        RaftState state = newState();

        final long voterTerm = state.term() - 1;

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteResponse()
                .term( voterTerm )
                .from( member1 )
                .grant()
                .build(), state, log() );

        // then
        assertEquals( CANDIDATE, outcome.getRole() );
    }

    @Test
    public void shouldDeclineVoteRequestsIfFromSameTerm() throws Throwable
    {
        // given
        RaftState raftState = newState();

        // when
        Outcome outcome = CANDIDATE.handler.handle( voteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( raftState.term() )
                .build(), raftState, log() );

        // then
        assertThat(
                outcome.getOutgoingMessages(),
                hasItem( new RaftMessages.Directed( member1, voteResponse().term( raftState.term() ).from( myself ).deny().build() ) )
        );
        assertEquals( Role.CANDIDATE, outcome.getRole() );
    }

    @Test
    public void shouldBecomeFollowerIfReceiveVoteRequestFromLaterTerm() throws Throwable
    {
        // given
        RaftState raftState = newState();

        // when
        long newTerm = raftState.term() + 1;
        Outcome outcome = CANDIDATE.handler.handle( voteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( newTerm )
                .build(), raftState, log() );

        // then
        assertEquals( newTerm ,outcome.getTerm() );
        assertEquals( Role.FOLLOWER, outcome.getRole() );
        assertThat( outcome.getVotesForMe(), empty() );

        assertThat(
                outcome.getOutgoingMessages(),
                hasItem( new RaftMessages.Directed( member1, voteResponse().term( newTerm ).from( myself ).grant().build() ) )
        );
    }

    @Test
    public void shouldDeclinePreVoteFromSameTerm() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .build();

        // when
        Outcome outcome = CANDIDATE.handler.handle( preVoteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( raftState.term() )
                .build(), raftState, log() );

        // then
        assertThat(
                outcome.getOutgoingMessages(),
                hasItem( new RaftMessages.Directed( member1, preVoteResponse().term( raftState.term() ).from( myself ).deny().build() ) )
        );
        assertEquals( Role.CANDIDATE, outcome.getRole() );
    }

    @Test
    public void shouldBecomeFollowerIfReceivePreVoteRequestFromLaterTerm() throws Throwable
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .build();
        long newTerm = raftState.term() + 1;

        // when
        Outcome outcome = CANDIDATE.handler.handle( preVoteRequest()
                .candidate( member1 )
                .from( member1 )
                .term( newTerm )
                .build(), raftState, log() );

        // then
        assertEquals( newTerm ,outcome.getTerm() );
        assertEquals( Role.FOLLOWER, outcome.getRole() );
        assertThat( outcome.getVotesForMe(), empty() );

        assertThat(
                outcome.getOutgoingMessages(),
                hasItem( new RaftMessages.Directed( member1, preVoteResponse().term( newTerm ).from( myself ).deny().build() ) )
        );
    }

    public RaftState newState() throws IOException
    {
        return raftState().myself( myself ).build();
    }

    private Log log()
    {
        return logProvider.getLog( getClass() );
    }

}
