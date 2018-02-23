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

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.MockitoExtension;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.preVoteRequest;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.preVoteResponse;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteRequest;
import static org.neo4j.causalclustering.core.consensus.TestMessageBuilders.voteResponse;
import static org.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static org.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@ExtendWith( MockitoExtension.class )
class CandidateTest
{
    private MemberId myself = member( 0 );
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );

    private LogProvider logProvider = NullLogProvider.getInstance();

    @Test
    void shouldBeElectedLeaderOnReceivingGrantedVoteResponseWithCurrentTerm() throws Exception
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
    void shouldStayAsCandidateOnReceivingDeniedVoteResponseWithCurrentTerm() throws Exception
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
    void shouldUpdateTermOnReceivingVoteResponseWithLaterTerm() throws Exception
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
    void shouldRejectVoteResponseWithOldTerm() throws Exception
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
    void shouldDeclineVoteRequestsIfFromSameTerm() throws Throwable
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
    void shouldBecomeFollowerIfReceiveVoteRequestFromLaterTerm() throws Throwable
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
    void shouldDeclinePreVoteFromSameTerm() throws Throwable
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
    void shouldBecomeFollowerIfReceivePreVoteRequestFromLaterTerm() throws Throwable
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

    private RaftState newState() throws IOException
    {
        return raftState().myself( myself ).build();
    }

    private Log log()
    {
        return logProvider.getLog( getClass() );
    }

}
