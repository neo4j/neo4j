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
package org.neo4j.causalclustering.core.consensus.roles;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static org.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class PreVotingInitiatorTest
{
    private MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );
    private MemberId member3 = member( 3 );
    private MemberId member4 = member( 4 );

    @Test
    public void shouldSetPreElectionOnElectionTimeout() throws Exception
    {
        // given
        RaftState state = initialState();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome );

        // then
        assertThat( outcome.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome.isPreElection(), equalTo( true ) );
    }

    @Test
    public void shouldSendPreVoteRequestsOnElectionTimeout() throws Exception
    {
        // given
        RaftState state = initialState();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome );

        // then
        assertThat( messageFor( outcome, member1 ).type(), equalTo( RaftMessages.Type.PRE_VOTE_REQUEST ) );
        assertThat( messageFor( outcome, member2 ).type(), equalTo( RaftMessages.Type.PRE_VOTE_REQUEST ) );
    }

    @Test
    public void shouldProceedToRealElectionIfReceiveQuorumOfPositiveResponses() throws Exception
    {
        // given
        RaftState state = initialState();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome2.getRole(), equalTo( CANDIDATE ) );
        assertThat( outcome2.isPreElection(), equalTo( false ) );
        assertThat( outcome2.getPreVotesForMe(), contains( member1 ) );
    }

    @Test
    public void shouldIgnorePositiveResponsesFromOlderTerm() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .term( 1 )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome2.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome2.isPreElection(), equalTo( true ) );
        assertThat( outcome2.getPreVotesForMe(), empty() );
    }

    @Test
    public void shouldIgnorePositiveResponsesIfNotInPreVotingStage() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        Follower underTest = new Follower();

        // when
        Outcome outcome = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome.isPreElection(), equalTo( false ) );
        assertThat( outcome.getPreVotesForMe(), empty() );
    }

    @Test
    public void shouldNotMoveToRealElectionWithoutQuorum() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2, member3, member4 ) )
                .build();

        Follower underTest = new Follower();
        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome2.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome2.isPreElection(), equalTo( true ) );
        assertThat( outcome2.getPreVotesForMe(), contains( member1 ) );
    }

    @Test
    public void shouldMoveToRealElectionWithQuorumOf5() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2, member3, member4 ) )
                .build();

        Follower underTest = new Follower();
        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member2, 0L, true ), state, log() );

        // then
        assertThat( outcome3.getRole(), equalTo( CANDIDATE ) );
        assertThat( outcome3.isPreElection(), equalTo( false ) );
    }

    @Test
    public void shouldNotCountVotesFromSameMemberTwice() throws Exception
    {
        // given
        RaftState state = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2, member3, member4 ) )
                .build();

        Follower underTest = new Follower();
        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( outcome3.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome3.isPreElection(), equalTo( true ) );
    }

    @Test
    public void shouldResetPreVotesWhenMovingBackToFollower() throws Exception
    {
        // given
        RaftState state = initialState();

        Outcome outcome1 = new Follower().handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );
        Outcome outcome2 = new Follower().handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );
        assertThat( CANDIDATE, equalTo( outcome2.getRole() ) );
        assertThat( outcome2.getPreVotesForMe(), contains( member1 ) );

        // when
        Outcome outcome3 = new Candidate().handle( new RaftMessages.Timeout.Election( myself ), state, log() );

        // then
        assertThat( outcome3.getPreVotesForMe(), empty() );
    }

    @Test
    public void shouldSendRealVoteRequestsIfReceivePositiveResponses() throws Exception
    {
        // given
        RaftState state = initialState();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, true ), state, log() );

        // then
        assertThat( messageFor( outcome2, member1 ).type(), equalTo( RaftMessages.Type.VOTE_REQUEST ) );
        assertThat( messageFor( outcome2, member2 ).type(), equalTo( RaftMessages.Type.VOTE_REQUEST ) );
    }

    @Test
    public void shouldNotProceedToRealElectionIfReceiveNegativeResponses() throws Exception
    {
                // given
        RaftState state = initialState();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, false ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member2, 0L, false ), state, log() );

        // then
        assertThat( outcome3.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome3.isPreElection(), equalTo( true ) );
        assertThat( outcome3.getPreVotesForMe(), empty() );
    }

    @Test
    public void shouldNotSendRealVoteRequestsIfReceiveNegativeResponses() throws Exception
    {
        // given
        RaftState state = initialState();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.PreVote.Response( member1, 0L, false ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member2, 0L, false ), state, log() );

        // then
        assertThat( outcome2.getOutgoingMessages(), empty() );
        assertThat( outcome3.getOutgoingMessages(), empty() );
    }

    @Test
    public void shouldResetPreVoteIfReceiveHeartbeatFromLeader() throws Exception
    {
        // given
        RaftState state = initialState();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.Heartbeat( member1, 0L, 0L, 0L ), state, log() );

        // then
        assertThat( outcome2.getRole(), equalTo( FOLLOWER ) );
        assertThat( outcome2.isPreElection(), equalTo( false ) );
        assertThat( outcome2.getPreVotesForMe(), empty() );
    }

    @Test
    public void shouldNotSendPreVoteRequestsIfReceiveHeartbeatFromLeader() throws Exception
    {
        // given
        RaftState state = initialState();

        Follower underTest = new Follower();

        Outcome outcome1 = underTest.handle( new RaftMessages.Timeout.Election( myself ), state, log() );
        state.update( outcome1 );

        // when
        Outcome outcome2 = underTest.handle( new RaftMessages.Heartbeat( member1, 0L, 0L, 0L ), state, log() );
        state.update( outcome2 );
        Outcome outcome3 = underTest.handle( new RaftMessages.PreVote.Response( member2, 0L, true ), state, log() );

        // then
        assertThat( outcome3.isPreElection(), equalTo( false ) );
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }

    private RaftState initialState() throws IOException
    {
        return raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();
    }
}
