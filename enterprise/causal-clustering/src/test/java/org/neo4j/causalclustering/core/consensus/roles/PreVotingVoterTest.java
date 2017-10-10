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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.core.consensus.MessageUtils.messageFor;
import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class PreVotingVoterTest
{
    private MemberId myself = member( 0 );

    /* A few members that we use at will in tests. */
    private MemberId member1 = member( 1 );
    private MemberId member2 = member( 2 );

    @Test
    public void shouldRespondPositivelyIfWouldVoteForCandidate() throws Exception
    {
        // given
        RaftState raftState = initialState();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( true )  );
    }

    @Test
    public void shouldRespondPositivelyEvenIfAlreadyVotedInRealElection() throws Exception
    {
        // given
        RaftState raftState = initialState();
        raftState.update( new Follower().handle( new RaftMessages.Vote.Request( member1, 0, member1, 0, 0 ), raftState, log() ) );

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member2, 0, member2, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member2 );
        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( true )  );
    }

    @Test
    public void shouldRespondNegativelyIfLeaderAndRequestNotFromGreaterTerm() throws Exception
    {
        // given
        RaftState raftState = initialState();

        // when
        Outcome outcome = new Leader().handle( new RaftMessages.PreVote.Request( member1, Long.MIN_VALUE, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( false )  );
    }

    @Test
    public void shouldRespondPositivelyIfLeaderAndRequestFromGreaterTerm() throws Exception
    {
        // given
        RaftState raftState = initialState();

        // when
        Outcome outcome = new Leader().handle( new RaftMessages.PreVote.Request( member1, Long.MAX_VALUE, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( true )  );
    }

    @Test
    public void shouldRespondNegativelyIfNotInPreVoteMyself() throws Exception
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .setPreElection( false )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( false )  );
    }

    @Test
    public void shouldRespondNegativelyIfWouldNotVoteForCandidate() throws Exception
    {
        // given
        RaftState raftState = raftState()
                .myself( myself )
                .term( 1 )
                .setPreElection( true )
                .supportsPreVoting( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );
        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( false )  );
    }

    @Test
    public void shouldRespondPositivelyToMultipleMembersIfWouldVoteForAny() throws Exception
    {
        // given
        RaftState raftState = initialState();

        // when
        Outcome outcome1 = new Follower().handle( new RaftMessages.PreVote.Request( member1, 0, member1, 0, 0 ), raftState, log() );
        raftState.update( outcome1 );
        Outcome outcome2 = new Follower().handle( new RaftMessages.PreVote.Request( member2, 0, member2, 0, 0 ), raftState, log() );
        raftState.update( outcome2 );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome2, member2 );

        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).voteGranted() , equalTo( true )  );
    }

    @Test
    public void shouldUseTermFromRequestIfHigherThanOwn() throws Exception
    {
        // given
        RaftState raftState = initialState();
        long newTerm = 1;

        // when
        Outcome outcome = new Follower().handle( new RaftMessages.PreVote.Request( member1, newTerm, member1, 0, 0 ), raftState, log() );

        // then
        RaftMessages.RaftMessage raftMessage = messageFor( outcome, member1 );

        assertThat( raftMessage.type(), equalTo( RaftMessages.Type.PRE_VOTE_RESPONSE ) );
        assertThat( ( (RaftMessages.PreVote.Response)raftMessage ).term() , equalTo( newTerm )  );
    }

    private RaftState initialState() throws IOException
    {
        return raftState()
                .myself( myself )
                .supportsPreVoting( true )
                .setPreElection( true )
                .votingMembers( asSet( myself, member1, member2 ) )
                .build();
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }
}
