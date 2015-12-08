/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.roles;

import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.coreedge.raft.NewLeaderBarrier;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.outcome.AppendLogEntry;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.RaftState;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import static org.neo4j.coreedge.raft.MessageUtils.messageFor;
import static org.neo4j.coreedge.raft.TestMessageBuilders.voteResponse;
import static org.neo4j.coreedge.raft.roles.Role.LEADER;
import static org.neo4j.coreedge.server.RaftTestMember.member;
import static org.neo4j.coreedge.raft.TestMessageBuilders.appendEntriesRequest;
import static org.neo4j.coreedge.raft.TestMessageBuilders.voteRequest;
import static org.neo4j.coreedge.raft.roles.Role.CANDIDATE;
import static org.neo4j.coreedge.raft.roles.Role.FOLLOWER;
import static org.neo4j.coreedge.raft.state.RaftStateBuilder.raftState;

@RunWith(MockitoJUnitRunner.class)
public class CandidateTest
{
    private RaftTestMember myself = member( 0 );

    /* A few members that we use at will in tests. */
    private RaftTestMember member1 = member( 1 );

    @Mock
    private Inbound inbound;

    private LogProvider logProvider = NullLogProvider.getInstance();
    public static final int HIGHEST_TERM = 99;

    @Test
    public void shouldBeElectedLeader() throws Exception
    {
        // given
        long term = 0;
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( term )
                .build();

        Candidate candidate = new Candidate();

        // when
        Outcome<RaftTestMember> outcome = candidate.handle( voteResponse()
                .term( term )
                .from( member1 )
                .grant()
                .build(), state, log() );

        // then
        assertEquals( LEADER, outcome.getNewRole() );
        assertThat( outcome.getLogCommands(), hasItem( new AppendLogEntry( 0, new RaftLogEntry( term, new NewLeaderBarrier() ) )) );
    }

    @Test
    public void candidateShouldUpdateTermToCurrentMessageAndBecomeFollower() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState().build();

        Candidate candidate = new Candidate();

        // when
        Outcome outcome = candidate.handle( voteRequest().from( member1 ).term( HIGHEST_TERM ).lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertEquals( FOLLOWER, outcome.getNewRole() );
        assertEquals( HIGHEST_TERM, outcome.getTerm() );
    }

    @Test
    public void candidateShouldBecomeFollowerIfReceivesMessageWithNewerTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .build();

        Candidate candidate = new Candidate();

        // when
        RaftMessages.Vote.Request<RaftTestMember> message = voteRequest()
                .from( member1 )
                .term( state.term() + 1 )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build();
        Outcome<RaftTestMember> outcome = candidate.handle( message, state, log() );

        // then
        assertEquals( message, messageFor( outcome,  myself ) );
        assertEquals( FOLLOWER, outcome.getNewRole() );
    }

    @Test
    public void candidateShouldRejectAnyMessageWithOldTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState().build();

        Candidate candidate = new Candidate();

        // when
        Outcome<RaftTestMember> outcome = candidate.handle( voteRequest().from( member1 ).term( state.term() - 1 ).lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertFalse( ((RaftMessages.Vote.Response) messageFor( outcome, member1 )).voteGranted() );
        assertEquals( CANDIDATE, outcome.getNewRole() );
    }

    @Test
    public void candidateShouldBecomeFollowerOnReceiptOfAppendEntriesFromLeaderWithHigherTerm() throws Exception
    {
        candidateShouldBecomeFollowerOnReceiptOfAppendEntriesFromLeaderWithTerm( 2 );
    }

    @Test
    public void candidateShouldBecomeFollowerOnReceiptOfAppendEntriesFromLeaderWithSameTerm() throws Exception
    {
        candidateShouldBecomeFollowerOnReceiptOfAppendEntriesFromLeaderWithTerm( 1 );
    }

    private void candidateShouldBecomeFollowerOnReceiptOfAppendEntriesFromLeaderWithTerm( int term ) throws Exception
    {
        // given
        RaftState<RaftTestMember> state = raftState()
                .myself( myself )
                .term( 1 )
                .build();

        Candidate candidate = new Candidate();

        // when
        RaftMessages.AppendEntries.Request<RaftTestMember> message = appendEntriesRequest()
                .from( member1 )
                .leader( member1 )
                .leaderTerm( term )
                .leaderCommit( 1 )
                .correlationId( UUID.randomUUID() )
                .prevLogIndex( 0 )
                .prevLogTerm( term - 1 ).build();
        Outcome<RaftTestMember> outcome = candidate.handle( message, state, log() );

        // then
        assertEquals( message, messageFor( outcome,  myself ) );
        assertEquals( FOLLOWER, outcome.getNewRole() );
    }

    private Log log()
    {
        return logProvider.getLog( getClass() );
    }

}
