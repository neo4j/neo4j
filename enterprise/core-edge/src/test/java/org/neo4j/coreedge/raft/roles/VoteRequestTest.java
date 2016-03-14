/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.RaftState;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.coreedge.raft.MessageUtils.messageFor;
import static org.neo4j.coreedge.raft.TestMessageBuilders.voteRequest;
import static org.neo4j.coreedge.raft.state.RaftStateBuilder.raftState;
import static org.neo4j.coreedge.server.RaftTestMember.member;

/**
 * Most behaviour for handling vote requests is identical for all roles.
 */
@RunWith(Parameterized.class)
public class VoteRequestTest
{
    @Parameterized.Parameters(name = "{0}")
    public static Collection data() {
        return asList( Role.values() );
    }

    @Parameterized.Parameter
    public Role role;

    private RaftTestMember myself = member( 0 );
    private RaftTestMember member1 = member( 1 );
    private RaftTestMember member2 = member( 2 );

    @Test
    public void shouldVoteForCandidateInLaterTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome<RaftTestMember> outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertTrue( ((RaftMessages.Vote.Response) messageFor( outcome, member1 )).voteGranted() );
    }

    @Test
    public void shouldDenyForCandidateInPreviousTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = newState();

        // when
        final long candidateTerm = state.term() - 1;

        Outcome<RaftTestMember> outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertFalse( ((RaftMessages.Vote.Response) messageFor( outcome, member1 )).voteGranted() );
        assertEquals( role, outcome.getRole() );
    }

    @Test
    public void shouldVoteForOnlyOneCandidatePerTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome<RaftTestMember> outcome1 = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        state.update( outcome1 );

        Outcome<RaftTestMember> outcome2 = role.handler.handle( voteRequest().from( member2 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertFalse( ((RaftMessages.Vote.Response) messageFor( outcome2, member2 )).voteGranted() );
    }

    @Test
    public void shouldStayInCurrentRoleOnRequestFromCurrentTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = newState();

        // when
        final long candidateTerm = state.term();

        Outcome<RaftTestMember> outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertEquals( role, outcome.getRole() );
    }

    @Test
    public void shouldMoveToFollowerIfRequestIsFromLaterTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome<RaftTestMember> outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertEquals( Role.FOLLOWER, outcome.getRole() );
    }

    @Test
    public void shouldUpdateTermIfRequestIsFromLaterTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = newState();

        // when
        final long candidateTerm = state.term() + 1;

        Outcome<RaftTestMember> outcome = role.handler.handle( voteRequest().from( member1 ).term( candidateTerm )
                .lastLogIndex( 0 )
                .lastLogTerm( -1 ).build(), state, log() );

        // then
        assertEquals( candidateTerm, outcome.getTerm() );
    }

    public RaftState<RaftTestMember> newState() throws IOException
    {
        return raftState().myself( myself ).build();
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }

}
