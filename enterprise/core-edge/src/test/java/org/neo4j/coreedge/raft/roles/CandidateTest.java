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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.coreedge.raft.NewLeaderBarrier;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.outcome.AppendLogEntry;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.RaftState;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.coreedge.raft.TestMessageBuilders.voteResponse;
import static org.neo4j.coreedge.raft.roles.Role.CANDIDATE;
import static org.neo4j.coreedge.raft.roles.Role.FOLLOWER;
import static org.neo4j.coreedge.raft.roles.Role.LEADER;
import static org.neo4j.coreedge.raft.state.RaftStateBuilder.raftState;
import static org.neo4j.coreedge.server.RaftTestMember.member;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class CandidateTest
{
    private RaftTestMember myself = member( 0 );
    private RaftTestMember member1 = member( 1 );

    @Mock
    private Inbound inbound;

    private LogProvider logProvider = NullLogProvider.getInstance();

    @Test
    public void shouldBeElectedLeaderOnReceivingGrantedVoteResponseWithCurrentTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = newState();

        // when
        Outcome<RaftTestMember> outcome = CANDIDATE.handler.handle( voteResponse()
                .term( state.term() )
                .from( member1 )
                .grant()
                .build(), state, log() );

        // then
        assertEquals( LEADER, outcome.getRole() );
        assertThat( outcome.getLogCommands(), hasItem( new AppendLogEntry( 0,
                new RaftLogEntry( state.term(), new NewLeaderBarrier() ) ) ) );
    }

    @Test
    public void shouldStayAsCandidateOnReceivingDeniedVoteResponseWithCurrentTerm() throws Exception
    {
        // given
        RaftState<RaftTestMember> state = newState();

        // when
        Outcome<RaftTestMember> outcome = CANDIDATE.handler.handle( voteResponse()
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
        RaftState<RaftTestMember> state = newState();

        final long voterTerm = state.term() + 1;

        // when
        Outcome<RaftTestMember> outcome = CANDIDATE.handler.handle( voteResponse()
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
        RaftState<RaftTestMember> state = newState();

        final long voterTerm = state.term() - 1;

        // when
        Outcome<RaftTestMember> outcome = CANDIDATE.handler.handle( voteResponse()
                .term( voterTerm )
                .from( member1 )
                .grant()
                .build(), state, log() );

        // then
        assertEquals( CANDIDATE, outcome.getRole() );
    }

    public RaftState<RaftTestMember> newState() throws IOException
    {
        return raftState().myself( myself ).build();
    }

    private Log log()
    {
        return logProvider.getLog( getClass() );
    }

}
