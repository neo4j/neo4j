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
package org.neo4j.coreedge.raft.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.membership.RaftMembership;
import org.neo4j.coreedge.raft.outcome.LogCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.follower.FollowerState;
import org.neo4j.coreedge.raft.state.follower.FollowerStates;
import org.neo4j.coreedge.raft.state.term.TermState;
import org.neo4j.coreedge.raft.state.vote.VoteState;
import org.neo4j.coreedge.server.RaftTestMember;

import static java.util.Collections.emptySet;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.raft.roles.Role.CANDIDATE;

public class RaftStateTest
{
    @Test
    public void shouldRemoveFollowerStateAfterBecomingLeader() throws Exception
    {
        // given
        RaftState<RaftTestMember> raftState = new RaftState<>( new RaftTestMember( 0 ),
                new InMemoryStateStorage<>( new TermState() ),
                new FakeMembership(), new InMemoryRaftLog(),
                new InMemoryStateStorage<>( new VoteState<>( ) ) );

        raftState.update( new Outcome<>( CANDIDATE, 1, null, -1, null, new HashSet<>(), -1, initialFollowerStates(), true, emptyLogCommands(),
                emptyOutgoingMessages(), Collections.emptySet(), -1) );

        // when
        raftState.update( new Outcome<>( CANDIDATE, 1, null, -1, null, new HashSet<>(), -1, new FollowerStates<>(), true, emptyLogCommands(),
                emptyOutgoingMessages(), Collections.emptySet(), -1) );

        // then
        assertEquals( 0, raftState.followerStates().size() );
    }

    private Collection<RaftMessages.Directed<RaftTestMember>> emptyOutgoingMessages()
    {
        return new ArrayList<>();
    }

    private FollowerStates<RaftTestMember> initialFollowerStates()
    {
        return new FollowerStates<>( new FollowerStates<>(), new RaftTestMember( 1 ), new FollowerState() );
    }

    private Collection<LogCommand> emptyLogCommands()
    {
        return Collections.emptyList();
    }

    private class FakeMembership implements RaftMembership<RaftTestMember>
    {
        @Override
        public Set<RaftTestMember> votingMembers()
        {
            return emptySet();
        }

        @Override
        public Set<RaftTestMember> replicationMembers()
        {
            return emptySet();
        }

        @Override
        public long logIndex()
        {
            return -1;
        }

        @Override
        public void registerListener( Listener listener )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deregisterListener( Listener listener )
        {
            throw new UnsupportedOperationException();
        }
    }
}
