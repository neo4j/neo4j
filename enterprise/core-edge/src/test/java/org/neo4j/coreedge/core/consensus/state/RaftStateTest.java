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
package org.neo4j.coreedge.core.consensus.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.log.InMemoryRaftLog;
import org.neo4j.coreedge.core.consensus.membership.RaftMembership;
import org.neo4j.coreedge.core.consensus.outcome.Outcome;
import org.neo4j.coreedge.core.consensus.outcome.RaftLogCommand;
import org.neo4j.coreedge.core.consensus.roles.follower.FollowerState;
import org.neo4j.coreedge.core.consensus.roles.follower.FollowerStates;
import org.neo4j.coreedge.core.consensus.term.TermState;
import org.neo4j.coreedge.core.consensus.vote.VoteState;
import org.neo4j.coreedge.core.state.storage.InMemoryStateStorage;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.emptySet;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.core.consensus.roles.Role.CANDIDATE;
import static org.neo4j.coreedge.identity.RaftTestMember.member;

public class RaftStateTest
{
    @Test
    public void shouldRemoveFollowerStateAfterBecomingLeader() throws Exception
    {
        // given
        RaftState raftState = new RaftState( member( 0 ),
                new InMemoryStateStorage<>( new TermState() ),
                new FakeMembership(), new InMemoryRaftLog(),
                new InMemoryStateStorage<>( new VoteState( ) ),
                NullLogProvider.getInstance() );

        raftState.update( new Outcome( CANDIDATE, 1, null, -1, null, new HashSet<>(), -1, initialFollowerStates(), true, emptyLogCommands(),
                emptyOutgoingMessages(), Collections.emptySet(), -1) );

        // when
        raftState.update( new Outcome( CANDIDATE, 1, null, -1, null, new HashSet<>(), -1, new FollowerStates<>(), true, emptyLogCommands(),
                emptyOutgoingMessages(), Collections.emptySet(), -1) );

        // then
        assertEquals( 0, raftState.followerStates().size() );
    }

    private Collection<RaftMessages.Directed> emptyOutgoingMessages()
    {
        return new ArrayList<>();
    }

    private FollowerStates initialFollowerStates()
    {
        return new FollowerStates<>( new FollowerStates<>(), member( 1 ), new FollowerState() );
    }

    private Collection<RaftLogCommand> emptyLogCommands()
    {
        return Collections.emptyList();
    }

    private class FakeMembership implements RaftMembership
    {
        @Override
        public Set votingMembers()
        {
            return emptySet();
        }

        @Override
        public Set replicationMembers()
        {
            return emptySet();
        }

        @Override
        public void registerListener( Listener listener )
        {
            throw new UnsupportedOperationException();
        }
    }
}
