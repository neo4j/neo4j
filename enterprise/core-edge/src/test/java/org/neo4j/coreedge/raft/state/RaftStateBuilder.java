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
package org.neo4j.coreedge.raft.state;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.raft.membership.RaftMembership;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.outcome.LogCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;

import static java.util.Collections.emptySet;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class RaftStateBuilder
{
    public static RaftStateBuilder raftState()
    {
        return new RaftStateBuilder();
    }

    public RaftTestMember myself;
    public Set<RaftTestMember> votingMembers = emptySet();
    public long term;
    public RaftTestMember leader;
    public long leaderCommit = -1;
    public RaftTestMember votedFor;
    private RaftLog entryLog = new InMemoryRaftLog();
    public Set<RaftTestMember> votesForMe = emptySet();
    public long lastLogIndexBeforeWeBecameLeader = -1;
    private FollowerStates<RaftTestMember> followerStates = new FollowerStates<>();

    public RaftStateBuilder myself( RaftTestMember myself )
    {
        this.myself = myself;
        return this;
    }

    public RaftStateBuilder votingMembers( Set<RaftTestMember> currentMembers )
    {
        this.votingMembers = currentMembers;
        return this;
    }

    public RaftStateBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public RaftStateBuilder leader( RaftTestMember leader )
    {
        this.leader = leader;
        return this;
    }

    public RaftStateBuilder leaderCommit( long leaderCommit )
    {
        this.leaderCommit = leaderCommit;
        return this;
    }

    public RaftStateBuilder votedFor( RaftTestMember votedFor )
    {
        this.votedFor = votedFor;
        return this;
    }

    public RaftStateBuilder entryLog( RaftLog entryLog )
    {
        this.entryLog = entryLog;
        return this;
    }

    public RaftStateBuilder votesForMe( Set<RaftTestMember> votesForMe )
    {
        this.votesForMe = votesForMe;
        return this;
    }

    public RaftStateBuilder lastLogIndexBeforeWeBecameLeader( long lastLogIndexBeforeWeBecameLeader )
    {
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
        return this;
    }

    public RaftState<RaftTestMember> build() throws RaftStorageException
    {
        InMemoryTermStore termStore = new InMemoryTermStore();
        InMemoryVoteStore<RaftTestMember> voteStore = new InMemoryVoteStore<>();
        StubMembership membership = new StubMembership();

        RaftState<RaftTestMember> state = new RaftState<>( myself, termStore, membership, entryLog, voteStore );

        Collection<RaftMessages.Directed<RaftTestMember>> noMessages = Collections.emptyList();
        List<LogCommand> noLogCommands = Collections.emptyList();

        state.update( new Outcome<>( null, term, leader, leaderCommit, votedFor, votesForMe, lastLogIndexBeforeWeBecameLeader,
                followerStates, false, noLogCommands, noMessages, Collections.emptySet() ) );
        return state;
    }

    public RaftStateBuilder votingMembers( RaftTestMember... members )
    {
        return votingMembers( asSet( members ) );
    }

    public RaftStateBuilder messagesSentToFollower( RaftTestMember member, long nextIndex )
    {
        return this;
    }

    private class StubMembership implements RaftMembership<RaftTestMember>
    {
        @Override
        public Set<RaftTestMember> votingMembers()
        {
            return votingMembers;
        }

        @Override
        public Set<RaftTestMember> replicationMembers()
        {
            return votingMembers;
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
