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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.coreedge.core.state.storage.InMemoryStateStorage;
import org.neo4j.coreedge.core.state.storage.StateStorage;
import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.log.InMemoryRaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLog;
import org.neo4j.coreedge.core.consensus.membership.RaftMembership;
import org.neo4j.coreedge.core.consensus.outcome.RaftLogCommand;
import org.neo4j.coreedge.core.consensus.outcome.Outcome;
import org.neo4j.coreedge.core.consensus.roles.follower.FollowerStates;
import org.neo4j.coreedge.core.consensus.term.TermState;
import org.neo4j.coreedge.core.consensus.vote.VoteState;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.emptySet;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class RaftStateBuilder
{
    public static RaftStateBuilder raftState()
    {
        return new RaftStateBuilder();
    }

    public MemberId myself;
    private Set<MemberId> votingMembers = emptySet();
    public long term;
    public MemberId leader;
    public long leaderCommit = -1;
    private MemberId votedFor;
    private RaftLog entryLog = new InMemoryRaftLog();
    private Set votesForMe = emptySet();
    private long lastLogIndexBeforeWeBecameLeader = -1;
    public long commitIndex = -1;
    private FollowerStates followerStates = new FollowerStates<>();

    public RaftStateBuilder myself( MemberId myself )
    {
        this.myself = myself;
        return this;
    }

    public RaftStateBuilder votingMembers( Set<MemberId> currentMembers )
    {
        this.votingMembers = currentMembers;
        return this;
    }

    public RaftStateBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public RaftStateBuilder leader( MemberId leader )
    {
        this.leader = leader;
        return this;
    }

    public RaftStateBuilder leaderCommit( long leaderCommit )
    {
        this.leaderCommit = leaderCommit;
        return this;
    }

    public RaftStateBuilder votedFor( MemberId votedFor )
    {
        this.votedFor = votedFor;
        return this;
    }

    public RaftStateBuilder entryLog( RaftLog entryLog )
    {
        this.entryLog = entryLog;
        return this;
    }

    public RaftStateBuilder votesForMe( Set votesForMe )
    {
        this.votesForMe = votesForMe;
        return this;
    }

    public RaftStateBuilder lastLogIndexBeforeWeBecameLeader( long lastLogIndexBeforeWeBecameLeader )
    {
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
        return this;
    }

    public RaftStateBuilder commitIndex( long commitIndex )
    {
        this.commitIndex = commitIndex;
        return this;
    }

    public RaftState build() throws IOException
    {
        StateStorage<TermState> termStore = new InMemoryStateStorage<>( new TermState() );
        StateStorage<VoteState> voteStore = new InMemoryStateStorage<>( new VoteState( ) );
        StubMembership membership = new StubMembership();

        RaftState state =
                new RaftState( myself, termStore, membership, entryLog, voteStore, NullLogProvider.getInstance() );

        Collection<RaftMessages.Directed> noMessages = Collections.emptyList();
        List<RaftLogCommand> noLogCommands = Collections.emptyList();

        state.update( new Outcome( null, term, leader, leaderCommit, votedFor, votesForMe,
                lastLogIndexBeforeWeBecameLeader, followerStates, false, noLogCommands,
                noMessages, Collections.emptySet(), commitIndex ) );

        return state;
    }

    public RaftStateBuilder votingMembers( MemberId... members )
    {
        return votingMembers( asSet( members ) );
    }

    public RaftStateBuilder messagesSentToFollower( MemberId member, long nextIndex )
    {
        return this;
    }

    private class StubMembership implements RaftMembership
    {
        @Override
        public Set<MemberId> votingMembers()
        {
            return votingMembers;
        }

        @Override
        public Set<MemberId> replicationMembers()
        {
            return votingMembers;
        }

        @Override
        public void registerListener( Listener listener )
        {
            throw new UnsupportedOperationException();
        }
    }
}
