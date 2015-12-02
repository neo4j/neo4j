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
package org.neo4j.coreedge.raft.state.explorer;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.outcome.LogCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.FollowerStates;
import org.neo4j.coreedge.raft.state.ReadableRaftState;

import static java.lang.String.format;

public class ComparableRaftState implements ReadableRaftState<RaftTestMember>
{
    protected final RaftTestMember myself;
    protected final Set<RaftTestMember> votingMembers;
    protected final Set<RaftTestMember> replicationMembers;
    protected long term = 0;
    protected RaftTestMember leader;
    private long leaderCommit = -1;
    protected RaftTestMember votedFor = null;
    protected Set<RaftTestMember> votesForMe = new HashSet<>();
    protected long lastLogIndexBeforeWeBecameLeader = -1;
    protected FollowerStates<RaftTestMember> followerStates = new FollowerStates<>();
    protected final RaftLog entryLog;

    public ComparableRaftState( RaftTestMember myself,
                                Set<RaftTestMember> votingMembers,
                                Set<RaftTestMember> replicationMembers,
                                RaftLog entryLog )
    {
        this.myself = myself;
        this.votingMembers = votingMembers;
        this.replicationMembers = replicationMembers;
        this.entryLog = entryLog;
    }

    public ComparableRaftState( ReadableRaftState<RaftTestMember> original ) throws RaftStorageException
    {
        this( original.myself(), original.votingMembers(), original.replicationMembers(), new ComparableRaftLog( original.entryLog() ) );
    }

    @Override
    public RaftTestMember myself()
    {
        return myself;
    }

    @Override
    public Set<RaftTestMember> votingMembers()
    {
        return votingMembers;
    }

    @Override
    public Set<RaftTestMember> replicationMembers()
    {
        return replicationMembers;
    }

    @Override
    public long term()
    {
        return term;
    }

    @Override
    public RaftTestMember leader()
    {
        return leader;
    }

    @Override
    public long leaderCommit()
    {
        return 0;
    }

    @Override
    public RaftTestMember votedFor()
    {
        return votedFor;
    }

    @Override
    public Set<RaftTestMember> votesForMe()
    {
        return votesForMe;
    }

    @Override
    public long lastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    @Override
    public FollowerStates<RaftTestMember> followerStates()
    {
        return followerStates;
    }

    @Override
    public ReadableRaftLog entryLog()
    {
        return entryLog;
    }

    public void update( Outcome<RaftTestMember> outcome ) throws RaftStorageException
    {
        term = outcome.getTerm();
        votedFor = outcome.getVotedFor();
        leader = outcome.getLeader();
        votesForMe = outcome.getVotesForMe();
        lastLogIndexBeforeWeBecameLeader = outcome.getLastLogIndexBeforeWeBecameLeader();
        followerStates= outcome.getFollowerStates();

        for ( LogCommand logCommand : outcome.getLogCommands() )
        {
            logCommand.applyTo( entryLog );
        }
    }

    @Override
    public String toString()
    {
        return format( "state{myself=%s, term=%s, leader=%s, leaderCommit=%d, appended=%d, committed=%d, " +
                        "votedFor=%s, votesForMe=%s, lastLogIndexBeforeWeBecameLeader=%d, followerStates=%s}",
                myself, term, leader, leaderCommit,
                entryLog.appendIndex(), entryLog.commitIndex(), votedFor, votesForMe,
                lastLogIndexBeforeWeBecameLeader, followerStates );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ComparableRaftState that = (ComparableRaftState) o;
        return Objects.equals( term, that.term ) &&
                Objects.equals( lastLogIndexBeforeWeBecameLeader, that.lastLogIndexBeforeWeBecameLeader ) &&
                Objects.equals( myself, that.myself ) &&
                Objects.equals( votingMembers, that.votingMembers ) &&
                Objects.equals( leader, that.leader ) &&
                Objects.equals( leaderCommit, that.leaderCommit ) &&
                Objects.equals( entryLog, that.entryLog ) &&
                Objects.equals( votedFor, that.votedFor ) &&
                Objects.equals( votesForMe, that.votesForMe ) &&
                Objects.equals( followerStates, that.followerStates );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( myself, votingMembers, term, leader, entryLog, votedFor, votesForMe, lastLogIndexBeforeWeBecameLeader, followerStates );
    }
}
