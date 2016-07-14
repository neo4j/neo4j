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
package org.neo4j.coreedge.raft.state.explorer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.outcome.RaftLogCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.coreedge.raft.state.follower.FollowerStates;
import org.neo4j.coreedge.server.CoreMember;

import static java.lang.String.format;

public class ComparableRaftState implements ReadableRaftState
{
    protected final CoreMember myself;
    protected final Set votingMembers;
    protected final Set replicationMembers;
    protected long term = 0;
    protected CoreMember leader;
    private long leaderCommit = -1;
    protected CoreMember votedFor = null;
    protected Set votesForMe = new HashSet<>();
    protected long lastLogIndexBeforeWeBecameLeader = -1;
    protected FollowerStates followerStates = new FollowerStates<>();
    protected final RaftLog entryLog;
    protected final InFlightMap<Long,RaftLogEntry> inFlightMap;
    private long commitIndex = -1;

    public ComparableRaftState( CoreMember myself, Set votingMembers, Set replicationMembers,
            RaftLog entryLog, InFlightMap<Long,RaftLogEntry> inFlightMap )
    {
        this.myself = myself;
        this.votingMembers = votingMembers;
        this.replicationMembers = replicationMembers;
        this.entryLog = entryLog;
        this.inFlightMap = inFlightMap;
    }

    public ComparableRaftState( ReadableRaftState original ) throws IOException
    {
        this( original.myself(), original.votingMembers(), original.replicationMembers(), new ComparableRaftLog( original.entryLog() ),
                new InFlightMap<>() );
    }

    @Override
    public CoreMember myself()
    {
        return myself;
    }

    @Override
    public Set votingMembers()
    {
        return votingMembers;
    }

    @Override
    public Set replicationMembers()
    {
        return replicationMembers;
    }

    @Override
    public long term()
    {
        return term;
    }

    @Override
    public CoreMember leader()
    {
        return leader;
    }

    @Override
    public long leaderCommit()
    {
        return 0;
    }

    @Override
    public CoreMember votedFor()
    {
        return votedFor;
    }

    @Override
    public Set votesForMe()
    {
        return votesForMe;
    }

    @Override
    public long lastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    @Override
    public FollowerStates followerStates()
    {
        return followerStates;
    }

    @Override
    public ReadableRaftLog entryLog()
    {
        return entryLog;
    }

    @Override
    public long commitIndex()
    {
        return commitIndex;
    }

    public void update( Outcome outcome ) throws IOException
    {
        term = outcome.getTerm();
        votedFor = outcome.getVotedFor();
        leader = outcome.getLeader();
        votesForMe = outcome.getVotesForMe();
        lastLogIndexBeforeWeBecameLeader = outcome.getLastLogIndexBeforeWeBecameLeader();
        followerStates= outcome.getFollowerStates();

        for ( RaftLogCommand logCommand : outcome.getLogCommands() )
        {
            logCommand.applyTo( entryLog );
            logCommand.applyTo( inFlightMap );
        }

        commitIndex = outcome.getCommitIndex();
    }

    @Override
    public String toString()
    {
        return format( "state{myself=%s, term=%s, leader=%s, leaderCommit=%d, appended=%d, committed=%d, " +
                        "votedFor=%s, votesForMe=%s, lastLogIndexBeforeWeBecameLeader=%d, followerStates=%s}",
                myself, term, leader, leaderCommit,
                entryLog.appendIndex(), commitIndex, votedFor, votesForMe,
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
