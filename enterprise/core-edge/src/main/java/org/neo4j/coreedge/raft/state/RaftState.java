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

import java.util.HashSet;
import java.util.Set;

import org.neo4j.coreedge.raft.membership.RaftMembership;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.outcome.LogCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.follower.FollowerStates;
import org.neo4j.coreedge.raft.state.term.TermState;
import org.neo4j.coreedge.raft.state.vote.VoteState;

public class RaftState<MEMBER> implements ReadableRaftState<MEMBER>
{
    private final MEMBER myself;
    private final RaftMembership<MEMBER> membership;
    private final TermState termState;
    private MEMBER leader;
    private long leaderCommit = -1;
    private final VoteState<MEMBER> voteState;
    private Set<MEMBER> votesForMe = new HashSet<>();
    private long lastLogIndexBeforeWeBecameLeader = -1;
    private FollowerStates<MEMBER> followerStates = new FollowerStates<>();
    private final RaftLog entryLog;

    public RaftState( MEMBER myself, TermState termState, RaftMembership<MEMBER> membership,
                      RaftLog entryLog, VoteState<MEMBER> voteState )
    {
        this.myself = myself;
        this.termState = termState;
        this.voteState = voteState;
        this.membership = membership;
        this.entryLog = entryLog;
    }

    @Override
    public MEMBER myself()
    {
        return myself;
    }

    @Override
    public Set<MEMBER> votingMembers()
    {
        return membership.votingMembers();
    }

    @Override
    public Set<MEMBER> replicationMembers()
    {
        return membership.replicationMembers();
    }

    @Override
    public long term()
    {
        return termState.currentTerm();
    }

    @Override
    public MEMBER leader()
    {
        return leader;
    }

    @Override
    public long leaderCommit()
    {
        return leaderCommit;
    }

    @Override
    public MEMBER votedFor()
    {
        return voteState.votedFor();
    }

    @Override
    public Set<MEMBER> votesForMe()
    {
        return votesForMe;
    }

    @Override
    public long lastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    @Override
    public FollowerStates<MEMBER> followerStates()
    {
        return followerStates;
    }

    @Override
    public ReadableRaftLog entryLog()
    {
        return entryLog;
    }

    public void update( Outcome<MEMBER> outcome ) throws RaftStorageException
    {
        termState.update( outcome.getTerm() );
        voteState.votedFor( outcome.getVotedFor() );
        leader = outcome.getLeader();
        leaderCommit = outcome.getLeaderCommit();
        votesForMe = outcome.getVotesForMe();
        lastLogIndexBeforeWeBecameLeader = outcome.getLastLogIndexBeforeWeBecameLeader();
        followerStates = outcome.getFollowerStates();

        for ( LogCommand logCommand : outcome.getLogCommands() )
        {
            logCommand.applyTo( entryLog );
        }
    }
}
