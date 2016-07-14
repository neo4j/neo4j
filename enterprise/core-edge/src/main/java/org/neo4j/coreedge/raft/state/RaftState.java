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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.membership.RaftMembership;
import org.neo4j.coreedge.raft.outcome.LogCommand;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.follower.FollowerStates;
import org.neo4j.coreedge.raft.state.term.TermState;
import org.neo4j.coreedge.raft.state.vote.VoteState;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftState implements ReadableRaftState
{
    private final CoreMember myself;
    private final StateStorage<TermState> termStorage;
    private final StateStorage<VoteState> voteStorage;
    private final RaftMembership membership;
    private final TermState termState;
    private final Log log;
    private CoreMember leader;
    private long leaderCommit = -1;
    private final VoteState voteState;
    private Set<CoreMember> votesForMe = new HashSet<>();
    private long lastLogIndexBeforeWeBecameLeader = -1;
    private FollowerStates<CoreMember> followerStates = new FollowerStates<>();
    private final RaftLog entryLog;
    private final InFlightMap<Long,RaftLogEntry> inFlightMap;
    private long commitIndex = -1;

    public RaftState( CoreMember myself,
                      StateStorage<TermState> termStorage,
                      RaftMembership membership,
                      RaftLog entryLog,
                      StateStorage<VoteState> voteStorage,
                      InFlightMap<Long, RaftLogEntry> inFlightMap, LogProvider logProvider )
    {
        this.myself = myself;
        this.termStorage = termStorage;
        this.voteStorage = voteStorage;
        this.termState = termStorage.getInitialState();
        this.voteState = voteStorage.getInitialState();
        this.membership = membership;
        this.entryLog = entryLog;
        this.inFlightMap = inFlightMap;
        log = logProvider.getLog( getClass() );
    }

    @Override
    public CoreMember myself()
    {
        return myself;
    }

    @Override
    public Set<CoreMember> votingMembers()
    {
        return membership.votingMembers();
    }

    @Override
    public Set<CoreMember> replicationMembers()
    {
        return membership.replicationMembers();
    }

    @Override
    public long term()
    {
        return termState.currentTerm();
    }

    @Override
    public CoreMember leader()
    {
        return leader;
    }

    @Override
    public long leaderCommit()
    {
        return leaderCommit;
    }

    @Override
    public CoreMember votedFor()
    {
        return voteState.votedFor();
    }

    @Override
    public Set<CoreMember> votesForMe()
    {
        return votesForMe;
    }

    @Override
    public long lastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    @Override
    public FollowerStates<CoreMember> followerStates()
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
        if ( termState.update( outcome.getTerm() ) )
        {
            termStorage.persistStoreData( termState );
        }
        if ( voteState.update( outcome.getVotedFor(), outcome.getTerm() ) )
        {
            voteStorage.persistStoreData( voteState );
        }

        logIfLeaderChanged( outcome.getLeader() );
        leader = outcome.getLeader();

        leaderCommit = outcome.getLeaderCommit();
        votesForMe = outcome.getVotesForMe();
        lastLogIndexBeforeWeBecameLeader = outcome.getLastLogIndexBeforeWeBecameLeader();
        followerStates = outcome.getFollowerStates();

        for ( LogCommand logCommand : outcome.getLogCommands() )
        {
            logCommand.applyTo( entryLog );
            logCommand.applyTo( inFlightMap );
        }
        commitIndex = outcome.getCommitIndex();
    }

    private void logIfLeaderChanged( CoreMember leader )
    {
        if ( this.leader == null )
        {
            if ( leader != null )
            {
                log.info( "First leader elected: %s", leader );
            }
            return;
        }

        if ( !this.leader.equals( leader ) )
        {
            log.info( "Leader changed from %s to %s", this.leader, leader );
        }
    }
}
