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
package org.neo4j.causalclustering.core.consensus.state;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.log.segmented.InFlightMap;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import org.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftState implements ReadableRaftState
{
    private final MemberId myself;
    private final StateStorage<TermState> termStorage;
    private final StateStorage<VoteState> voteStorage;
    private final RaftMembership membership;
    private final Log log;
    private final RaftLog entryLog;
    private final InFlightMap<RaftLogEntry> inFlightMap;

    private TermState termState;
    private VoteState voteState;

    private MemberId leader;
    private Set<MemberId> votesForMe = new HashSet<>();
    private FollowerStates<MemberId> followerStates = new FollowerStates<>();
    private long leaderCommit = -1;
    private long commitIndex = -1;
    private long lastLogIndexBeforeWeBecameLeader = -1;

    public RaftState( MemberId myself,
                      StateStorage<TermState> termStorage,
                      RaftMembership membership,
                      RaftLog entryLog,
                      StateStorage<VoteState> voteStorage,
                      InFlightMap<RaftLogEntry> inFlightMap, LogProvider logProvider )
    {
        this.myself = myself;
        this.termStorage = termStorage;
        this.voteStorage = voteStorage;
        this.membership = membership;
        this.entryLog = entryLog;
        this.inFlightMap = inFlightMap;
        log = logProvider.getLog( getClass() );
    }

    @Override
    public MemberId myself()
    {
        return myself;
    }

    @Override
    public Set<MemberId> votingMembers()
    {
        return membership.votingMembers();
    }

    @Override
    public Set<MemberId> replicationMembers()
    {
        return membership.replicationMembers();
    }

    @Override
    public long term()
    {
        return termState().currentTerm();
    }

    private TermState termState()
    {
        if ( termState == null )
        {
            termState = termStorage.getInitialState();
        }
        return termState;
    }

    @Override
    public MemberId leader()
    {
        return leader;
    }

    @Override
    public long leaderCommit()
    {
        return leaderCommit;
    }

    @Override
    public MemberId votedFor()
    {
        return voteState().votedFor();
    }

    private VoteState voteState()
    {
        if ( voteState == null )
        {
            voteState = voteStorage.getInitialState();
        }
        return voteState;
    }

    @Override
    public Set<MemberId> votesForMe()
    {
        return votesForMe;
    }

    @Override
    public long lastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    @Override
    public FollowerStates<MemberId> followerStates()
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
        if ( termState().update( outcome.getTerm() ) )
        {
            termStorage.persistStoreData( termState() );
        }
        if ( voteState().update( outcome.getVotedFor(), outcome.getTerm() ) )
        {
            voteStorage.persistStoreData( voteState() );
        }

        logIfLeaderChanged( outcome.getLeader() );
        leader = outcome.getLeader();

        leaderCommit = outcome.getLeaderCommit();
        votesForMe = outcome.getVotesForMe();
        lastLogIndexBeforeWeBecameLeader = outcome.getLastLogIndexBeforeWeBecameLeader();
        followerStates = outcome.getFollowerStates();

        for ( RaftLogCommand logCommand : outcome.getLogCommands() )
        {
            logCommand.applyTo( entryLog, log );
            logCommand.applyTo( inFlightMap, log );
        }
        commitIndex = outcome.getCommitIndex();
    }

    private void logIfLeaderChanged( MemberId leader )
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

    public ExposedRaftState copy()
    {
        return new ExposedRaftState()
        {
            final long leaderCommit = RaftState.this.leaderCommit();
            final long commitIndex = RaftState.this.commitIndex();
            final long appendIndex = RaftState.this.entryLog().appendIndex();
            final long term = RaftState.this.term();

            final Set<MemberId> votingMembers = RaftState.this.votingMembers(); // returned set is never mutated

            @Override
            public long leaderCommit()
            {
                return this.leaderCommit;
            }

            @Override
            public long commitIndex()
            {
                return this.commitIndex;
            }

            @Override
            public long appendIndex()
            {
                return this.appendIndex;
            }

            @Override
            public long term()
            {
                return this.term;
            }

            @Override
            public Set<MemberId> votingMembers()
            {
                return this.votingMembers;
            }
        };
    }
}
