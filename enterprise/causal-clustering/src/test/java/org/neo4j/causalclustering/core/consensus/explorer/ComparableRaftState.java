/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.explorer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.lang.String.format;

public class ComparableRaftState implements ReadableRaftState
{
    protected final MemberId myself;
    private final Set<MemberId> votingMembers;
    private final Set<MemberId> replicationMembers;
    private final Log log;
    protected long term = 0;
    protected MemberId leader;
    private long leaderCommit = -1;
    private MemberId votedFor = null;
    private Set<MemberId> votesForMe = new HashSet<>();
    private Set<MemberId> heartbeatResponses = new HashSet<>();
    private long lastLogIndexBeforeWeBecameLeader = -1;
    private FollowerStates<MemberId> followerStates = new FollowerStates<>();
    protected final RaftLog entryLog;
    private final InFlightCache inFlightCache;
    private long commitIndex = -1;

    ComparableRaftState( MemberId myself, Set<MemberId> votingMembers, Set<MemberId> replicationMembers,
                         RaftLog entryLog, InFlightCache inFlightCache, LogProvider logProvider )
    {
        this.myself = myself;
        this.votingMembers = votingMembers;
        this.replicationMembers = replicationMembers;
        this.entryLog = entryLog;
        this.inFlightCache = inFlightCache;
        this.log = logProvider.getLog( getClass() );
    }

    ComparableRaftState( ReadableRaftState original ) throws IOException
    {
        this( original.myself(), original.votingMembers(), original.replicationMembers(),
                new ComparableRaftLog( original.entryLog() ), new ConsecutiveInFlightCache(), NullLogProvider.getInstance() );
    }

    @Override
    public MemberId myself()
    {
        return myself;
    }

    @Override
    public Set<MemberId> votingMembers()
    {
        return votingMembers;
    }

    @Override
    public Set<MemberId> replicationMembers()
    {
        return replicationMembers;
    }

    @Override
    public long term()
    {
        return term;
    }

    @Override
    public MemberId leader()
    {
        return leader;
    }

    @Override
    public long leaderCommit()
    {
        return 0;
    }

    @Override
    public MemberId votedFor()
    {
        return votedFor;
    }

    @Override
    public Set<MemberId> votesForMe()
    {
        return votesForMe;
    }

    @Override
    public Set<MemberId> heartbeatResponses()
    {
        return heartbeatResponses;
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
        term = outcome.getTerm();
        votedFor = outcome.getVotedFor();
        leader = outcome.getLeader();
        votesForMe = outcome.getVotesForMe();
        lastLogIndexBeforeWeBecameLeader = outcome.getLastLogIndexBeforeWeBecameLeader();
        followerStates = outcome.getFollowerStates();

        for ( RaftLogCommand logCommand : outcome.getLogCommands() )
        {
            logCommand.applyTo( entryLog, log );
            logCommand.applyTo( inFlightCache, log );
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
