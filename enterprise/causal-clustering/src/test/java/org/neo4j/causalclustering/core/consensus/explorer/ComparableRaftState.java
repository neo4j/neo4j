/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.explorer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
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
    protected long term;
    protected MemberId leader;
    private LeaderInfo leaderInfo = LeaderInfo.INITIAL;
    private long leaderCommit = -1;
    private MemberId votedFor;
    private Set<MemberId> votesForMe = new HashSet<>();
    private Set<MemberId> preVotesForMe = new HashSet<>();
    private Set<MemberId> heartbeatResponses = new HashSet<>();
    private long lastLogIndexBeforeWeBecameLeader = -1;
    private FollowerStates<MemberId> followerStates = new FollowerStates<>();
    protected final RaftLog entryLog;
    private final InFlightCache inFlightCache;
    private long commitIndex = -1;
    private boolean isPreElection;
    private final boolean refusesToBeLeader;

    ComparableRaftState( MemberId myself, Set<MemberId> votingMembers, Set<MemberId> replicationMembers, boolean refusesToBeLeader,
                         RaftLog entryLog, InFlightCache inFlightCache, LogProvider logProvider )
    {
        this.myself = myself;
        this.votingMembers = votingMembers;
        this.replicationMembers = replicationMembers;
        this.entryLog = entryLog;
        this.inFlightCache = inFlightCache;
        this.log = logProvider.getLog( getClass() );
        this.refusesToBeLeader = refusesToBeLeader;
    }

    ComparableRaftState( ReadableRaftState original ) throws IOException
    {
        this( original.myself(), original.votingMembers(), original.replicationMembers(), original.refusesToBeLeader(),
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
    public LeaderInfo leaderInfo()
    {
        return leaderInfo;
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

    @Override
    public boolean supportPreVoting()
    {
        return false;
    }

    @Override
    public boolean isPreElection()
    {
        return isPreElection;
    }

    @Override
    public Set<MemberId> preVotesForMe()
    {
        return preVotesForMe;
    }

    @Override
    public boolean refusesToBeLeader()
    {
        return refusesToBeLeader;
    }

    public void update( Outcome outcome ) throws IOException
    {
        term = outcome.getTerm();
        votedFor = outcome.getVotedFor();
        leader = outcome.getLeader();
        votesForMe = outcome.getVotesForMe();
        lastLogIndexBeforeWeBecameLeader = outcome.getLastLogIndexBeforeWeBecameLeader();
        followerStates = outcome.getFollowerStates();
        isPreElection = outcome.isPreElection();

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
        return Objects.hash( myself, votingMembers, term, leader, entryLog, votedFor, votesForMe,
                lastLogIndexBeforeWeBecameLeader, followerStates );
    }
}
