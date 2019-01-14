/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.outcome;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import org.neo4j.causalclustering.identity.MemberId;

import static java.util.Collections.emptySet;

/**
 * Holds the outcome of a RAFT role's handling of a message. The role handling logic is stateless
 * and responds to RAFT messages in the context of a supplied state. The outcome is later consumed
 * to update the state and do operations embedded as commands within the outcome.
 *
 * A state update could be to change role, change term, etc.
 * A command could be to append to the RAFT log, tell the log shipper that there was a mismatch, etc.
 */
public class Outcome implements Message, ConsensusOutcome
{
    /* Common */
    private Role nextRole;

    private long term;
    private MemberId leader;

    private long leaderCommit;

    private Collection<RaftLogCommand> logCommands = new ArrayList<>();
    private Collection<RaftMessages.Directed> outgoingMessages = new ArrayList<>();

    private long commitIndex;

    /* Follower */
    private MemberId votedFor;
    private boolean renewElectionTimeout;
    private boolean needsFreshSnapshot;
    private boolean isPreElection;
    private Set<MemberId> preVotesForMe;

    /* Candidate */
    private Set<MemberId> votesForMe;
    private long lastLogIndexBeforeWeBecameLeader;

    /* Leader */
    private FollowerStates<MemberId> followerStates;
    private Collection<ShipCommand> shipCommands = new ArrayList<>();
    private boolean electedLeader;
    private OptionalLong steppingDownInTerm;
    private Set<MemberId> heartbeatResponses;

    public Outcome( Role currentRole, ReadableRaftState ctx )
    {
        defaults( currentRole, ctx );
    }

    public Outcome( Role nextRole, long term, MemberId leader, long leaderCommit, MemberId votedFor,
                    Set<MemberId> votesForMe, Set<MemberId> preVotesForMe, long lastLogIndexBeforeWeBecameLeader,
                    FollowerStates<MemberId> followerStates, boolean renewElectionTimeout,
                    Collection<RaftLogCommand> logCommands, Collection<RaftMessages.Directed> outgoingMessages,
                    Collection<ShipCommand> shipCommands, long commitIndex, Set<MemberId> heartbeatResponses, boolean isPreElection )
    {
        this.nextRole = nextRole;
        this.term = term;
        this.leader = leader;
        this.leaderCommit = leaderCommit;
        this.votedFor = votedFor;
        this.votesForMe = new HashSet<>( votesForMe );
        this.preVotesForMe = new HashSet<>( preVotesForMe );
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
        this.followerStates = followerStates;
        this.renewElectionTimeout = renewElectionTimeout;
        this.heartbeatResponses = new HashSet<>( heartbeatResponses );

        this.logCommands.addAll( logCommands );
        this.outgoingMessages.addAll( outgoingMessages );
        this.shipCommands.addAll( shipCommands );
        this.commitIndex = commitIndex;
        this.isPreElection = isPreElection;
        this.steppingDownInTerm = OptionalLong.empty();
    }

    private void defaults( Role currentRole, ReadableRaftState ctx )
    {
        nextRole = currentRole;

        term = ctx.term();
        leader = ctx.leader();

        leaderCommit = ctx.leaderCommit();

        votedFor = ctx.votedFor();
        renewElectionTimeout = false;
        needsFreshSnapshot = false;

        isPreElection = (currentRole == Role.FOLLOWER) && ctx.isPreElection();
        steppingDownInTerm = OptionalLong.empty();
        preVotesForMe = isPreElection ? new HashSet<>( ctx.preVotesForMe() ) : emptySet();
        votesForMe = (currentRole == Role.CANDIDATE) ? new HashSet<>( ctx.votesForMe() ) : emptySet();
        heartbeatResponses = (currentRole == Role.LEADER) ? new HashSet<>( ctx.heartbeatResponses() ) : emptySet();

        lastLogIndexBeforeWeBecameLeader = (currentRole == Role.LEADER) ? ctx.lastLogIndexBeforeWeBecameLeader() : -1;
        followerStates = (currentRole == Role.LEADER) ? ctx.followerStates() : new FollowerStates<>();

        commitIndex = ctx.commitIndex();
    }

    public void setNextRole( Role nextRole )
    {
        this.nextRole = nextRole;
    }

    public void setNextTerm( long nextTerm )
    {
        this.term = nextTerm;
    }

    public void setLeader( MemberId leader )
    {
        this.leader = leader;
    }

    public void setLeaderCommit( long leaderCommit )
    {
        this.leaderCommit = leaderCommit;
    }

    public void addLogCommand( RaftLogCommand logCommand )
    {
        this.logCommands.add( logCommand );
    }

    public void addOutgoingMessage( RaftMessages.Directed message )
    {
        this.outgoingMessages.add( message );
    }

    public void setVotedFor( MemberId votedFor )
    {
        this.votedFor = votedFor;
    }

    public void renewElectionTimeout()
    {
        this.renewElectionTimeout = true;
    }

    public void markNeedForFreshSnapshot()
    {
        this.needsFreshSnapshot = true;
    }

    public void addVoteForMe( MemberId voteFrom )
    {
        this.votesForMe.add( voteFrom );
    }

    public void setLastLogIndexBeforeWeBecameLeader( long lastLogIndexBeforeWeBecameLeader )
    {
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
    }

    public void replaceFollowerStates( FollowerStates<MemberId> followerStates )
    {
        this.followerStates = followerStates;
    }

    public void addShipCommand( ShipCommand shipCommand )
    {
        shipCommands.add( shipCommand );
    }

    public void electedLeader()
    {
        assert !isSteppingDown();
        this.electedLeader = true;
    }

    public void steppingDown( long stepDownTerm )
    {
        assert !electedLeader;
        steppingDownInTerm = OptionalLong.of( stepDownTerm );
    }

    @Override
    public String toString()
    {
        return "Outcome{" +
               "nextRole=" + nextRole +
               ", term=" + term +
               ", leader=" + leader +
               ", leaderCommit=" + leaderCommit +
               ", logCommands=" + logCommands +
               ", outgoingMessages=" + outgoingMessages +
               ", commitIndex=" + commitIndex +
               ", votedFor=" + votedFor +
               ", renewElectionTimeout=" + renewElectionTimeout +
               ", needsFreshSnapshot=" + needsFreshSnapshot +
               ", votesForMe=" + votesForMe +
               ", preVotesForMe=" + preVotesForMe +
               ", lastLogIndexBeforeWeBecameLeader=" + lastLogIndexBeforeWeBecameLeader +
               ", followerStates=" + followerStates +
               ", shipCommands=" + shipCommands +
               ", electedLeader=" + electedLeader +
               ", steppingDownInTerm=" + steppingDownInTerm +
               '}';
    }

    public Role getRole()
    {
        return nextRole;
    }

    public long getTerm()
    {
        return term;
    }

    public MemberId getLeader()
    {
        return leader;
    }

    public long getLeaderCommit()
    {
        return leaderCommit;
    }

    public Collection<RaftLogCommand> getLogCommands()
    {
        return logCommands;
    }

    public Collection<RaftMessages.Directed> getOutgoingMessages()
    {
        return outgoingMessages;
    }

    public MemberId getVotedFor()
    {
        return votedFor;
    }

    public boolean electionTimeoutRenewed()
    {
        return renewElectionTimeout;
    }

    @Override
    public boolean needsFreshSnapshot()
    {
        return needsFreshSnapshot;
    }

    public Set<MemberId> getVotesForMe()
    {
        return votesForMe;
    }

    public long getLastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    public FollowerStates<MemberId> getFollowerStates()
    {
        return followerStates;
    }

    public Collection<ShipCommand> getShipCommands()
    {
        return shipCommands;
    }

    public boolean isElectedLeader()
    {
        return electedLeader;
    }

    public boolean isSteppingDown()
    {
        return steppingDownInTerm.isPresent();
    }

    public OptionalLong stepDownTerm()
    {
        return steppingDownInTerm;
    }

    @Override
    public long getCommitIndex()
    {
        return commitIndex;
    }

    public void setCommitIndex( long commitIndex )
    {
        this.commitIndex = commitIndex;
    }

    public void addHeartbeatResponse( MemberId from )
    {
        this.heartbeatResponses.add( from );
    }

    public Set<MemberId> getHeartbeatResponses()
    {
        return heartbeatResponses;
    }

    public void setPreElection( boolean isPreElection )
    {
        this.isPreElection = isPreElection;
    }

    public boolean isPreElection()
    {
        return isPreElection;
    }

    public void addPreVoteForMe( MemberId from )
    {
        this.preVotesForMe.add( from );
    }

    public Set<MemberId> getPreVotesForMe()
    {
        return preVotesForMe;
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
        Outcome outcome = (Outcome) o;
        return term == outcome.term && leaderCommit == outcome.leaderCommit && commitIndex == outcome.commitIndex &&
                renewElectionTimeout == outcome.renewElectionTimeout && needsFreshSnapshot == outcome.needsFreshSnapshot &&
                isPreElection == outcome.isPreElection && lastLogIndexBeforeWeBecameLeader == outcome.lastLogIndexBeforeWeBecameLeader &&
                electedLeader == outcome.electedLeader && nextRole == outcome.nextRole &&
                Objects.equals( steppingDownInTerm, outcome.steppingDownInTerm ) && Objects.equals( leader, outcome.leader ) &&
                Objects.equals( logCommands, outcome.logCommands ) && Objects.equals( outgoingMessages, outcome.outgoingMessages ) &&
                Objects.equals( votedFor, outcome.votedFor ) && Objects.equals( preVotesForMe, outcome.preVotesForMe ) &&
                Objects.equals( votesForMe, outcome.votesForMe ) && Objects.equals( followerStates, outcome.followerStates ) &&
                Objects.equals( shipCommands, outcome.shipCommands ) && Objects.equals( heartbeatResponses, outcome.heartbeatResponses );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( nextRole, term, leader, leaderCommit, logCommands, outgoingMessages, commitIndex, votedFor, renewElectionTimeout,
                needsFreshSnapshot, isPreElection, preVotesForMe, votesForMe, lastLogIndexBeforeWeBecameLeader, followerStates, shipCommands, electedLeader,
                steppingDownInTerm, heartbeatResponses );
    }
}
