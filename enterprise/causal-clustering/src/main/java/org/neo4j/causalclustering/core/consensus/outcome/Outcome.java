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
package org.neo4j.causalclustering.core.consensus.outcome;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import org.neo4j.causalclustering.identity.MemberId;

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

    /* Candidate */
    private Set<MemberId> votesForMe;
    private long lastLogIndexBeforeWeBecameLeader;

    /* Leader */
    private FollowerStates<MemberId> followerStates;
    private Collection<ShipCommand> shipCommands = new ArrayList<>();
    private boolean electedLeader;
    private boolean steppingDown;

    public Outcome( Role currentRole, ReadableRaftState ctx )
    {
        defaults( currentRole, ctx );
    }

    public Outcome( Role nextRole, long term, MemberId leader, long leaderCommit, MemberId votedFor,
                    Set<MemberId> votesForMe, long lastLogIndexBeforeWeBecameLeader,
                    FollowerStates<MemberId> followerStates, boolean renewElectionTimeout,
                    Collection<RaftLogCommand> logCommands, Collection<RaftMessages.Directed> outgoingMessages,
                    Collection<ShipCommand> shipCommands, long commitIndex )
    {
        this.nextRole = nextRole;
        this.term = term;
        this.leader = leader;
        this.leaderCommit = leaderCommit;
        this.votedFor = votedFor;
        this.votesForMe = new HashSet<>( votesForMe );
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
        this.followerStates = followerStates;
        this.renewElectionTimeout = renewElectionTimeout;

        this.logCommands.addAll( logCommands );
        this.outgoingMessages.addAll( outgoingMessages );
        this.shipCommands.addAll( shipCommands );
        this.commitIndex = commitIndex;
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

        votesForMe = (currentRole == Role.CANDIDATE) ? new HashSet<>( ctx.votesForMe() ) : new HashSet<>();

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
        assert !steppingDown;
        this.electedLeader = true;
    }

    public void steppingDown()
    {
        assert !electedLeader;
        this.steppingDown = true;
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
               ", lastLogIndexBeforeWeBecameLeader=" + lastLogIndexBeforeWeBecameLeader +
               ", followerStates=" + followerStates +
               ", shipCommands=" + shipCommands +
               ", electedLeader=" + electedLeader +
               ", steppingDown=" + steppingDown +
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
        return steppingDown;
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
}
