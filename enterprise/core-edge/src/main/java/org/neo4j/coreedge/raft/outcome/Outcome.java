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
package org.neo4j.coreedge.raft.outcome;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.FollowerStates;
import org.neo4j.coreedge.raft.state.ReadableRaftState;

/**
 * Holds the outcome of a RAFT role's handling of a message. The role handling logic is stateless
 * and responds to RAFT messages in the context of a supplied state. The outcome is later consumed
 * to update the state and do operations embedded as commands within the outcome.
 *
 * A state update could be to change role, change term, etc.
 * A command could be to append to the RAFT log, tell the log shipper that there was a mismatch, etc.
 */
public class Outcome<MEMBER> implements Serializable
{
    private static final long serialVersionUID = 4288616769553581132L;

    /* Common */
    private Role newRole;

    private long term;
    private MEMBER leader;

    private long leaderCommit;

    private Collection<LogCommand> logCommands = new ArrayList<>();
    private Collection<RaftMessages.Directed<MEMBER>> outgoingMessages = new ArrayList<>();

    /* Follower */
    private MEMBER votedFor;
    private boolean renewElectionTimeout;

    /* Candidate */
    private Set<MEMBER> votesForMe;
    private long lastLogIndexBeforeWeBecameLeader;

    /* Leader */
    private FollowerStates<MEMBER> followerStates;
    private Collection<ShipCommand> shipCommands = new ArrayList<>();

    public Outcome( Role currentRole, ReadableRaftState<MEMBER> ctx )
    {
        defaults( currentRole, ctx );
    }

    public Outcome( Role newRole, long term, MEMBER leader, long leaderCommit, MEMBER votedFor,
            Set<MEMBER> votesForMe, long lastLogIndexBeforeWeBecameLeader,
            FollowerStates<MEMBER> followerStates, boolean renewElectionTimeout,
            Collection<LogCommand> logCommands, Collection<RaftMessages.Directed<MEMBER>> outgoingMessages,
            Collection<ShipCommand> shipCommands )
    {
        this.newRole = newRole;
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
    }

    private void defaults( Role currentRole, ReadableRaftState<MEMBER> ctx )
    {
        newRole = currentRole;

        term = ctx.term();
        leader = ctx.leader();

        leaderCommit = ctx.leaderCommit();

        votedFor = ctx.votedFor();
        renewElectionTimeout = false;

        votesForMe = (currentRole == Role.CANDIDATE) ? new HashSet<>( ctx.votesForMe() ) : new HashSet<>();

        lastLogIndexBeforeWeBecameLeader = (currentRole == Role.LEADER) ? ctx.lastLogIndexBeforeWeBecameLeader() : -1;
        followerStates = (currentRole == Role.LEADER) ? ctx.followerStates() : new FollowerStates<>();
    }

    public void setNextRole( Role nextRole )
    {
        this.newRole = nextRole;
    }

    public void setNextTerm( long nextTerm )
    {
        this.term = nextTerm;
    }

    public void setLeader( MEMBER leader )
    {
        this.leader = leader;
    }

    public void setLeaderCommit( long leaderCommit )
    {
        this.leaderCommit = leaderCommit;
    }

    public void addLogCommand( LogCommand logCommand )
    {
        this.logCommands.add( logCommand );
    }

    public void addOutgoingMessage( RaftMessages.Directed<MEMBER> message )
    {
        this.outgoingMessages.add( message );
    }

    public void setVotedFor( MEMBER votedFor )
    {
        this.votedFor = votedFor;
    }

    public void renewElectionTimeout()
    {
        this.renewElectionTimeout = true;
    }

    public void addVoteForMe( MEMBER voteFrom )
    {
        this.votesForMe.add( voteFrom );
    }

    public void setLastLogIndexBeforeWeBecameLeader( long lastLogIndexBeforeWeBecameLeader )
    {
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
    }

    public void replaceFollowerStates( FollowerStates<MEMBER> followerStates )
    {
        this.followerStates = followerStates;
    }

    public void addShipCommand( ShipCommand shipCommand )
    {
        shipCommands.add( shipCommand );
    }

    @Override
    public String toString()
    {
        return "Outcome{" +
               "nextRole=" + newRole +
               ", newTerm=" + term +
               ", leader=" + leader +
               ", leaderCommit=" + leaderCommit +
               ", logCommands=" + logCommands +
               ", shipCommands=" + shipCommands +
               ", votedFor=" + votedFor +
               ", updatedVotesForMe=" + votesForMe +
               ", lastLogIndexBeforeWeBecameLeader=" + lastLogIndexBeforeWeBecameLeader +
               ", updatedFollowerStates=" + followerStates +
               ", renewElectionTimeout=" + renewElectionTimeout +
               ", outgoingMessages=" + outgoingMessages +
               '}';
    }

    public Role getNewRole()
    {
        return newRole;
    }

    public long getTerm()
    {
        return term;
    }

    public MEMBER getLeader()
    {
        return leader;
    }

    public long getLeaderCommit()
    {
        return leaderCommit;
    }

    public Collection<LogCommand> getLogCommands()
    {
        return logCommands;
    }

    public Collection<RaftMessages.Directed<MEMBER>> getOutgoingMessages()
    {
        return outgoingMessages;
    }

    public MEMBER getVotedFor()
    {
        return votedFor;
    }

    public boolean electionTimeoutRenewed()
    {
        return renewElectionTimeout;
    }

    public Set<MEMBER> getVotesForMe()
    {
        return votesForMe;
    }

    public long getLastLogIndexBeforeWeBecameLeader()
    {
        return lastLogIndexBeforeWeBecameLeader;
    }

    public FollowerStates<MEMBER> getFollowerStates()
    {
        return followerStates;
    }

    public Collection<ShipCommand> getShipCommands()
    {
        return shipCommands;
    }
}
