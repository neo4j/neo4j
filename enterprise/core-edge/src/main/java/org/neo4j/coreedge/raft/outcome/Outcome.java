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
import java.util.Collection;
import java.util.Set;

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.FollowerStates;

public class Outcome<MEMBER> implements Serializable
{
    public final Role newRole;
    public final long newTerm;
    public final MEMBER leader;
    public final long leaderCommit;
    public final MEMBER votedFor;
    public final Set<MEMBER> votesForMe;
    public final long lastLogIndexBeforeWeBecameLeader;
    public final FollowerStates<MEMBER> followerStates;
    public final boolean renewElectionTimeout;
    public final Iterable<LogCommand> logCommands;
    public final Iterable<ShipCommand> shipCommands;
    public final Collection<RaftMessages.Directed<MEMBER>> outgoingMessages;

    public Outcome(Role newRole, long newTerm, MEMBER leader, long leaderCommit, MEMBER votedFor,
                   Set<MEMBER> votesForMe, long lastLogIndexBeforeWeBecameLeader,
                   FollowerStates<MEMBER> followerStates, boolean renewElectionTimeout,
                   Iterable<LogCommand> logCommands, Collection<RaftMessages.Directed<MEMBER>> outgoingMessages,
                   Iterable<ShipCommand> shipCommands )
    {
        this.newRole = newRole;
        this.newTerm = newTerm;
        this.leader = leader;
        this.leaderCommit = leaderCommit;
        this.votedFor = votedFor;
        this.votesForMe = votesForMe;
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
        this.followerStates = followerStates;
        this.renewElectionTimeout = renewElectionTimeout;
        this.logCommands = logCommands;
        this.outgoingMessages = outgoingMessages;
        this.shipCommands = shipCommands;
    }

    @Override
    public String toString()
    {
        return "Outcome{" +
               "newRole=" + newRole +
               ", newTerm=" + newTerm +
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
}
