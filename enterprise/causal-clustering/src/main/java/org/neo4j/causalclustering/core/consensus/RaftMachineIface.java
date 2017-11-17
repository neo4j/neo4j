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
package org.neo4j.causalclustering.core.consensus;

import java.io.IOException;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import org.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;

public interface RaftMachineIface extends LeaderLocator, CoreMetaData
{
    /**
     * This should be called after the major recovery operations are complete. Before this is called
     * this instance cannot become a leader (the timers are disabled) and entries will not be cached
     * in the in-flight map, because the application process is not running and ready to consume them.
     */
    void postRecoveryActions();

    void stopTimers();

    void triggerElection() throws IOException;

    void panic();

    RaftCoreState coreState();

    void installCoreState( RaftCoreState coreState ) throws IOException;

    void setTargetMembershipSet( Set<MemberId> targetMembers );

    /**
     * Every call to state() gives you an immutable copy of the current state.
     *
     * @return A fresh view of the state.
     */
    ExposedRaftState state();

    ConsensusOutcome handle( RaftMessages.RaftMessage incomingMessage ) throws IOException;

    Role currentRole();

    MemberId identity();

    RaftLogShippingManager logShippingManager();

    long term();

    Set<MemberId> votingMembers();

    Set<MemberId> replicationMembers();
}
