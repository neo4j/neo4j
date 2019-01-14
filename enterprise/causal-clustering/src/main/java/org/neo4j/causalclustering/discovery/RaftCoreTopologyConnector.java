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
package org.neo4j.causalclustering.discovery;

import java.util.Set;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.consensus.LeaderListener;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Makes the Raft aware of changes to the core topology and vice versa
 */
public class RaftCoreTopologyConnector extends LifecycleAdapter implements CoreTopologyService.Listener, LeaderListener
{
    private final CoreTopologyService coreTopologyService;
    private final RaftMachine raftMachine;
    private final String dbName;

    public RaftCoreTopologyConnector( CoreTopologyService coreTopologyService, RaftMachine raftMachine, String dbName )
    {
        this.coreTopologyService = coreTopologyService;
        this.raftMachine = raftMachine;
        this.dbName = dbName;
    }

    @Override
    public void start()
    {
        coreTopologyService.addLocalCoreTopologyListener( this );
        raftMachine.registerListener( this );
    }

    @Override
    public synchronized void onCoreTopologyChange( CoreTopology coreTopology )
    {
        Set<MemberId> targetMembers = coreTopology.members().keySet();
        raftMachine.setTargetMembershipSet( targetMembers );
    }

    @Override
    public void onLeaderSwitch( LeaderInfo leaderInfo )
    {
        coreTopologyService.setLeader( leaderInfo, dbName );
    }

    @Override
    public void onLeaderStepDown( long stepDownTerm )
    {
        coreTopologyService.handleStepDown( stepDownTerm, dbName );
    }

    @Override
    public String dbName()
    {
        return this.dbName;
    }
}
