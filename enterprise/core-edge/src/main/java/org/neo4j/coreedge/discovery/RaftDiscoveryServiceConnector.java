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
package org.neo4j.coreedge.discovery;

import java.util.Set;

import org.neo4j.coreedge.core.consensus.RaftMachine;
import org.neo4j.coreedge.core.consensus.RaftMachine.BootstrapException;
import org.neo4j.coreedge.core.consensus.membership.MemberIdSet;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class RaftDiscoveryServiceConnector extends LifecycleAdapter implements CoreTopologyService.Listener
{
    private final CoreTopologyService discoveryService;
    private final RaftMachine raftMachine;

    public RaftDiscoveryServiceConnector( CoreTopologyService discoveryService, RaftMachine raftMachine )
    {
        this.discoveryService = discoveryService;
        this.raftMachine = raftMachine;
    }

    @Override
    public void start() throws BootstrapException
    {
        ClusterTopology clusterTopology = discoveryService.currentTopology();
        Set<MemberId> initialMembers = clusterTopology.coreMembers();

        if ( clusterTopology.canBeBootstrapped() )
        {
            raftMachine.bootstrapWithInitialMembers( new MemberIdSet( initialMembers ) );
        }

        discoveryService.addCoreTopologyListener( this );
    }

    @Override
    public synchronized void onCoreTopologyChange()
    {
        Set<MemberId> targetMembers = discoveryService.currentTopology().coreMembers();
        raftMachine.setTargetMembershipSet( targetMembers );
    }
}
