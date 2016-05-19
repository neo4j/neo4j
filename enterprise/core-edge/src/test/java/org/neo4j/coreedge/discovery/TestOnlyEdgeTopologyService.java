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

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.coreedge.server.edge.EnterpriseEdgeEditionModule;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

class TestOnlyEdgeTopologyService extends LifecycleAdapter implements EdgeTopologyService
{
    private final BoltAddress edgeMe;
    private final TestOnlyDiscoveryServiceFactory cluster;

    TestOnlyEdgeTopologyService( Config config, TestOnlyDiscoveryServiceFactory cluster )
    {
        this.cluster = cluster;
        this.edgeMe = toEdge( config );
    }

    private BoltAddress toEdge( Config config )
    {
        return new BoltAddress(
                new AdvertisedSocketAddress(
                        EnterpriseEdgeEditionModule.extractBoltAddress( config ).toString() ) );
    }

    @Override
    public void stop()
    {
        cluster.edgeMembers.remove( edgeMe );
    }

    @Override
    public ClusterTopology currentTopology()
    {
        return new TestOnlyClusterTopology( false, cluster.coreMembers, cluster.boltAddresses, cluster.edgeMembers );
    }

    @Override
    public void registerEdgeServer( HostnamePort address )
    {
        cluster.edgeMembers.add( edgeMe );
    }
}
