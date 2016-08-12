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

import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class SharedDiscoveryEdgeClient extends LifecycleAdapter implements TopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final EdgeAddresses addresses;
    private final Log log;

    SharedDiscoveryEdgeClient( SharedDiscoveryService sharedDiscoveryService, AdvertisedSocketAddress boltAddress,
                               LogProvider logProvider )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.addresses = new EdgeAddresses( boltAddress );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        sharedDiscoveryService.registerEdgeMember( addresses );
        log.info( "Registered edge server at %s", addresses );
    }

    @Override
    public void stop() throws Throwable
    {
        sharedDiscoveryService.unRegisterEdgeMember( addresses );
    }

    @Override
    public ClusterTopology currentTopology()
    {
        ClusterTopology topology = sharedDiscoveryService.currentTopology( null );
        log.info( "Current topology is %s", topology );
        return topology;
    }
}
