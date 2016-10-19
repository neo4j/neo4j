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
package org.neo4j.causalclustering.discovery;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class SharedDiscoveryReadReplicaClient extends LifecycleAdapter implements TopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final ReadReplicaAddresses addresses;
    private final Log log;

    SharedDiscoveryReadReplicaClient( SharedDiscoveryService sharedDiscoveryService, Config config,
                               LogProvider logProvider )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.addresses = new ReadReplicaAddresses( ClientConnectorAddresses.extractFromConfig( config ) );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        sharedDiscoveryService.registerReadReplica( addresses );
        log.info( "Registered read replica at %s", addresses );
    }

    @Override
    public void stop() throws Throwable
    {
        sharedDiscoveryService.unRegisterReadReplica( addresses );
    }

    @Override
    public CoreTopology coreServers()
    {
        CoreTopology topology = sharedDiscoveryService.coreTopology( null );
        log.info( "Core topology is %s", topology );
        return topology;
    }
}
