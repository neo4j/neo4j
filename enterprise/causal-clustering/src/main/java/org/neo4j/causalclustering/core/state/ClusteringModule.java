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
package org.neo4j.causalclustering.core.state;

import java.io.File;

import org.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.ClusterIdentity;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

import static java.lang.Thread.sleep;
import static org.neo4j.causalclustering.core.server.CoreServerModule.CLUSTER_ID_NAME;

public class ClusteringModule
{
    private final CoreTopologyService topologyService;
    private final ClusterIdentity clusterIdentity;

    public ClusteringModule( DiscoveryServiceFactory discoveryServiceFactory, MemberId myself,
            PlatformModule platformModule, File clusterStateDirectory )
    {
        LifeSupport life = platformModule.life;
        Config config = platformModule.config;
        LogProvider logProvider = platformModule.logging.getInternalLogProvider();
        LogProvider userLogProvider = platformModule.logging.getUserLogProvider();
        Dependencies dependencies = platformModule.dependencies;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;

        topologyService = discoveryServiceFactory
                .coreTopologyService( config, myself, platformModule.jobScheduler, logProvider, userLogProvider );

        life.add( topologyService );

        dependencies.satisfyDependency( topologyService ); // for tests

        SimpleStorage<ClusterId> clusterIdStorage =
                new SimpleFileStorage<>( fileSystem, clusterStateDirectory, CLUSTER_ID_NAME, new ClusterId.Marshal(),
                        logProvider );

        CoreBootstrapper coreBootstrapper =
                new CoreBootstrapper( platformModule.storeDir, platformModule.pageCache, fileSystem, config, logProvider );

        clusterIdentity = new ClusterIdentity( clusterIdStorage, topologyService, logProvider, Clocks.systemClock(),
                () -> sleep( 100 ), 300_000, coreBootstrapper );
    }

    public CoreTopologyService topologyService()
    {
        return topologyService;
    }

    public ClusterIdentity clusterIdentity()
    {
        return clusterIdentity;
    }
}
