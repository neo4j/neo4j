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
package org.neo4j.coreedge.core.state;

import java.io.File;

import org.neo4j.coreedge.core.state.storage.SimpleStorage;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.DiscoveredMemberRepository;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

import static java.lang.Thread.sleep;

public class ClusteringModule
{
    private static final String CLUSTER_ID_NAME = "cluster-id";
    private final CoreTopologyService topologyService;

    public ClusteringModule( DiscoveryServiceFactory discoveryServiceFactory, MemberId myself, PlatformModule platformModule, File clusterStateDirectory )
    {
        LifeSupport life = platformModule.life;
        Config config = platformModule.config;
        LogProvider logProvider = platformModule.logging.getInternalLogProvider();
        Dependencies dependencies = platformModule.dependencies;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;

        SimpleStorage<ClusterId> clusterIdStorage = new SimpleStorage<>( fileSystem, clusterStateDirectory,
                CLUSTER_ID_NAME, new ClusterId.Marshal(), logProvider );

        DiscoveredMemberRepository discoveredMemberRepository =
                new DiscoveredMemberRepository( clusterStateDirectory, fileSystem, logProvider );

        topologyService = discoveryServiceFactory.coreTopologyService( config, myself, discoveredMemberRepository,
                logProvider );
        BindingService bindingService = new BindingService( clusterIdStorage, topologyService, logProvider,
                Clocks.systemClock(), () -> sleep( 100 ), 60000 );

        life.add( topologyService );
        life.add( bindingService );

        dependencies.satisfyDependency( topologyService ); // for tests
    }

    public CoreTopologyService topologyService()
    {
        return topologyService;
    }
}
