/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core;

import java.io.File;
import java.util.function.Function;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;

public class CoreGraphDatabase extends GraphDatabaseFacade
{
    protected CoreGraphDatabase()
    {
    }

    public CoreGraphDatabase( File storeDir, Config config, GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        this( storeDir, config, dependencies, new HazelcastDiscoveryServiceFactory() );
    }

    public CoreGraphDatabase( File storeDir, Config config,
            GraphDatabaseFacadeFactory.Dependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory )
    {
        Function<PlatformModule,EditionModule> factory =
                platformModule -> new EnterpriseCoreEditionModule( platformModule, discoveryServiceFactory );
        new GraphDatabaseFacadeFactory( DatabaseInfo.CORE, factory ).initFacade( storeDir, config, dependencies, this );
    }

    public Role getRole()
    {
        return getDependencyResolver().resolveDependency( RaftMachine.class ).currentRole();
    }

    public void disableCatchupServer() throws Throwable
    {
        ((EnterpriseCoreEditionModule) editionModule).disableCatchupServer();
    }
}
