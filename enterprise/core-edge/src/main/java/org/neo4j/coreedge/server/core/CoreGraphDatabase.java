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
package org.neo4j.coreedge.server.core;

import java.io.File;
import java.util.Map;

import org.neo4j.coreedge.server.EnterpriseCoreFacadeFactory;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;

public class CoreGraphDatabase extends GraphDatabaseFacade
{
    private EnterpriseCoreEditionModule coreEditionModule;

    public CoreGraphDatabase( File storeDir, Map<String, String> params,
                              GraphDatabaseFacadeFactory.Dependencies dependencies, DiscoveryServiceFactory
                                      discoveryServiceFactory )
    {
        new EnterpriseCoreFacadeFactory( discoveryServiceFactory ).newFacade( storeDir, params, dependencies, this );
    }

    public CoreGraphDatabase( File storeDir, Map<String, String> params,
                              GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        this( storeDir, params, dependencies, new HazelcastDiscoveryServiceFactory() );
    }

    @Override
    public void init( PlatformModule platformModule, EditionModule editionModule, DataSourceModule dataSourceModule )
    {
        super.init( platformModule, editionModule, dataSourceModule );
        this.coreEditionModule = (EnterpriseCoreEditionModule) editionModule;
    }

    public Role getRole()
    {
        return coreEditionModule.raft().currentRole();
    }
}
