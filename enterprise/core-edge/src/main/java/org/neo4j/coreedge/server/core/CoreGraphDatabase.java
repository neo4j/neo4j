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
package org.neo4j.coreedge.server.core;

import java.io.File;
import java.util.Map;

import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.EnterpriseCoreFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

public class CoreGraphDatabase extends GraphDatabaseFacade
{
    private final CoreEditionSPI coreEditionSPI;

    public CoreGraphDatabase( File storeDir, Map<String, String> params,
                              GraphDatabaseFacadeFactory.Dependencies dependencies,
                              DiscoveryServiceFactory discoveryServiceFactory )
    {
        GraphDatabaseFacade coreGraphDatabaseFacade =
                new EnterpriseCoreFacadeFactory( discoveryServiceFactory ).initFacade( storeDir, params, dependencies, this );
        coreEditionSPI = (CoreEditionSPI) coreGraphDatabaseFacade.editionSPI();
    }

    public CoreGraphDatabase( File storeDir, Map<String, String> params,
                              GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        this( storeDir, params, dependencies, new HazelcastDiscoveryServiceFactory() );
    }

    public CoreMember id()
    {
        return coreEditionSPI.id();
    }

    public Role getRole()
    {
        return coreEditionSPI.currentRole();
    }

    public void downloadSnapshot( AdvertisedSocketAddress source )
    {
        coreEditionSPI.downloadSnapshot( source );
    }

    public void compact()
    {
        coreEditionSPI.compact();
    }
}
