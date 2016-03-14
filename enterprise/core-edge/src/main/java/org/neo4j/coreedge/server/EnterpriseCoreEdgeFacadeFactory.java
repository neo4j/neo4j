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
package org.neo4j.coreedge.server;

import java.io.File;
import java.util.Map;

import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;

/**
 * This facade creates instances of the Enterprise Core-Edge edition of Neo4j.
 */
abstract class EnterpriseCoreEdgeFacadeFactory extends GraphDatabaseFacadeFactory
{
    @Override
    public GraphDatabaseFacade initFacade( File storeDir, Map<String, String> params, Dependencies dependencies,
                                          GraphDatabaseFacade graphDatabaseFacade )
    {
        return super.initFacade( storeDir, params, dependencies, graphDatabaseFacade );
    }

    protected void makeHazelcastQuiet( PlatformModule platformModule )
    {
        // Make hazelcast quiet for core and edge servers
        if ( platformModule.config.get( CoreEdgeClusterSettings.disable_middleware_logging ) )
        {
            // This is clunky, but the documented programmatic way doesn't seem to work
            System.setProperty( "hazelcast.logging.type", "none" );
        }
    }
}
