/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.database;

import java.io.File;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.EnterpriseDiscoveryServiceFactorySelector;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.EnterpriseGraphDatabase;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;

public class EnterpriseGraphFactory implements GraphFactory
{
    @Override
    public GraphDatabaseFacade newGraphDatabase( Config config, Dependencies dependencies )
    {
        EnterpriseEditionSettings.Mode mode = config.get( EnterpriseEditionSettings.mode );
        File storeDir = config.get( GraphDatabaseSettings.databases_root_path );
        DiscoveryServiceFactory discoveryServiceFactory = new EnterpriseDiscoveryServiceFactorySelector().select( config );

        switch ( mode )
        {
        case HA:
            return new HighlyAvailableGraphDatabase( storeDir, config, dependencies );
        case ARBITER:
            // Should never reach here because this mode is handled separately by the scripts.
            throw new IllegalArgumentException( "The server cannot be started in ARBITER mode." );
        case CORE:
            return new CoreGraphDatabase( storeDir, config, dependencies, discoveryServiceFactory );
        case READ_REPLICA:
            return new ReadReplicaGraphDatabase( storeDir, config, dependencies, discoveryServiceFactory );
        default:
            return new EnterpriseGraphDatabase( storeDir, config, dependencies );
        }
    }
}
