/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest;

import java.net.URI;
import java.util.List;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.causalclustering.CausalClusteringService;
import org.neo4j.server.web.WebServer;

import static java.util.Arrays.asList;

public class DatabaseRoleInfoServerModule implements ServerModule
{
    private final WebServer server;
    private final Config config;
    private final Log log;

    public DatabaseRoleInfoServerModule( WebServer server, Config config, LogProvider logProvider )
    {
        this.server = server;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start()
    {
        URI baseUri = managementApiUri();
        server.addJAXRSClasses( getClassNames(), baseUri.toString(), null );

        log.info( "Mounted REST API at: %s", baseUri.toString() );
    }

    @Override
    public void stop()
    {
        URI baseUri = managementApiUri();
        server.removeJAXRSClasses( getClassNames(), baseUri.toString() );
    }

    private List<String> getClassNames()
    {
        return asList(
                MasterInfoService.class.getName(),
                CoreDatabaseAvailabilityService.class.getName(),
                ReadReplicaDatabaseAvailabilityService.class.getName(),
                CausalClusteringService.class.getName()
        );
    }

    private URI managementApiUri()
    {
        return config.get( ServerSettings.management_api_path );
    }
}
