/**
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
package org.neo4j.server.webadmin.rest;

import java.net.URI;
import java.util.List;

import org.apache.commons.configuration.Configuration;

import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.JAXRSHelper.listFrom;

public class MasterInfoServerModule implements ServerModule
{
    private final WebServer server;
    private final Configuration config;
    private final ConsoleLogger log;

    public MasterInfoServerModule( WebServer server, Configuration config, Logging logging )
    {
        this.server = server;
        this.config = config;
        this.log = logging.getConsoleLog( getClass() );
    }

    @Override
    public void start()
    {
        URI baseUri = managementApiUri();
        server.addJAXRSClasses( getClassNames(), baseUri.toString(), null );

        log.log( "Mounted REST API at: " + baseUri.toString() );
    }

    @Override
    public void stop()
    {
        URI baseUri = managementApiUri();
        server.removeJAXRSClasses( getClassNames(), baseUri.toString() );
    }

    private List<String> getClassNames()
    {
        return listFrom( MasterInfoService.class.getName() );
    }

    private URI managementApiUri()
    {
        return URI.create( config.getString( Configurator.MANAGEMENT_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_MANAGEMENT_API_PATH ) );
    }
}
