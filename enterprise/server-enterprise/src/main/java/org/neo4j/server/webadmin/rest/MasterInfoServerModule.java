/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.net.UnknownHostException;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.JAXRSHelper.listFrom;

public class MasterInfoServerModule implements ServerModule
{
    private static final Logger log = Logger.getLogger( MasterInfoServerModule.class );
    private final WebServer server;
    private final Configuration config;

    public MasterInfoServerModule( WebServer server, Configuration config )
    {
        this.server = server;
        this.config = config;
    }

    @Override
    public void start( StringLogger logger )
    {
        try
        {
            URI baseUri = managementApiUri();
            server.addJAXRSClasses( getClassNames(), baseUri.toString(), null );

            log.info( "Mounted REST API at: " + baseUri.toString() );
            if ( logger != null )
            {
                logger.logMessage( "Mounted REST API at: " + baseUri.toString() );
            }
        }
        catch ( UnknownHostException e )
        {
            log.warn( e );
        }
    }

    @Override
    public void stop()
    {
        try
        {
            URI baseUri = managementApiUri();
            server.removeJAXRSClasses( getClassNames(), baseUri.toString() );
        }
        catch ( UnknownHostException e )
        {
            log.warn( e );
        }
    }

    private List<String> getClassNames()
    {
        return listFrom( MasterInfoService.class.getName() );
    }

    private URI managementApiUri( ) throws UnknownHostException
    {
        return URI.create( config.getString( Configurator.MANAGEMENT_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_MANAGEMENT_API_PATH ) );
    }
}
