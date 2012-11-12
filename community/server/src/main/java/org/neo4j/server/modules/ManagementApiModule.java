/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.modules;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.logging.Logger;

import static org.neo4j.server.JAXRSHelper.listFrom;

public class ManagementApiModule implements ServerModule
{
    private final Logger log = Logger.getLogger( ManagementApiModule.class );

    public void start( NeoServerWithEmbeddedWebServer neoServer, StringLogger logger )
    {
        try
        {
            neoServer.getWebServer()
                    .addJAXRSPackages( listFrom( new String[] { Configurator.MANAGEMENT_API_PACKAGE } ),
                            managementApiUri( neoServer ).toString() );
            log.info( "Mounted management API at [%s]", managementApiUri( neoServer ).toString() );
            if ( logger != null )
                logger.logMessage( "Mounted management API at: " + managementApiUri( neoServer ).toString() );
        }
        catch ( UnknownHostException e )
        {
            log.warn( e );
        }
    }

    public void stop()
    {
        // Do nothing.
    }

    private URI managementApiUri( NeoServerWithEmbeddedWebServer neoServer ) throws UnknownHostException
    {
        try
        {
            return new URI( neoServer.getConfiguration()
                    .getString( Configurator.MANAGEMENT_PATH_PROPERTY_KEY, Configurator.DEFAULT_MANAGEMENT_API_PATH ) );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }
}
