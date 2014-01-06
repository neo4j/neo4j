/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.rest.discovery;

import java.net.URI;
import java.net.URISyntaxException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.configuration.Configuration;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.rest.repr.DiscoveryRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;

/**
 * Used to discover the rest of the server URIs through a HTTP GET request to
 * the server root (/).
 */
@Path( "/" )
public class DiscoveryService
{

    private static final Logger LOGGER = Log.getLogger(DiscoveryService.class);
    private final Configuration configuration;
    private final OutputFormat outputFormat;

    public DiscoveryService( @Context Configuration configuration, @Context OutputFormat outputFormat )
    {
        this.configuration = configuration;
        this.outputFormat = outputFormat;
    }

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    public Response getDiscoveryDocument() throws URISyntaxException
    {
        String webAdminManagementUri = configuration.getString( Configurator.MANAGEMENT_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_MANAGEMENT_API_PATH );
        String dataUri = configuration.getString( Configurator.REST_API_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_DATA_API_PATH );

        DiscoveryRepresentation dr = new DiscoveryRepresentation( webAdminManagementUri, dataUri );
        return outputFormat.ok( dr );
    }

    @GET
    @Produces( MediaType.WILDCARD )
    public Response redirectToBrowser()
    {
        try
        {
            return Response.seeOther( new URI( Configurator.BROWSER_PATH ) )
                    .build();
        }
        catch ( URISyntaxException e )
        {
            LOGGER.warn( e.getMessage() );
            return Response.serverError()
                    .build();
        }
    }
}
