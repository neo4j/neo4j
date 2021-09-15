/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.Collections;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Variant;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.repr.DiscoveryRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;

/**
 * Used to discover the rest of the server URIs through a HTTP GET request to
 * the server root (/).
 */
@Path( "/" )
public class DiscoveryService
{
    private final Config config;
    private final OutputFormat outputFormat;
    private final DiscoverableURIs uris;
    private final Map<String,Object> varyHeader;

    // Your IDE might tell you to make this less visible than public. Don't. JAX-RS demands is to be public.
    public DiscoveryService( @Context Config config, @Context OutputFormat outputFormat, @Context DiscoverableURIs uris )
    {
        this.config = config;
        this.outputFormat = outputFormat;
        this.uris = uris;
        this.varyHeader = Collections.singletonMap( HttpHeaders.VARY, HttpHeaders.ACCEPT );
    }

    Response getDiscoveryDocument( UriInfo uriInfo )
    {
        return outputFormat.ok(
                new DiscoveryRepresentation( new DiscoverableURIs.Builder( uris ).overrideAbsolutesFromRequest( uriInfo.getBaseUri() ).build() ) );
    }

    @GET
    @Produces( MediaType.WILDCARD )
    public Response get( @Context Request request, @Context UriInfo uriInfo )
    {

        Variant v = request.selectVariant( Variant.mediaTypes( MediaType.APPLICATION_JSON_TYPE, MediaType.TEXT_HTML_TYPE ).add().build() );
        if ( v == null )
        {
            return Response.serverError().status( Response.Status.NOT_ACCEPTABLE ).build();
        }
        else if ( v.getMediaType() == MediaType.APPLICATION_JSON_TYPE )
        {
            return getDiscoveryDocument( uriInfo );
        }
        else
        {
            return outputFormat.seeOther( config.get( ServerSettings.browser_path ), varyHeader );
        }
    }
}
