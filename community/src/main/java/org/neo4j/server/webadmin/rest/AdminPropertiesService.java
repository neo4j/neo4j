package org.neo4j.server.webadmin.rest;
/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import org.apache.commons.configuration.Configuration;
import org.neo4j.server.rest.domain.renderers.JsonRenderers;
import org.neo4j.server.webadmin.rest.representations.AdminPropertyRepresentation;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("/properties")
public class AdminPropertiesService
{
    private final String configurationNamespace = "org.neo4j.server.webadmin.";
    private Configuration config;
    private final UriInfo uriInfo;
    private final String MANAGEMENT_URI_KEY = "management.uri";
    private final String DATA_URI_KEY = "data.uri";

    public AdminPropertiesService( @Context UriInfo uriInfo,
                                   @Context Configuration config )
    {
        this.uriInfo = uriInfo;
        this.config = config;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{key}")
    public Response getValue( @PathParam("key") String key )
    {
        String lowerCaseKey = key.toLowerCase();
        String value = null;

        if ( "neo4j-servers".equals( lowerCaseKey ) )
        {
	        AdminPropertyRepresentation representation = new AdminPropertyRepresentation( uriInfo.getBaseUri().toString() );

	        representation.addUrl( "url", getDataUri() );
	        representation.addUrl( "manageUrl", getManagementUri() );

	        return Response.ok( JsonRenderers.DEFAULT.render( representation ) ).
			        type( MediaType.APPLICATION_JSON ).
			        build();
        }
        else if ( DATA_URI_KEY.equals( lowerCaseKey ) )
        {
            value = getDataUri();
        }
        else if ( MANAGEMENT_URI_KEY.equals( lowerCaseKey ) )
        {
            value = getManagementUri();
        }
        else
        {
            value = config.getString( configurationNamespace + key );
        }

        if ( value == null )
        {
            value = "undefined";
        }
        return Response.ok( value ).type( MediaType.APPLICATION_JSON ).build();
    }

    private String getDataUri()
    {
        String dataUri = slashTerminatedUri( config.getString( configurationNamespace + DATA_URI_KEY ) );
        if ( dataUri != null )
            return dataUri;
        else
            return hostPath("/db/data/");

    }

    private String getManagementUri()
    {
        String managementUri = slashTerminatedUri( config.getString( configurationNamespace + MANAGEMENT_URI_KEY ) );
        if ( managementUri != null )
            return managementUri;
        else
            return hostPath("/db/manage/");
    }

    private String hostPath(String path) {
	    return uriInfo.getBaseUriBuilder().replacePath( path ).build( ).toString();
    }

    private String slashTerminatedUri( String uri )
    {
        if (uri == null) return null;
        if ( !uri.endsWith( "/" ) )
        {
            return uri + "/";
        }
        return uri;
    }
}
