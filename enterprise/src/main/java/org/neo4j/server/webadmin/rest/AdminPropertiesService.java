package org.neo4j.server.webadmin.rest; /**
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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path( "/properties" )
public class AdminPropertiesService
{
    private final String configurationNamespace = "org.neo4j.server.webadmin.";
    private Configuration config;
    private final UriInfo uriInfo;

    public AdminPropertiesService( @Context UriInfo uriInfo,
                                   @Context Configuration config )
    {
        this.uriInfo = uriInfo;
        this.config = config;
    }

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    @Path( "/{key}" )
    public Response getValue( @PathParam( "key" ) String key )
    {
        // Legacy mapping for webadmin app
        if ( key.toLowerCase().equals( "neo4j-servers" ) )
        {
            StringBuilder sb = new StringBuilder();
            sb.append( "{ \"" );
            sb.append( uriInfo.getBaseUri() );
            sb.append( "\" : " );
            sb.append( "{\"url\" : \"" );
            sb.append( getConfigValue( "data.uri" ) );
            sb.append( "\"" );
            sb.append( "," );
            sb.append( "\"manageUrl\" : \"" );
            sb.append( getConfigValue( "management.uri" ) );
            sb.append( "\"}" );
            sb.append( "}" );

            return Response.ok( sb.toString() ).type( MediaType.APPLICATION_JSON ).build();
        }

        String value = config.getString( configurationNamespace + key );

        if ( value == null )
        {
            value = "undefined";
        }
        return Response.ok( value ).type( MediaType.APPLICATION_JSON ).build();
    }

    private String getConfigValue( String key )
    {
        String k = configurationNamespace + key;
        String value = config.getString( k );
        if(value==null)
        {
            throw new IllegalArgumentException( "Value of " + k + " was not found." );
        }
        return value;
    }
}
