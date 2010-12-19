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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.configuration.Configuration;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.ValueRepresentation;

@Path("/properties")
public class AdminPropertiesService
{
    private final String configurationNamespace = "org.neo4j.server.webadmin.";
    private final Configuration config;
    private final UriInfo uriInfo;
    private final String MANAGEMENT_URI_KEY = "management.uri";
    private final String DATA_URI_KEY = "data.uri";
    private final OutputFormat output;

    public AdminPropertiesService( @Context UriInfo uriInfo,
                                   @Context Configuration config,
                                   @Context OutputFormat output )
    {
        this.uriInfo = uriInfo;
        this.config = config;
        this.output = output;
    }

    @GET
    @Path("/{key}")
    public Response getValue( @PathParam("key") String key )
    {
        String lowerCaseKey = key.toLowerCase();
        String value;

        if ( "neo4j-servers".equals( lowerCaseKey ) )
        {
            return output.ok( new MappingRepresentation( "neo4j-servers" )
            {
                @Override
                protected void serialize( MappingSerializer serializer )
                {
                    serializer.putMapping( uriInfo.getBaseUri().toString(),
                            new MappingRepresentation( "urls" )
                            {
                                @Override
                                protected void serialize( MappingSerializer serializer )
                                {
                                    serializer.putString( "url", getDataUri() );
                                    serializer.putString( "manageUrl", getManagementUri() );
                                }
                            } );
                }
            } );
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
        return output.ok( ValueRepresentation.string( value ) );
    }

    private String getDataUri()
    {
        String dataUri = slashTerminatedUri( config.getString( configurationNamespace + DATA_URI_KEY ) );
        if ( dataUri != null )
            return dataUri;
        else
            return hostPath(Configurator.REST_API_PATH);

    }

    private String getManagementUri()
    {
        String managementUri = slashTerminatedUri( config.getString( configurationNamespace + MANAGEMENT_URI_KEY ) );
        if ( managementUri != null )
            return managementUri;
        else
            return hostPath(Configurator.WEB_ADMIN_REST_API_PATH);
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
