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

package org.neo4j.server.webadmin.rest;

import org.apache.commons.configuration.Configuration;
import org.neo4j.rest.domain.JsonRenderers;
import org.neo4j.server.NeoServer;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;

import static org.neo4j.server.webadmin.rest.WebUtils.addHeaders;

/**
 * A simple key/value store for handling preferences in the admin interface.
 *
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 */
@Path( AdminPropertiesService.ROOT_PATH )
public class AdminPropertiesService
{

    public static final String ROOT_PATH = "/properties";

    public static final String PROPERTY_PATH = "/{key}";

    /**
     * Get settings file, creating one if it does not exist.
     *
     * @return
     * @throws IOException
     */
    public static File getPropertiesFile() throws IOException
    {
        File settingsDirectory = new File( "../conf/" );
        if ( !settingsDirectory.exists() )
        {
            if ( !settingsDirectory.mkdir() )
            {
                throw new IllegalStateException( settingsDirectory.toString() );
            }
        }

        // Make sure settings file exists
        File settingsFile = new File( settingsDirectory, "client.conf" );

        if ( !settingsFile.exists() && !settingsFile.createNewFile() )
        {
            throw new IllegalStateException( settingsFile.toString() );
        }

        return settingsFile;
    }

    //
    // PUBLIC
    //


    @GET
    @Produces( MediaType.APPLICATION_JSON )
    @Path( PROPERTY_PATH )
    public Response getValue( @PathParam( "key" ) String key )
    {
        String value = getConfiguration().getString( NeoServer.WEBADMIN_NAMESPACE + key );

        if ( value == null )
        {
            return addHeaders(
                    Response.ok( "undefined",
                            JsonRenderers.DEFAULT.getMediaType() ) ).build();
        }

        return addHeaders(
                Response.ok( value, JsonRenderers.DEFAULT.getMediaType() ) ).build();
    }

    private Configuration getConfiguration()
    {
        return NeoServer.INSTANCE.configuration();
    }

    @POST
    @Produces( MediaType.APPLICATION_JSON )
    @Consumes( MediaType.APPLICATION_JSON )
    @Path( PROPERTY_PATH )
    public Response jsonSetValue( @PathParam( "key" ) String key, String value )
    {
        return setValue( key, value );
    }

    @POST
    @Produces( MediaType.APPLICATION_JSON )
    @Consumes( MediaType.APPLICATION_FORM_URLENCODED )
    @Path( PROPERTY_PATH )
    public Response formSetValueJSON( @PathParam( "key" ) String key,
                                      @FormParam( "value" ) String value )
    {
        return setValue( key, value );
    }

    //
    // INTERNALS
    //

    private synchronized Response setValue( String key, String value )
    {
        getConfiguration().addProperty( NeoServer.WEBADMIN_NAMESPACE + key, value );

        return Response.ok().build();
    }
}
