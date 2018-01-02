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
package org.neo4j.server.webadmin.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.status;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Path(MasterInfoService.BASE_PATH)
public class MasterInfoService implements AdvertisableService
{
    public static final String BASE_PATH = "server/ha";
    public static final String IS_MASTER_PATH = "/master";
    public static final String IS_SLAVE_PATH = "/slave";
    public static final String IS_AVAILABLE_PATH = "/available";

    private final OutputFormat output;
    private final HighlyAvailableGraphDatabase haDb;

    public MasterInfoService( @Context OutputFormat output, @Context GraphDatabaseService db )
    {
        this.output = output;
        if ( db instanceof HighlyAvailableGraphDatabase )
        {
            this.haDb = (HighlyAvailableGraphDatabase) db;
        }
        else
        {
            this.haDb = null;
        }
    }

    @GET
    public Response discover() throws BadInputException
    {
        if ( haDb == null )
        {
            return status( FORBIDDEN ).build();
        }

        String isMasterUri = IS_MASTER_PATH;
        String isSlaveUri = IS_SLAVE_PATH;

        HaDiscoveryRepresentation dr = new HaDiscoveryRepresentation( BASE_PATH, isMasterUri, isSlaveUri );
        return output.ok( dr );
    }

    @GET
    @Path(IS_MASTER_PATH)
    public Response isMaster() throws BadInputException
    {
        if ( haDb == null )
        {
            return status( FORBIDDEN ).build();
        }

        String role = haDb.role().toLowerCase();
        if ( role.equals( "master" ))
        {
            return positiveResponse();
        }

        if ( role.equals( "slave" ))
        {
            return negativeResponse();
        }

        return unknownResponse();
    }

    @GET
    @Path(IS_SLAVE_PATH)
    public Response isSlave() throws BadInputException
    {
        if ( haDb == null )
        {
            return status( FORBIDDEN ).build();
        }

        String role = haDb.role().toLowerCase();
        if ( role.equals( "slave" ))
        {
            return positiveResponse();
        }

        if ( role.equals( "master" ))
        {
            return negativeResponse();
        }

        return unknownResponse();
    }

    @GET
    @Path( IS_AVAILABLE_PATH )
    public Response isAvailable()
    {
        if ( haDb == null )
        {
            return status( FORBIDDEN ).build();
        }

        String role = haDb.role().toLowerCase();
        if ( "slave".equals( role ) || "master".equals( role ))
        {
            return plainTextResponse( OK, role );
        }

        return unknownResponse();
    }

    private Response negativeResponse()
    {
        return plainTextResponse( NOT_FOUND, "false" );
    }

    private Response positiveResponse()
    {
        return plainTextResponse( OK, "true" );
    }

    private Response unknownResponse()
    {
        return plainTextResponse( NOT_FOUND, "UNKNOWN" );
    }

    private Response plainTextResponse( Response.Status status, String entityBody )
    {
        return status( status ).type( TEXT_PLAIN_TYPE ).entity( entityBody ).build();
    }

    @Override
    public String getName()
    {
        return "ha";
    }

    @Override
    public String getServerPath()
    {
        return BASE_PATH;
    }
}
