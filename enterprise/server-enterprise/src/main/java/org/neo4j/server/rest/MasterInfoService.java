/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.rest;

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
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.status;

@Path( MasterInfoService.BASE_PATH )
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
    public Response discover()
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
    @Path( IS_MASTER_PATH )
    public Response isMaster()
    {
        if ( haDb == null )
        {
            return status( FORBIDDEN ).build();
        }

        String role = haDb.role().toLowerCase();
        if ( role.equals( "master" ) )
        {
            return positiveResponse();
        }

        if ( role.equals( "slave" ) )
        {
            return negativeResponse();
        }

        return unknownResponse();
    }

    @GET
    @Path( IS_SLAVE_PATH )
    public Response isSlave()
    {
        if ( haDb == null )
        {
            return status( FORBIDDEN ).build();
        }

        String role = haDb.role().toLowerCase();
        if ( role.equals( "slave" ) )
        {
            return positiveResponse();
        }

        if ( role.equals( "master" ) )
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
        if ( "slave".equals( role ) || "master".equals( role ) )
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
