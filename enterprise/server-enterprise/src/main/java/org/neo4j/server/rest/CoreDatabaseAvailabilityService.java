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

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.status;

/**
 * To be deprecated by {@link org.neo4j.server.rest.causalclustering.CausalClusteringService}.
 */
@Path( CoreDatabaseAvailabilityService.BASE_PATH )
public class CoreDatabaseAvailabilityService implements AdvertisableService
{
    public static final String BASE_PATH = "server/core";
    public static final String IS_WRITABLE_PATH = "/writable";
    public static final String IS_AVAILABLE_PATH = "/available";
    public static final String IS_READ_ONLY_PATH = "/read-only";

    private final OutputFormat output;
    private final CoreGraphDatabase coreDatabase;

    public CoreDatabaseAvailabilityService( @Context OutputFormat output, @Context GraphDatabaseService db )
    {
        this.output = output;
        if ( db instanceof CoreGraphDatabase )
        {
            this.coreDatabase = (CoreGraphDatabase) db;
        }
        else
        {
            this.coreDatabase = null;
        }
    }

    @GET
    public Response discover()
    {
        if ( coreDatabase == null )
        {
            return status( FORBIDDEN ).build();
        }

        return output.ok( new CoreDatabaseAvailabilityDiscoveryRepresentation( BASE_PATH, IS_WRITABLE_PATH ) );
    }

    @GET
    @Path( IS_WRITABLE_PATH )
    public Response isWritable()
    {
        if ( coreDatabase == null )
        {
            return status( FORBIDDEN ).build();
        }

        if ( coreDatabase.getRole() == Role.LEADER )
        {
            return positiveResponse();
        }

        return negativeResponse();
    }

    @GET
    @Path( IS_READ_ONLY_PATH )
    public Response isReadOnly()
    {
        if ( coreDatabase == null )
        {
            return status( FORBIDDEN ).build();
        }

        if ( coreDatabase.getRole() == Role.FOLLOWER || coreDatabase.getRole() == Role.CANDIDATE )
        {
            return positiveResponse();
        }

        return negativeResponse();
    }

    @GET
    @Path( IS_AVAILABLE_PATH )
    public Response isAvailable()
    {
        if ( coreDatabase == null )
        {
            return status( FORBIDDEN ).build();
        }

        return positiveResponse();
    }

    private Response negativeResponse()
    {
        return plainTextResponse( NOT_FOUND, "false" );
    }

    private Response positiveResponse()
    {
        return plainTextResponse( OK, "true" );
    }

    private Response plainTextResponse( Response.Status status, String entityBody )
    {
        return status( status ).type( TEXT_PLAIN_TYPE ).entity( entityBody ).build();
    }

    @Override
    public String getName()
    {
        return "core";
    }

    @Override
    public String getServerPath()
    {
        return BASE_PATH;
    }
}
