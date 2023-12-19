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

import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.status;

/**
 * To be deprecated by {@link org.neo4j.server.rest.causalclustering.CausalClusteringService}.
 */
@Path( ReadReplicaDatabaseAvailabilityService.BASE_PATH )
public class ReadReplicaDatabaseAvailabilityService implements AdvertisableService
{
    static final String BASE_PATH = "server/read-replica";
    private static final String IS_AVAILABLE_PATH = "/available";

    private final ReadReplicaGraphDatabase readReplica;

    public ReadReplicaDatabaseAvailabilityService( @Context OutputFormat output, @Context GraphDatabaseService db )
    {
        if ( db instanceof ReadReplicaGraphDatabase )
        {
            this.readReplica = (ReadReplicaGraphDatabase) db;
        }
        else
        {
            this.readReplica = null;
        }
    }

    @GET
    @Path( IS_AVAILABLE_PATH )
    public Response isAvailable()
    {
        if ( readReplica == null )
        {
            return status( FORBIDDEN ).build();
        }

        return positiveResponse();
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
        return "read-replica";
    }

    @Override
    public String getServerPath()
    {
        return BASE_PATH;
    }
}
