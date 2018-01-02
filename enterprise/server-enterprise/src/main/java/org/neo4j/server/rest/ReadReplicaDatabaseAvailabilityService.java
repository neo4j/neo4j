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
