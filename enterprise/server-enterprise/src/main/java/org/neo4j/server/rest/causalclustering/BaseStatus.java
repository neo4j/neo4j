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
package org.neo4j.server.rest.causalclustering;

import javax.ws.rs.core.Response;

import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.status;
import static org.neo4j.server.rest.causalclustering.CausalClusteringService.BASE_PATH;

abstract class BaseStatus implements CausalClusteringStatus
{
    private final OutputFormat output;

    BaseStatus( OutputFormat output )
    {
        this.output = output;
    }

    @Override
    public Response discover()
    {
        return output.ok( new CausalClusteringDiscovery( BASE_PATH ) );
    }

    Response positiveResponse()
    {
        return plainTextResponse( OK, "true" );
    }

    Response negativeResponse()
    {
        return plainTextResponse( NOT_FOUND, "false" );
    }

    private Response plainTextResponse( Response.Status status, String entityBody )
    {
        return status( status ).type( TEXT_PLAIN_TYPE ).entity( entityBody ).build();
    }
}
