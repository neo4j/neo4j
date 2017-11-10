/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.rest.causalclustering;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.OutputFormat;

@Path( CausalClusteringService.BASE_PATH )
public class CausalClusteringService implements AdvertisableService
{
    static final String BASE_PATH = "server/causalclustering/";

    static final String AVAILABLE = "available";
    static final String WRITABLE = "writable";
    static final String READ_ONLY = "read-only";

    private final CausalClusteringStatus status;

    public CausalClusteringService( @Context OutputFormat output, @Context GraphDatabaseService db )
    {
        this.status = CausalClusteringStatusFactory.build( output, db );
    }

    @GET
    public Response discover() throws BadInputException
    {
        return status.discover();
    }

    @GET
    @Path( WRITABLE )
    public Response isWritable() throws BadInputException
    {
        return status.writable();
    }

    @GET
    @Path( READ_ONLY )
    public Response isReadOnly() throws BadInputException
    {
        return status.readonly();
    }

    @GET
    @Path( AVAILABLE )
    public Response isAvailable()
    {
        return status.available();
    }

    @Override
    public String getName()
    {
        return "causalclustering";
    }

    @Override
    public String getServerPath()
    {
        return BASE_PATH;
    }
}
