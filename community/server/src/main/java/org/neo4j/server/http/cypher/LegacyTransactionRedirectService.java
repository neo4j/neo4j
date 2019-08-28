/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.http.cypher;

import java.net.URI;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.DatabaseService;

import static org.neo4j.server.http.cypher.CypherResource.TRANSACTION;

@Path( "/transaction" )
public class LegacyTransactionRedirectService
{
    private final Config config;
    private final DatabaseService databaseService;

    public LegacyTransactionRedirectService( @Context DatabaseService databaseService, @Context Config config )
    {
        this.config = config;
        this.databaseService = databaseService;
    }

    @POST
    public Response redirect()
    {
        var newLocation = transactionPathForDefaultDatabase();
        return Response.temporaryRedirect( URI.create( newLocation ) ).build();
    }

    @POST
    @Path( "/{id}" )
    public Response redirectStatements( @PathParam( "id" ) long id )
    {
        var newLocation = String.format( "%s/%s", transactionPathForDefaultDatabase(), id );
        return Response.temporaryRedirect( URI.create( newLocation ) ).build();
    }

    @POST
    @Path( "/{id}/commit" )
    public Response redirectCommitTransaction( @PathParam( "id" ) long id )
    {
        var newLocation = String.format( "%s/%s/commit", transactionPathForDefaultDatabase(), id );
        return Response.temporaryRedirect( URI.create( newLocation ) ).build();
    }

    @DELETE
    @Path( "/{id}" )
    public Response redirectRollbackTransaction( @PathParam( "id" ) long id )
    {
        var newLocation = String.format( "%s/%s", transactionPathForDefaultDatabase(), id );
        return Response.temporaryRedirect( URI.create( newLocation ) ).build();
    }

    @POST
    @Path( "/commit" )
    public Response redirectCommitNewTransaction()
    {
        var newLocation = String.format( "%s/commit", transactionPathForDefaultDatabase() );
        return Response.temporaryRedirect( URI.create( newLocation ) ).build();
    }

    private String transactionPathForDefaultDatabase()
    {
        var defaultDatabaseName = databaseService.getDatabase().databaseName();
        return String.format( "%s/%s/%s", config.get( ServerSettings.db_api_path ).getPath(), defaultDatabaseName, TRANSACTION );
    }
}
