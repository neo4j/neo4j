/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.rest.web;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.cypher.CypherException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.CypherResultRepresentation;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.InvalidArgumentsException;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.transactional.CommitOnSuccessfulStatusCodeRepresentationWriteHandler;

@Path("/cypher")
public class CypherService
{

    private static final String PARAMS_KEY = "params";
    private static final String QUERY_KEY = "query";

    private static final String INCLUDE_STATS_PARAM = "includeStats";
    private static final String INCLUDE_PLAN_PARAM = "includePlan";
    private static final String PROFILE_PARAM = "profile";
    private final GraphDatabaseService database;

    private final CypherExecutor cypherExecutor;
    private final OutputFormat output;
    private final InputFormat input;

    public CypherService( @Context CypherExecutor cypherExecutor, @Context InputFormat input,
                          @Context OutputFormat output, @Context GraphDatabaseService database )
    {
        this.cypherExecutor = cypherExecutor;
        this.input = input;
        this.output = output;
        this.database = database;
    }

    public OutputFormat getOutputFormat()
    {
        return output;
    }

    @POST
    @SuppressWarnings({"unchecked", "ParameterCanBeLocal"})
    public Response cypher(String body,
                           @Context HttpServletRequest request,
                           @QueryParam( INCLUDE_STATS_PARAM ) boolean includeStats,
                           @QueryParam( INCLUDE_PLAN_PARAM ) boolean includePlan,
                           @QueryParam( PROFILE_PARAM ) boolean profile) throws BadInputException {

        Map<String,Object> command = input.readMap( body );

        if( !command.containsKey(QUERY_KEY) ) {
            return output.badRequest( new InvalidArgumentsException( "You have to provide the 'query' parameter." ) );
        }

        String query = (String) command.get( QUERY_KEY );
        Map<String, Object> params;
        try
        {
            params = (Map<String, Object>) (command.containsKey( PARAMS_KEY ) && command.get( PARAMS_KEY ) != null ?
                    command.get( PARAMS_KEY ) :
                    new HashMap<String, Object>());
        }
        catch ( ClassCastException e )
        {
            return output.badRequest( new IllegalArgumentException("Parameters must be a JSON map") );
        }
        try
        {
            QueryExecutionEngine executionEngine = cypherExecutor.getExecutionEngine();
            boolean periodicCommitQuery = executionEngine.isPeriodicCommit( query );
            CommitOnSuccessfulStatusCodeRepresentationWriteHandler handler = (CommitOnSuccessfulStatusCodeRepresentationWriteHandler) this.output.getRepresentationWriteHandler();
            if ( periodicCommitQuery )
            {
                handler.closeTransaction();
            }

            Result result;
            if ( profile )
            {
                result = executionEngine.profileQuery( query, params, new ServerQuerySession( request ) );
                includePlan = true;
            }
            else
            {
                result = executionEngine.executeQuery( query, params, new ServerQuerySession( request ) );
                includePlan = result.getQueryExecutionType().requestedExecutionPlanDescription();
            }

            if ( periodicCommitQuery )
            {
                handler.setTransaction( database.beginTx() );
            }

            return output.ok( new CypherResultRepresentation( result, includeStats, includePlan ) );
        }
        catch ( Throwable e )
        {
            if (e.getCause() instanceof CypherException)
            {
                return output.badRequest( e.getCause() );
            } else
            {
                return output.badRequest( e );
            }
        }
    }
}
