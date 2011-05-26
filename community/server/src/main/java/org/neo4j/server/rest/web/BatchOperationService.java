/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.Cookie;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.mortbay.jetty.HttpURI;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.BatchOperationRepresentation;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.web.WebServer;

@Path( "/batch" )
public class BatchOperationService
{

    private static final String ID_KEY = "id";
    private static final String METHOD_KEY = "method";
    private static final String BODY_KEY = "body";
    private static final String TO_KEY = "to";
    private final OutputFormat output;
    private final InputFormat input;
    private final WebServer webServer;
    private final Database database;

    public BatchOperationService( @Context Database database,
            @Context WebServer webServer, @Context InputFormat input,
            @Context OutputFormat output )
    {
        this.input = input;
        this.output = output;
        this.webServer = webServer;
        this.database = database;
    }

    @POST
    @SuppressWarnings( "unchecked" )
    public Response performBatchOperations( @Context UriInfo uriInfo,
            String body )
    {

        AbstractGraphDatabase db = database.graph;
        Response response;

        Transaction tx = db.beginTx();
        try
        {

            List<Object> operations = input.readList( body );
            List<BatchOperationRepresentation> results = new ArrayList<BatchOperationRepresentation>(
                    operations.size() );

            InternalJettyServletRequest req = new InternalJettyServletRequest();
            InternalJettyServletResponse res = new InternalJettyServletResponse();

            String servletBaseUrl = uriInfo.getBaseUri().toString();
            String servletPath = uriInfo.getBaseUri().getPath();
            servletBaseUrl = servletBaseUrl.substring( 0,
                    servletBaseUrl.length() - 1 );

            String opBody, opMethod, opPath;
            Integer opId;
            Map<String, Object> op;
            for ( Object rawOperation : operations )
            {
                op = (Map<String, Object>) rawOperation;

                opMethod = (String) op.get( METHOD_KEY );
                opPath = (String) op.get( TO_KEY );
                opBody = op.containsKey( BODY_KEY ) ? JsonHelper.createJsonFrom( op.get( BODY_KEY ) )
                        : "";
                opId = op.containsKey( ID_KEY ) ? (Integer) op.get( ID_KEY )
                        : null;
                
                if(!opPath.startsWith( "/" )) {
                   opPath = "/" + opPath;
                }

                req.setup( opMethod, new HttpURI( servletBaseUrl + opPath ),
                        opBody, // Request body
                        new Cookie[] {} );
                res.setup();

                webServer.handle( servletPath + opPath, req, res );
                
                if(is2XXStatusCode(res.getStatus())) {
                    results.add( new BatchOperationRepresentation(
                            opId,
                            opPath,
                            res.getOutputStream().toString(),
                            res.getHeaders() ) );
                } else {
                    tx.failure();
                    return output.badRequest( new RuntimeException(res.getReason()) );
                }
            }

            tx.success();
            
            return output.ok( BatchOperationRepresentation.list( results ) );
        }
        catch ( Exception e )
        {
            tx.failure();
            return output.badRequest( e );
        }
        finally
        {
            tx.finish();
        }
    }
    
    private boolean is2XXStatusCode(int statusCode) {
        return statusCode - 200 >= 0 && statusCode - 200 < 100;
    }
}
