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

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.BatchOperationResults;
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

    public BatchOperationService( @Context Database database, @Context WebServer webServer, @Context InputFormat input,
            @Context OutputFormat output )
    {
        this.input = input;
        this.output = output;
        this.webServer = webServer;
        this.database = database;
    }

    @POST
    @SuppressWarnings( "unchecked" )
    public Response performBatchOperations( @Context UriInfo uriInfo, String body )
    {

        AbstractGraphDatabase db = database.graph;

        Transaction tx = db.beginTx();
        try
        {

            List<Object> operations = input.readList( body );
            BatchOperationResults results = new BatchOperationResults( operations.size() );

            for ( Object rawOperation : operations )
            {
                performJob( results, uriInfo, (Map<String, Object>) rawOperation );
            }

            tx.success();

            return Response.ok()
                    .entity( results.toJSON()
                            .getBytes( "UTF-8" ) )
                    .header( HttpHeaders.CONTENT_ENCODING, "UTF-8" )
                    .type( MediaType.APPLICATION_JSON )
                    .build();
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

    private void performJob( BatchOperationResults results, UriInfo uriInfo, Map<String, Object> desc )
            throws IOException, ServletException
    {

        InternalJettyServletRequest req = new InternalJettyServletRequest();
        InternalJettyServletResponse res = new InternalJettyServletResponse();

        String method = (String) desc.get( METHOD_KEY );
        String path = (String) desc.get( TO_KEY );
        String body = desc.containsKey( BODY_KEY ) ? JsonHelper.createJsonFrom( desc.get( BODY_KEY ) ) : "";
        Integer id = desc.containsKey( ID_KEY ) ? (Integer) desc.get( ID_KEY ) : null;

        // Replace {[ID]} placeholders with location values
        Map<Integer, String> locations = results.getLocations();
        path = replaceLocationPlaceholders( path, locations );
        body = replaceLocationPlaceholders( body, locations );

        URI targetUri = calculateTargetUri( uriInfo, path );

        req.setup( method, targetUri.toString(), body );
        res.setup();

        webServer.invokeDirectly( targetUri.getPath(), req, res );

        if ( is2XXStatusCode( res.getStatus() ) )
        {
            results.addOperationResult( path, id, res.getOutputStream()
                    .toString(), res.getHeader( "Location" ) );
        }
        else
        {
            throw new RuntimeException( res.getReason() );
        }
    }

    private URI calculateTargetUri( UriInfo serverUriInfo, String requestedPath )
    {
        URI baseUri = serverUriInfo.getBaseUri();

        if ( requestedPath.startsWith( baseUri.toString() ) )
        {
            requestedPath = requestedPath.substring( baseUri.toString()
                    .length() );
        }

        if ( !requestedPath.startsWith( "/" ) )
        {
            requestedPath = "/" + requestedPath;
        }

        return baseUri.resolve( "." + requestedPath );
    }

    private String replaceLocationPlaceholders( String str, Map<Integer, String> locations )
    {
        // TODO: Potential memory-hog on big requests, write smarter
        // implementation.
        for ( Integer jobId : locations.keySet() )
        {
            str = str.replace( "{" + jobId + "}", locations.get( jobId ) );
        }
        return str;
    }

    private boolean is2XXStatusCode( int statusCode )
    {
        return statusCode - 200 >= 0 && statusCode - 200 < 100;
    }
}
