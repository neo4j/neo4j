/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.io.InputStream;
import java.io.StringWriter;
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
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.domain.BatchOperationFailedException;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.BatchOperationResults;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.web.WebServer;

@Path( "/batch" )
public class BatchOperationService
{

    private static final String ID_KEY = "id";
    private static final String METHOD_KEY = "method";
    private static final String BODY_KEY = "body";
    private static final String TO_KEY = "to";

    private static final JsonFactory jsonFactory = new JsonFactory();

    private final OutputFormat output;
    private final WebServer webServer;
    private final Database database;

    public BatchOperationService(@Context Database database, @Context WebServer webServer, @Context OutputFormat output)
    {
        this.output = output;
        this.webServer = webServer;
        this.database = database;
    }

    @POST
    public Response performBatchOperations( @Context UriInfo uriInfo, @Context HttpHeaders httpHeaders,
                                            InputStream body ) throws BadInputException
    {
        GraphDatabaseAPI db = database.graph;

        Transaction tx = db.beginTx();
        try
        {
            JsonParser jp = jsonFactory.createJsonParser(body);
            ObjectMapper mapper = new ObjectMapper();

            BatchOperationResults results = new BatchOperationResults();

            JsonToken token;
            String field;
            String jobMethod, jobPath, jobBody;
            Integer jobId;

            // TODO: Perhaps introduce a simple DSL for
            // deserializing streamed JSON?
            while( (token = jp.nextToken()) != null) {
                 if(token == JsonToken.START_OBJECT) {
                     jobMethod = jobPath = jobBody = "";
                     jobId = null;
                     while( (token = jp.nextToken()) != JsonToken.END_OBJECT && token != null) {
                         field = jp.getText();
                         token = jp.nextToken();
                         if(field.equals(METHOD_KEY)) {
                             jobMethod = jp.getText().toUpperCase();
                         } else if(field.equals(TO_KEY)) {
                             jobPath = jp.getText();
                         } else if(field.equals(ID_KEY)) {
                             jobId = jp.getIntValue();
                         } else if(field.equals(BODY_KEY)) {
                            JsonNode node = mapper.readTree(jp);
                            StringWriter out = new StringWriter();
                            JsonGenerator gen = jsonFactory.createJsonGenerator(out);
                            mapper.writeTree(gen, node);
                            gen.flush();
                            gen.close();
                            jobBody = out.toString();
                         }
                     }

                     // Read one job description. Execute it.
                     performJob(results, uriInfo, jobMethod, jobPath, jobBody, jobId, httpHeaders);
                 }
            }

            Response res = Response.ok()
                    .entity( results.toJSON() )
                    .header( HttpHeaders.CONTENT_ENCODING, "UTF-8" )
                    .type( MediaType.APPLICATION_JSON )
                    .build();

            tx.success();
            return res;
        }
        catch ( Exception e )
        {
            tx.failure();
            return output.serverError( e );
        }
        finally
        {
            tx.finish();
        }
    }

    private void performJob( BatchOperationResults results, UriInfo uriInfo, String method, String path, String body,
                             Integer id, HttpHeaders httpHeaders )
            throws IOException, ServletException
    {
        // Replace {[ID]} placeholders with location values
        Map<Integer, String> locations = results.getLocations();
        path = replaceLocationPlaceholders( path, locations );
        body = replaceLocationPlaceholders( body, locations );

        URI targetUri = calculateTargetUri( uriInfo, path );

        InternalJettyServletRequest req = new InternalJettyServletRequest(method, targetUri.toString(), body );
        InternalJettyServletResponse res = new InternalJettyServletResponse();
        addHeaders(req, httpHeaders);

        webServer.invokeDirectly( targetUri.getPath(), req, res );

        if ( is2XXStatusCode( res.getStatus() ) )
        {
            results.addOperationResult( path, id, res.getOutputStream()
                    .toString(), res.getHeader( "Location" ) );
        }
        else
        {
            throw new BatchOperationFailedException( res.getStatus(), res.getOutputStream()
                    .toString() );
        }
    }

    private void addHeaders(final InternalJettyServletRequest res, final HttpHeaders httpHeaders)
    {
        for ( Map.Entry<String, List<String>> header : httpHeaders.getRequestHeaders().entrySet() )
        {
            final String key = header.getKey();
            final List<String> value = header.getValue();
            if ( value == null ) continue;
            if ( value.size() != 1 )
                throw new IllegalArgumentException(
                        "expecting one value per header" );
            if ( key.equals( "Accept" ) || key.equals( "Content-Type" ) )
            {
                continue; // We add them explicitly
            }
            else
            {
                res.addHeader( key, value.get( 0 ) );
            }
        }
        // Make sure they are there and always json
        // Taking advantage of Map semantics here
        res.addHeader( "Accept", "application/json" );
        res.addHeader( "Content-Type", "application/json" );
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
        return statusCode >= 200 && statusCode < 300;
    }
}
