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
import java.io.OutputStream;
import java.util.Map;

import javax.servlet.ServletOutputStream;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.mortbay.log.Log;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.batch.BatchOperationResults;
import org.neo4j.server.rest.batch.NonStreamingBatchOperations;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.StreamingJsonFormat;
import org.neo4j.server.web.WebServer;

@Path("/batch")
public class BatchOperationService {

    private final OutputFormat output;
    private final WebServer webServer;
    private final Database database;

    public BatchOperationService(@Context Database database,
            @Context WebServer webServer, @Context OutputFormat output)
    {
        this.output = output;
        this.webServer = webServer;
        this.database = database;
    }

    @POST
    public Response performBatchOperations(@Context UriInfo uriInfo,
            @Context HttpHeaders httpHeaders, InputStream body)
            throws BadInputException
    {
        if (isStreaming(httpHeaders)) {
            return batchProcessAndStream( uriInfo, httpHeaders, body );
        } else {
            return batchProcess( uriInfo, httpHeaders, body );
        }
    }

    private Response batchProcessAndStream( final UriInfo uriInfo, final HttpHeaders httpHeaders, final InputStream body )
    {
        try
        {
            final StreamingOutput stream = new StreamingOutput() {
                public void write(final OutputStream output) throws IOException, WebApplicationException
                {
                    Transaction tx = database.getGraph().beginTx();
                    try {
                        final ServletOutputStream servletOutputStream = new ServletOutputStream() {
                            public void write(int i) throws IOException {
                                output.write(i);
                            }
                        };
                        new StreamingBatchOperations(webServer).readAndExecuteOperations( uriInfo, httpHeaders, body, servletOutputStream );
                        tx.success();
                    } catch (Exception e) {
                        Log.warn( "Error executing batch request ", e );
                        tx.failure();
//                        if (e instanceof RuntimeException) throw (RuntimeException)e;
//                        throw new WebApplicationException(e);
                    } finally {
                        tx.finish();
                    }
                }
            };

            return Response.ok(stream)
                    .header( HttpHeaders.CONTENT_ENCODING, "UTF-8" )
                    .type( MediaType.APPLICATION_JSON ).build();
        } catch (Exception e)
        {
            return output.serverError(e);
        }
    }

    private Response batchProcess( UriInfo uriInfo, HttpHeaders httpHeaders, InputStream body )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            NonStreamingBatchOperations batchOperations = new NonStreamingBatchOperations( webServer );
            BatchOperationResults results = batchOperations.performBatchJobs( uriInfo, httpHeaders, body );

            Response res = Response.ok().entity(results.toJSON())
                    .header(HttpHeaders.CONTENT_ENCODING, "UTF-8")
                    .type( MediaType.APPLICATION_JSON).build();

            tx.success();
            return res;
        } catch (Exception e)
        {
            tx.failure();
            return output.serverError(e);
        } finally
        {
            tx.finish();
        }
    }

    private boolean isStreaming( HttpHeaders httpHeaders )
    {
        if ("true".equalsIgnoreCase(httpHeaders.getRequestHeaders().getFirst(StreamingJsonFormat.STREAM_HEADER)))
        {
            return true;
        }

        for ( MediaType mediaType : httpHeaders.getAcceptableMediaTypes() )
        {
            Map<String, String> parameters = mediaType.getParameters();
            if ( parameters.containsKey( "stream" ) && "true".equalsIgnoreCase(parameters.get("stream")))
            {
                return true;
            }
        }
        return false;
    }

}
