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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.servlet.WriteListener;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.neo4j.server.rest.batch.BatchOperationResults;
import org.neo4j.server.rest.batch.NonStreamingBatchOperations;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.RepresentationWriteHandler;
import org.neo4j.server.rest.repr.StreamingFormat;
import org.neo4j.server.web.HttpHeaderUtils;
import org.neo4j.server.web.WebServer;

@Path("/batch")
public class BatchOperationService {

    private static final Logger LOGGER = Log.getLogger(BatchOperationService.class);

    private final OutputFormat output;
    private final WebServer webServer;
    private RepresentationWriteHandler representationWriteHandler = RepresentationWriteHandler.DO_NOTHING;

    public BatchOperationService( @Context WebServer webServer, @Context OutputFormat output )
    {
        this.output = output;
        this.webServer = webServer;
    }

    public void setRepresentationWriteHandler( RepresentationWriteHandler representationWriteHandler )
    {
        this.representationWriteHandler = representationWriteHandler;
    }

    @POST
    public Response performBatchOperations(@Context UriInfo uriInfo,
            @Context HttpHeaders httpHeaders, @Context HttpServletRequest req, InputStream body)
    {
        if ( isStreaming( httpHeaders ) )
        {
            return batchProcessAndStream( uriInfo, httpHeaders, req, body );
        }
        return batchProcess( uriInfo, httpHeaders, req, body );
    }

    private Response batchProcessAndStream( final UriInfo uriInfo, final HttpHeaders httpHeaders,
                                            final HttpServletRequest req, final InputStream body )
    {
        try
        {
            final StreamingOutput stream = new StreamingOutput()
            {
                @Override
                public void write( final OutputStream output ) throws IOException, WebApplicationException
                {
                    try
                    {
                        final ServletOutputStream servletOutputStream = new ServletOutputStream()
                        {
                            @Override
                            public void write( int i ) throws IOException
                            {
                                output.write( i );
                            }

                            @Override
                            public boolean isReady()
                            {
                                return true;
                            }

                            @Override
                            public void setWriteListener( WriteListener writeListener )
                            {
                                try
                                {
                                    writeListener.onWritePossible();
                                }
                                catch ( IOException e )
                                {
                                    // Ignore
                                }
                            }
                        };
                        new StreamingBatchOperations( webServer ).readAndExecuteOperations( uriInfo, httpHeaders, req,
                                body, servletOutputStream );
                        representationWriteHandler.onRepresentationWritten();
                    }
                    catch ( Exception e )
                    {
                        LOGGER.warn( "Error executing batch request ", e );
                    }
                    finally
                    {
                        representationWriteHandler.onRepresentationFinal();
                    }
                }
            };
            return Response.ok(stream)
                    .type( HttpHeaderUtils.mediaTypeWithCharsetUtf8(MediaType.APPLICATION_JSON_TYPE) ).build();
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    private Response batchProcess( UriInfo uriInfo, HttpHeaders httpHeaders, HttpServletRequest req, InputStream body )
    {
        try
        {
            NonStreamingBatchOperations batchOperations = new NonStreamingBatchOperations( webServer );
            BatchOperationResults results = batchOperations.performBatchJobs( uriInfo, httpHeaders, req, body );

            Response res = Response.ok().entity(results.toJSON())
                    .type(HttpHeaderUtils.mediaTypeWithCharsetUtf8(MediaType.APPLICATION_JSON_TYPE)).build();
            representationWriteHandler.onRepresentationWritten();
            return res;
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
        finally
        {
            representationWriteHandler.onRepresentationFinal();
        }
    }

    private boolean isStreaming( HttpHeaders httpHeaders )
    {
        if ( "true".equalsIgnoreCase( httpHeaders.getRequestHeaders().getFirst( StreamingFormat.STREAM_HEADER ) ) )
        {
            return true;
        }
        for ( MediaType mediaType : httpHeaders.getAcceptableMediaTypes() )
        {
            Map<String, String> parameters = mediaType.getParameters();
            if ( parameters.containsKey( "stream" ) && "true".equalsIgnoreCase( parameters.get( "stream" ) ) )
            {
                return true;
            }
        }
        return false;
    }
}
