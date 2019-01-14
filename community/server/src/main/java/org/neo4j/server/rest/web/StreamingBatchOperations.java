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
package org.neo4j.server.rest.web;

import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriInfo;

import org.neo4j.server.rest.batch.BatchOperations;
import org.neo4j.server.rest.batch.StreamingBatchOperationResults;
import org.neo4j.server.rest.domain.BatchOperationFailedException;
import org.neo4j.server.rest.repr.formats.StreamingJsonFormat;
import org.neo4j.server.web.WebServer;

public class StreamingBatchOperations extends BatchOperations
{

    private static final Logger LOGGER = Log.getLogger( StreamingBatchOperations.class );
    private StreamingBatchOperationResults results;

    public StreamingBatchOperations( WebServer webServer )
    {
        super( webServer );
    }

    public void readAndExecuteOperations( UriInfo uriInfo, HttpHeaders httpHeaders, HttpServletRequest req,
            InputStream body, ServletOutputStream output ) throws IOException, ServletException
    {
        results = new StreamingBatchOperationResults( jsonFactory.createJsonGenerator( output ), output );
        Map<Integer,String> locations = results.getLocations();
        parseAndPerform( uriInfo, httpHeaders, req, body, locations );
        results.close();
    }

    @Override
    protected void invoke( String method, String path, String body, Integer id, URI targetUri,
            InternalJettyServletRequest req, InternalJettyServletResponse res ) throws IOException
    {
        results.startOperation( path, id );
        try
        {
            res = new BatchInternalJettyServletResponse( results.getServletOutputStream() );
            webServer.invokeDirectly( targetUri.getPath(), req, res );
        }
        catch ( Exception e )
        {
            LOGGER.warn( e );
            results.writeError( 500, e.getMessage() );
            throw new BatchOperationFailedException( 500, e.getMessage(), e );

        }
        final int status = res.getStatus();
        if ( is2XXStatusCode( status ) )
        {
            results.addOperationResult( status, id, res.getHeader( "Location" ) );
        }
        else
        {
            final String message = "Error " + status + " executing batch operation: " +
                    ((id != null) ? id + ". " : "") + method + " " + path + " " + body;
            results.writeError( status, res.getReason() );
            throw new BatchOperationFailedException( status, message, new Exception( res.getReason() ) );
        }
    }

    @Override
    protected void addHeaders( final InternalJettyServletRequest res, final HttpHeaders httpHeaders )
    {
        super.addHeaders( res, httpHeaders );
        res.addHeader( StreamingJsonFormat.STREAM_HEADER, "true" );
    }

    private static class BatchInternalJettyServletResponse extends InternalJettyServletResponse
    {
        private final ServletOutputStream output;

        BatchInternalJettyServletResponse( ServletOutputStream output )
        {
            this.output = output;
        }

        @Override
        public ServletOutputStream getOutputStream()
        {
            return output;
        }

        @Override
        public PrintWriter getWriter()
        {
            return new PrintWriter( new OutputStreamWriter( output, StandardCharsets.UTF_8 ) );
        }
    }
}
