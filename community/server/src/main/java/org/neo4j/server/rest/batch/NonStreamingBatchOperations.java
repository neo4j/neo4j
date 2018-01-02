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
package org.neo4j.server.rest.batch;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriInfo;

import org.neo4j.server.rest.domain.BatchOperationFailedException;
import org.neo4j.server.rest.web.InternalJettyServletRequest;
import org.neo4j.server.rest.web.InternalJettyServletResponse;
import org.neo4j.server.web.WebServer;

public class NonStreamingBatchOperations extends BatchOperations
{

    private BatchOperationResults results;

    public NonStreamingBatchOperations( WebServer webServer )
    {
        super( webServer );
    }

    public BatchOperationResults performBatchJobs( UriInfo uriInfo, HttpHeaders httpHeaders, HttpServletRequest req, InputStream body ) throws IOException, ServletException
    {
        results = new BatchOperationResults();
        parseAndPerform( uriInfo, httpHeaders, req, body, results.getLocations() );
        return results;
    }

    @Override
    protected void invoke( String method, String path, String body, Integer id, URI targetUri, InternalJettyServletRequest req, InternalJettyServletResponse res ) throws IOException, ServletException
    {
        webServer.invokeDirectly(targetUri.getPath(), req, res);

        String resultBody = res.getOutputStream().toString();
        if (is2XXStatusCode(res.getStatus()))
        {
            results.addOperationResult(path, id, resultBody, res.getHeader("Location"));
        } else
        {
            throw new BatchOperationFailedException(res.getStatus(), resultBody, null );
        }
    }

}
