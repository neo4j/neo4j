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
package org.neo4j.server.rest.transactional;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.core.HttpResponseContext;

import org.neo4j.graphdb.Transaction;
import org.neo4j.server.rest.repr.RepresentationWriteHandler;

public class CommitOnSuccessfulStatusCodeRepresentationWriteHandler implements RepresentationWriteHandler
{
    private final HttpContext httpContext;
    private Transaction transaction;

    public CommitOnSuccessfulStatusCodeRepresentationWriteHandler( HttpContext httpContext, Transaction transaction )
    {
        this.httpContext = httpContext;
        this.transaction = transaction;
    }

    @Override
    public void onRepresentationStartWriting()
    {
        // do nothing
    }

    @Override
    public void onRepresentationWritten()
    {
        HttpResponseContext response = httpContext.getResponse();

        int statusCode = response.getStatus();
        if ( statusCode >= 200 && statusCode < 300 )
        {
            transaction.success();
        }
    }

    @Override
    public void onRepresentationFinal()
    {
        closeTransaction();
    }


    public void closeTransaction()
    {
        transaction.close();
    }

    public void setTransaction( Transaction transaction )
    {
        this.transaction = transaction;
    }
}
