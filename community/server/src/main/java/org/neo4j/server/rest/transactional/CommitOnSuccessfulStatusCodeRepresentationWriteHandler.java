/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.server.rest.transactional;

import javax.servlet.http.HttpServletResponse;

import org.neo4j.graphdb.Transaction;
import org.neo4j.server.rest.repr.RepresentationWriteHandler;

import static javax.ws.rs.core.Response.Status.NO_CONTENT;

public class CommitOnSuccessfulStatusCodeRepresentationWriteHandler implements RepresentationWriteHandler
{
    private static final int MIN_HTTP_CODE = 100;
    private static final int MAX_HTTP_CODE = 599;

    private final HttpServletResponse response;
    private Transaction transaction;

    public CommitOnSuccessfulStatusCodeRepresentationWriteHandler( HttpServletResponse response, Transaction transaction )
    {
        this.response = response;
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
        int statusCode = responseStatus();
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

    private int responseStatus()
    {
        int status = response.getStatus();
        if ( status >= MIN_HTTP_CODE && status <= MAX_HTTP_CODE )
        {
            return status;
        }
        return NO_CONTENT.getStatusCode();
    }
}
