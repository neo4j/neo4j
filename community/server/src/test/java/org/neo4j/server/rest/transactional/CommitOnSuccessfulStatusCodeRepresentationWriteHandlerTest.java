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

import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;

import org.neo4j.graphdb.Transaction;
import org.neo4j.server.rest.repr.RepresentationWriteHandler;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.OK;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CommitOnSuccessfulStatusCodeRepresentationWriteHandlerTest
{
    private final HttpServletResponse response = mock( HttpServletResponse.class );
    private final Transaction tx = mock( Transaction.class );

    private final RepresentationWriteHandler handler = new CommitOnSuccessfulStatusCodeRepresentationWriteHandler( response, tx );

    @Test
    void shouldMarkTransactionAsSuccessfulWhenResponseCodeIs200()
    {
        when( response.getStatus() ).thenReturn( OK.getStatusCode() );

        handler.onRepresentationWritten();

        verify( tx ).success();
    }

    @Test
    void shouldMarkTransactionAsSuccessfulWhenResponseCodeIsNotDefined()
    {
        when( response.getStatus() ).thenReturn( -1 );

        handler.onRepresentationWritten();

        verify( tx ).success();
    }

    @Test
    void shouldNotMarkTransactionAsSuccessfulWhenResponseCodeIs500()
    {
        when( response.getStatus() ).thenReturn( INTERNAL_SERVER_ERROR.getStatusCode() );

        handler.onRepresentationWritten();

        verify( tx, never() ).success();
    }
}
