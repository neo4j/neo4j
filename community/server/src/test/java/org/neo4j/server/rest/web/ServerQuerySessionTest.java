/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import javax.servlet.http.HttpServletRequest;

import org.junit.Test;

import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.query.FakeTransactionalContext;
import org.neo4j.kernel.impl.query.QuerySession;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerQuerySessionTest
{

    @Test
    public void shouldIncludeUsernameInToString()
    {
        QuerySession session = new ServerQuerySession( null, new FakeTransactionalContext( AccessMode.Static.READ ) );

        assertThat( session.toString(), equalTo( String.format( "server-session\t%s", "READ" ) ) );
    }

    @Test
    public void shouldIncludeUsernameInToStringWithHttp()
    {
        HttpServletRequest mock = mock( HttpServletRequest.class );
        when(mock.getRemoteAddr()).thenReturn( "remote-addr" );
        when(mock.getRequestURI()).thenReturn( "req-uri" );

        QuerySession session = new ServerQuerySession( mock, new FakeTransactionalContext( AccessMode.Static.READ ) );

        assertThat( session.toString(), equalTo( String.format( "server-session\thttp\t%s\t%s\t%s", "remote-addr", "req-uri", "READ" ) ) );
    }

}
