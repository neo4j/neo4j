/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.web;

import org.eclipse.jetty.http.HttpCompliance;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JettyHttpConnectionTest
{
    @Test
    void shouldHaveId()
    {
        Connector connector = connectorMock( "https" );
        JettyHttpConnection connection = newConnection( connector );

        assertEquals( "http-1", connection.id() );
    }

    @Test
    void shouldHaveConnectTime()
    {
        JettyHttpConnection connection = newConnection( connectorMock( "http" ) );

        assertThat( connection.connectTime(), greaterThan( 0L ) );
    }

    @Test
    void shouldHaveConnector()
    {
        JettyHttpConnection connection = newConnection( connectorMock( "http+routing" ) );

        assertEquals( "http+routing", connection.connector() );
    }

    @Test
    void shouldHaveUsernameAndUserAgent()
    {
        JettyHttpConnection connection = newConnection( connectorMock( "http+routing" ) );

        assertNull( connection.username() );
        connection.updateUser( "hello", "my-http-driver/1.2.3" );
        assertEquals( "hello", connection.username() );
        assertEquals( "my-http-driver/1.2.3", connection.userAgent() );
    }

    private static JettyHttpConnection newConnection( Connector connector )
    {
        return new JettyHttpConnection( "http-1", new HttpConfiguration(), connector, mock( EndPoint.class ),
                HttpCompliance.LEGACY, false );
    }

    private static Connector connectorMock( String name )
    {
        Connector connector = mock( Connector.class );
        when( connector.getName() ).thenReturn( name );
        when( connector.getExecutor() ).thenReturn( Runnable::run );
        when( connector.getServer() ).thenReturn( new Server() );
        return connector;
    }
}
