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
package org.neo4j.bolt.v1.transport.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.helpers.HostnamePort;

import static java.util.Arrays.asList;

@RunWith( Parameterized.class )
public class ConnectionIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket(  getClass() );

    @Parameterized.Parameter
    public TransportConnection connection;

    private HostnamePort address;

    @Parameterized.Parameters
    public static Collection<TransportConnection> transports()
    {
        return asList( new SecureSocketConnection(), new SocketConnection(), new SecureWebSocketConnection(),
                new WebSocketConnection() );
    }

    @Before
    public void setUp()
    {
        address = server.lookupDefaultConnector();
    }

    @After
    public void cleanup() throws IOException
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    @Test
    public void shouldCloseConnectionOnInvalidHandshake() throws Exception
    {
        // GIVEN
        connection.connect( address );

        // WHEN
        connection.send( new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xB0, (byte) 0x17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} );

        // THEN
        exception.expect( IOException.class );
        connection.recv( 4 );
    }
}
