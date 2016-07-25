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
package org.neo4j.bolt.v1.transport.integration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.helpers.HostnamePort;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
public class ConnectionIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket();

    @Parameterized.Parameter(0)
    public Connection connection;

    @Parameterized.Parameter(1)
    public HostnamePort address;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
                new Object[]{
                        new SecureSocketConnection(),
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        new SocketConnection(),
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        new SecureWebSocketConnection(),
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        new WebSocketConnection(),
                        new HostnamePort( "localhost:7687" )
                });
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
