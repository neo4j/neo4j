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
package org.neo4j.bolt.transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SecureWebSocketConnection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.WebSocketConnection;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class ConnectionIT
{
    @Inject
    private Neo4jWithSocket server;

    public TransportConnection connection;

    private HostnamePort address;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( withOptionalBoltEncryption() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
    }

    @AfterEach
    public void cleanup() throws IOException
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    public static Stream<Arguments> transportFactory()
    {
        return Stream.of( Arguments.of( new SecureSocketConnection() ),
                Arguments.of( new SocketConnection() ),
                Arguments.of( new SecureWebSocketConnection()),
                Arguments.of( new WebSocketConnection() ) );
    }

    @ParameterizedTest( name = "{displayName} {index}" )
    @MethodSource( "transportFactory" )
    public void shouldCloseConnectionOnInvalidHandshake( TransportConnection connection ) throws Exception
    {
        this.connection = connection;

        // GIVEN
        connection.connect( address );

        // WHEN
        connection.send( new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xB0, (byte) 0x17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} );

        // THEN
        assertThrows( IOException.class, () -> connection.recv( 4 ) );
    }
}
