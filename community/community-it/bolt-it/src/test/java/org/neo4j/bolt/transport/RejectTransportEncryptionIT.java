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

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SecureWebSocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class RejectTransportEncryptionIT
{
    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( settings ->
        {
            settings.put( BoltConnector.encryption_level, DISABLED );
        } );
        server.init( testInfo );
    }

    @AfterEach
    public void cleanup()
    {
        server.shutdownDatabase();
    }

    private TransportConnection client;
    private TransportTestUtil util;

    public static Stream<Arguments> transportFactory()
    {
        return Stream.of(
                Arguments.of( SecureWebSocketConnection.class,
                        new IOException( "Failed to connect to the server within 30 seconds" ) ),
                Arguments.of( SecureSocketConnection.class,
                        new IOException( "Remote host terminated the handshake" )
                ) );
    }

    @BeforeEach
    public void setup()
    {
        this.util = new TransportTestUtil();
    }

    @AfterEach
    public void teardown() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    @ParameterizedTest( name = "{displayName} {index}" )
    @MethodSource( "transportFactory" )
    public void shouldRejectConnectionAfterHandshake( Class<? extends TransportConnection> c, Exception expected ) throws Exception
    {
        this.client = c.getDeclaredConstructor().newInstance();

        var exception = assertThrows( expected.getClass(), () -> client.connect( server.lookupDefaultConnector() ).send( util.defaultAcceptedVersions() ) );
        assertEquals( expected.getMessage(), exception.getMessage() );
    }
}
