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
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.WebSocketConnection;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.REQUIRED;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class RequiredTransportEncryptionIT
{
    private HostnamePort address;
    private TransportConnection client;
    private TransportTestUtil util;

    public static Stream<Arguments> factoryProvider()
    {
        return Stream.of( Arguments.of( SocketConnection.class ), Arguments.of( WebSocketConnection.class ) );
    }

    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( settings -> settings.put( BoltConnector.encryption_level, REQUIRED ) );
        server.init( testInfo );

        address = server.lookupDefaultConnector();
        util = new TransportTestUtil();
    }

    @AfterEach
    public void cleanup() throws IOException
    {
        if ( client != null )
        {
            client.disconnect();
        }
        server.shutdownDatabase();
    }

    @ParameterizedTest( name = "{displayName} {index}" )
    @MethodSource( "factoryProvider" )
    public void shouldCloseUnencryptedConnectionOnHandshakeWhenEncryptionIsRequired( Class<? extends TransportConnection> c ) throws Exception
    {
        this.client = c.getDeclaredConstructor().newInstance();

        // When
        client.connect( address ).send( util.defaultAcceptedVersions() );

        assertThat( client ).satisfies( eventuallyDisconnects() );
    }
}
