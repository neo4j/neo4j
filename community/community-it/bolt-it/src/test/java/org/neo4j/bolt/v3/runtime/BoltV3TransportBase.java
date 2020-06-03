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
package org.neo4j.bolt.v3.runtime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SecureWebSocketConnection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.WebSocketConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;
import static org.neo4j.bolt.v3.BoltProtocolV3ComponentFactory.newMessageEncoder;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public abstract class BoltV3TransportBase
{
    protected static final String USER_AGENT = "TestClient/3.0";

    @Inject
    public Neo4jWithSocket server;

    protected HostnamePort address;
    protected TransportConnection connection;
    protected TransportTestUtil util;

    private static Stream<Arguments> argumentsProvider()
    {
        // TODO : is this used
        return Stream.of( Arguments.of( SocketConnection.class ), Arguments.of( WebSocketConnection.class ),
                Arguments.of( SecureSocketConnection.class ), Arguments.of( SecureWebSocketConnection.class ) );
    }

    protected void init( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        connection = connectionClass.getDeclaredConstructor().newInstance();
    }

    @BeforeEach
    public void setUp( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( withOptionalBoltEncryption() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
        util = new TransportTestUtil( newMessageEncoder() );
    }

    @AfterEach
    public void tearDown() throws IOException
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    protected void negotiateBoltV3() throws Exception
    {
        connection.connect( address )
                .send( util.acceptedVersions( 3, 0, 0, 0 ) )
                .send( util.chunk( new HelloMessage( MapUtil.map( "user_agent", USER_AGENT ) ) ) );

        assertThat( connection ).satisfies( eventuallyReceives( new byte[]{0, 0, 0, 3} ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }
}
