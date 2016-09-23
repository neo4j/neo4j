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
package org.neo4j.bolt.v1.runtime.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.bolt.v1.messaging.message.InitMessage;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.helpers.HostnamePort;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.BoltConnector.EncryptionLevel.REQUIRED;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnector;

public class BoltConfigIT
{

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(),
            settings -> {
                settings.put( boltConnector("0").type.name(), "BOLT" );
                settings.put( boltConnector("0").enabled.name(), "true" );
                settings.put( boltConnector("0").address.name(), "localhost:7888" );
                settings.put( boltConnector("1").type.name(), "BOLT" );
                settings.put( boltConnector("1").enabled.name(), "true" );
                settings.put( boltConnector("1").address.name(), "localhost:7687" );
                settings.put( boltConnector("1").encryption_level.name(), REQUIRED.name() );
            } );

    @Test
    public void shouldSupportMultipleConnectors() throws Throwable
    {
        // Given
        // When

        // Then
        HostnamePort address0 = new HostnamePort( "localhost:7888" );
        assertConnectionAccepted( address0, new WebSocketConnection() );
        assertConnectionAccepted( address0, new SecureWebSocketConnection() );
        assertConnectionAccepted( address0, new SocketConnection() );
        assertConnectionAccepted( address0, new SecureSocketConnection() );

        HostnamePort address1 = new HostnamePort( "localhost:7687" );
        assertConnectionRejected( address1, new WebSocketConnection() );
        assertConnectionAccepted( address1, new SecureWebSocketConnection() );
        assertConnectionRejected( address1, new SocketConnection() );
        assertConnectionAccepted( address1, new SecureSocketConnection() );

    }

    private void assertConnectionRejected( HostnamePort address, TransportConnection client ) throws Exception
    {
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) );

        assertThat( client, eventuallyDisconnects() );
    }

    private void assertConnectionAccepted( HostnamePort address, TransportConnection client ) throws Exception
    {
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk( InitMessage.init( "TestClient/1.1", emptyMap() ) ) );
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
    }
}
