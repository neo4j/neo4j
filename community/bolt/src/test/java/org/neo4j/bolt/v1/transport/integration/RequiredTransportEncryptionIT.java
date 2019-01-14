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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.BoltConnector;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket.DEFAULT_CONNECTOR_KEY;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.REQUIRED;

@RunWith( Parameterized.class )
public class RequiredTransportEncryptionIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(),
            settings ->
            {
                Setting<BoltConnector.EncryptionLevel> encryptionLevel =
                        new BoltConnector( DEFAULT_CONNECTOR_KEY ).encryption_level;
                settings.put( encryptionLevel.name(), REQUIRED.name() );
            } );

    @Parameterized.Parameter( 0 )
    public Factory<TransportConnection> cf;

    private HostnamePort address;
    private TransportConnection client;
    private TransportTestUtil util;

    @Parameterized.Parameters
    public static Collection<Factory<TransportConnection>> transports()
    {
        return asList( SocketConnection::new, WebSocketConnection::new );
    }

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
        this.address = server.lookupDefaultConnector();
        this.util = new TransportTestUtil( new Neo4jPackV1() );
    }

    @After
    public void teardown() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    @Test
    public void shouldCloseUnencryptedConnectionOnHandshakeWhenEncryptionIsRequired() throws Throwable
    {
        // When
        client.connect( address )
                .send( util.acceptedVersions( 1, 0, 0, 0 ) );

        assertThat( client, TransportTestUtil.eventuallyDisconnects() );
    }
}
