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

import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.function.Factory;
import org.neo4j.kernel.configuration.BoltConnector;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.JavaVersion.JAVA_9;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket.DEFAULT_CONNECTOR_KEY;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.DISABLED;

@RunWith( Parameterized.class )
public class RejectTransportEncryptionIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(),
            settings ->
            {
                settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).type.name(), "BOLT" );
                settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).encryption_level.name(), DISABLED.name() );
            } );
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Parameterized.Parameter( 0 )
    public Factory<TransportConnection> cf;

    @Parameterized.Parameter( 1 )
    public Exception expected;

    private TransportConnection client;
    private TransportTestUtil util;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
                new Object[]{
                        (Factory<TransportConnection>) SecureWebSocketConnection::new,
                        new IOException( "Failed to connect to the server within 10 seconds" )
                },
                new Object[]{
                        (Factory<TransportConnection>) SecureSocketConnection::new, new IOException(
                        isJavaVersionAtLeast( JAVA_9 ) ? "Remote host terminated the handshake"
                                                       : "Remote host closed connection during handshake" )

                } );
    }

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
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
    public void shouldRejectConnectionAfterHandshake() throws Throwable
    {
        exception.expect( expected.getClass() );
        exception.expectMessage( expected.getMessage() );
        client.connect( server.lookupDefaultConnector() ).send( util.acceptedVersions( 1, 0, 0, 0 ) );
    }
}
