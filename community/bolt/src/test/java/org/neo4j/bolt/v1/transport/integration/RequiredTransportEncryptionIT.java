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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.BoltConnector.EncryptionLevel.REQUIRED;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnector;

@RunWith( Parameterized.class )
public class RequiredTransportEncryptionIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket(
            settings -> {
                Setting<GraphDatabaseSettings.BoltConnector.EncryptionLevel> encryption_level =
                        boltConnector( "0" ).encryption_level;
                settings.put( encryption_level, REQUIRED.name() );
            } );

    @Parameterized.Parameter( 0 )
    public Factory<TransportConnection> cf;

    @Parameterized.Parameter( 1 )
    public HostnamePort address;

    private TransportConnection client;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
                new Object[]{
                        (Factory<TransportConnection>) SocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<TransportConnection>) WebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                } );
    }

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
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
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) );

        assertThat( client, TransportTestUtil.eventuallyDisconnects() );
    }
}
