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
package org.neo4j.bolt.v3.runtime.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.MapUtil;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.v3.messaging.BoltProtocolV3ComponentFactory.newMessageEncoder;
import static org.neo4j.bolt.v3.messaging.BoltProtocolV3ComponentFactory.newNeo4jPack;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;

@RunWith( Parameterized.class )
public abstract class BoltV3TransportBase
{
    protected static final String USER_AGENT = "TestClient/3.0";

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), settings -> settings.put( auth_enabled.name(), "false" ) );

    @Parameterized.Parameter
    public Class<? extends TransportConnection> connectionClass;

    protected HostnamePort address;
    protected TransportConnection connection;
    protected TransportTestUtil util;

    @Parameterized.Parameters( name = "{0}" )
    public static List<Class<? extends TransportConnection>> transports()
    {
        return asList( SocketConnection.class, WebSocketConnection.class, SecureSocketConnection.class, SecureWebSocketConnection.class );
    }

    @Before
    public void setUp() throws Exception
    {
        address = server.lookupDefaultConnector();
        connection = connectionClass.newInstance();
        util = new TransportTestUtil( newNeo4jPack(), newMessageEncoder() );
    }

    @After
    public void tearDown() throws Exception
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

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 3} ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }
}
