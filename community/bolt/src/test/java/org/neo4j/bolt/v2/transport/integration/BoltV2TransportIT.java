/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt.v2.transport.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.List;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.values.storable.PointValue;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.virtual.VirtualValues.map;

@RunWith( Parameterized.class )
public class BoltV2TransportIT
{
    private static final String USER_AGENT = "TestClient/2.0";

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), settings -> settings.put( auth_enabled.name(), "false" ) );

    @Parameter
    public Class<? extends TransportConnection> connectionClass;

    private HostnamePort address;
    private TransportConnection connection;
    private TransportTestUtil util;

    @Parameters( name = "{0}" )
    public static List<Class<? extends TransportConnection>> transports()
    {
        return asList( SocketConnection.class, WebSocketConnection.class, SecureSocketConnection.class, SecureWebSocketConnection.class );
    }

    @Before
    public void setUp() throws Exception
    {
        address = server.lookupDefaultConnector();
        connection = connectionClass.newInstance();
        util = new TransportTestUtil( new Neo4jPackV2() );
    }

    @After
    public void tearDown() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    @Test
    public void shouldNegotiateProtocolV2() throws Exception
    {
        connection.connect( address )
                .send( util.acceptedVersions( 2, 0, 0, 0 ) )
                .send( util.chunk( init( USER_AGENT, emptyMap() ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 2} ) );
    }

    @Test
    public void shouldNegotiateProtocolV2WhenClientSupportsBothV1AndV2() throws Exception
    {
        connection.connect( address )
                .send( util.acceptedVersions( 2, 1, 0, 0 ) )
                .send( util.chunk( init( USER_AGENT, emptyMap() ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 2} ) );
    }

    @Test
    public void shouldSendPoints() throws Exception
    {
        PointValue point = pointValue( WGS84, 39.111748, -76.775635 );

        negotiateBoltV2();
        connection.send( util.chunk(
                run( "CREATE (n: Object {location: $location}) RETURN 42", map( singletonMap( "location", point ) ) ),
                pullAll() ) );

        assertThat( connection, util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( equalTo( longValue( 42 ) ) ) ),
                msgSuccess() ) );
    }

    @Test
    public void shouldReceivePoints() throws Exception
    {
        negotiateBoltV2();
        connection.send( util.chunk(
                run( "RETURN point({x: 40.7624, y: 73.9738})" ),
                pullAll() ) );

        assertThat( connection, util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( equalTo( pointValue( Cartesian, 40.7624, 73.9738 ) ) ) ),
                msgSuccess() ) );
    }

    @Test
    public void shouldSendAndReceivePoints() throws Throwable
    {
        PointValue point = pointValue( WGS84, 38.8719, 77.0563 );

        negotiateBoltV2();
        connection.send( util.chunk(
                run( "RETURN $point", map( singletonMap( "point", point ) ) ),
                pullAll() ) );

        assertThat( connection, util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( equalTo( point ) ) ),
                msgSuccess() ) );
    }

    private void negotiateBoltV2() throws Exception
    {
        connection.connect( address )
                .send( util.acceptedVersions( 2, 0, 0, 0 ) )
                .send( util.chunk( init( USER_AGENT, emptyMap() ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 2} ) );
    }
}
