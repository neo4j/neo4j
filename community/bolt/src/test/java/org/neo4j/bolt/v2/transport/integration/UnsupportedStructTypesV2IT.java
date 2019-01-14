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
package org.neo4j.bolt.v2.transport.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.neo4j.bolt.messaging.StructType;
import org.neo4j.bolt.v1.messaging.BoltRequestMessage;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;

@RunWith( Parameterized.class )
public class UnsupportedStructTypesV2IT
{
    private static final String USER_AGENT = "TestClient/2.0";

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), settings -> settings.put( auth_enabled.name(), "false" ) );

    @Parameterized.Parameter
    public Class<? extends TransportConnection> connectionClass;

    private HostnamePort address;
    private TransportConnection connection;
    private TransportTestUtil util;

    @Parameterized.Parameters( name = "{0}" )
    public static List<Class<? extends TransportConnection>> transports()
    {
        return asList( SocketConnection.class, WebSocketConnection.class, SecureSocketConnection.class, SecureWebSocketConnection.class );
    }

    @Before
    public void setup() throws Exception
    {
        address = server.lookupDefaultConnector();
        connection = connectionClass.newInstance();
        util = new TransportTestUtil( new Neo4jPackV2() );
    }

    @After
    public void cleanup() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    @Test
    public void shouldFailWhenPoint2DIsSentWithInvalidCrsId() throws Exception
    {
        testFailureWithUnpackableValue( packer ->
        {
            packer.packStructHeader( 3, StructType.POINT_2D.signature() );
            packer.pack( Values.of( 5 ) );
            packer.pack( Values.of( 3.15 ) );
            packer.pack( Values.of( 4.012 ) );
        }, "Unable to construct Point value: `Unknown coordinate reference system code: 5`" );
    }

    @Test
    public void shouldFailWhenPoint3DIsSentWithInvalidCrsId() throws Exception
    {
        testFailureWithUnpackableValue( packer ->
        {
            packer.packStructHeader( 4, StructType.POINT_3D.signature() );
            packer.pack( Values.of( 1200 ) );
            packer.pack( Values.of( 3.15 ) );
            packer.pack( Values.of( 4.012 ) );
            packer.pack( Values.of( 5.905 ) );
        }, "Unable to construct Point value: `Unknown coordinate reference system code: 1200`" );
    }

    @Test
    public void shouldFailWhenPoint2DDimensionsDoNotMatch() throws Exception
    {
        testDisconnectWithUnpackableValue( packer ->
        {
            packer.packStructHeader( 3, StructType.POINT_3D.signature() );
            packer.pack( Values.of( CoordinateReferenceSystem.Cartesian_3D.getCode() ) );
            packer.pack( Values.of( 3.15 ) );
            packer.pack( Values.of( 4.012 ) );
        }, "Unable to construct Point value: `Cannot create point, CRS cartesian-3d expects 3 dimensions, but got coordinates [3.15, 4.012]`" );
    }

    @Test
    public void shouldFailWhenPoint3DDimensionsDoNotMatch() throws Exception
    {
        testFailureWithUnpackableValue( packer ->
        {
            packer.packStructHeader( 4, StructType.POINT_3D.signature() );
            packer.pack( Values.of( CoordinateReferenceSystem.Cartesian.getCode() ) );
            packer.pack( Values.of( 3.15 ) );
            packer.pack( Values.of( 4.012 ) );
            packer.pack( Values.of( 5.905 ) );
        }, "Unable to construct Point value: `Cannot create point, CRS cartesian expects 2 dimensions, but got coordinates [3.15, 4.012, 5.905]`" );
    }

    @Test
    public void shouldFailWhenZonedDateTimeZoneIdIsNotKnown() throws Exception
    {
        testFailureWithUnpackableValue( packer ->
        {
            packer.packStructHeader( 3, StructType.DATE_TIME_WITH_ZONE_NAME.signature() );
            packer.pack( Values.of( 0 ) );
            packer.pack( Values.of( 0 ) );
            packer.pack( Values.of( "Europe/Marmaris" ) );
        }, "Unable to construct ZonedDateTime value: `Unknown time-zone ID: Europe/Marmaris`" );
    }

    private void testFailureWithUnpackableValue( ThrowingConsumer<Neo4jPack.Packer, IOException> valuePacker, String expectedMessage ) throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.chunk( init( USER_AGENT, Collections.emptyMap() ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createRunWith( valuePacker ) ) );

        assertThat( connection,
                util.eventuallyReceives( msgFailure( Status.Statement.TypeError, expectedMessage ) ) );
        assertThat( connection, eventuallyDisconnects() );
    }

    private void testDisconnectWithUnpackableValue( ThrowingConsumer<Neo4jPack.Packer, IOException> valuePacker, String expectedMessage ) throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.chunk( init( USER_AGENT, Collections.emptyMap() ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createRunWith( valuePacker ) ) );

        assertThat( connection, eventuallyDisconnects() );
    }

    private byte[] createRunWith( ThrowingConsumer<Neo4jPack.Packer, IOException> valuePacker ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPackV2().newPacker( out );

        packer.packStructHeader( 2, BoltRequestMessage.RUN.signature() );
        packer.pack( "RETURN $x" );
        packer.packMapHeader( 1 );
        packer.pack( "x" );
        valuePacker.accept( packer );

        return out.bytes();
    }
}
