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

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

import org.neo4j.bolt.v1.messaging.BoltRequestMessage;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.values.storable.Values.pointValue;

@RunWith( Parameterized.class )
public class UnsupportedStructTypesV1IT
{
    private static final String USER_AGENT = "TestClient/1.0";

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
        util = new TransportTestUtil( new Neo4jPackV1() );
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
    public void shouldFailWhenPoint2DIsSentWithRun() throws Exception
    {
        testFailureWithV2Value( pointValue( CoordinateReferenceSystem.WGS84, 1.2, 3.4 ), "Point" );
    }

    @Test
    public void shouldFailWhenPoint3DIsSentWithRun() throws Exception
    {
        testFailureWithV2Value( pointValue( CoordinateReferenceSystem.WGS84_3D, 1.2, 3.4, 4.5 ), "Point" );
    }

    @Test
    public void shouldFailWhenDurationIsSentWithRun() throws Exception
    {
        testFailureWithV2Value( ValueUtils.of( Duration.ofDays( 10 ) ), "Duration" );
    }

    @Test
    public void shouldFailWhenDateIsSentWithRun() throws Exception
    {
        testFailureWithV2Value( ValueUtils.of( LocalDate.now() ), "Date" );
    }

    @Test
    public void shouldFailWhenLocalTimeIsSentWithRun() throws Exception
    {
        testFailureWithV2Value( ValueUtils.of( LocalTime.now() ), "LocalTime" );
    }

    @Test
    public void shouldFailWhenLocalDateTimeIsSentWithRun() throws Exception
    {
        testFailureWithV2Value( ValueUtils.of( LocalDateTime.now() ), "LocalDateTime" );
    }

    @Test
    public void shouldFailWhenOffsetTimeIsSentWithRun() throws Exception
    {
        testFailureWithV2Value( ValueUtils.of( OffsetTime.now() ), "OffsetTime" );
    }

    @Test
    public void shouldFailWhenOffsetDateTimeIsSentWithRun() throws Exception
    {
        testFailureWithV2Value( ValueUtils.of( OffsetDateTime.now() ), "OffsetDateTime" );
    }

    @Test
    public void shouldFailWhenZonedDateTimeIsSentWithRun() throws Exception
    {
        testFailureWithV2Value( ValueUtils.of( ZonedDateTime.now() ), "ZonedDateTime" );
    }

    private void testFailureWithV2Value( AnyValue value, String description ) throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.chunk( init( USER_AGENT, Collections.emptyMap() ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createRunWithV2Value( value ) ) );

        assertThat( connection,
                util.eventuallyReceives( msgFailure( Status.Statement.TypeError, description + " values cannot be unpacked with this version of bolt." ) ) );
        assertThat( connection, eventuallyDisconnects() );
    }

    private byte[] createRunWithV2Value( AnyValue value ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPackV2().newPacker( out );

        packer.packStructHeader( 2, BoltRequestMessage.RUN.signature() );
        packer.pack( "RETURN $x" );
        packer.packMapHeader( 1 );
        packer.pack( "x" );
        packer.pack( value );

        return out.bytes();
    }
}
