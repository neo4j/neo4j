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

import org.neo4j.bolt.messaging.StructType;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.Neo4jPackV2;
import org.neo4j.bolt.packstream.PackedOutputArray;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SecureWebSocketConnection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.WebSocketConnection;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class UnsupportedStructTypesV2IT
{
    @Inject
    private Neo4jWithSocket server;

    private HostnamePort address;
    private TransportConnection connection;
    private TransportTestUtil util;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( withOptionalBoltEncryption() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
        util = new TransportTestUtil( new Neo4jPackV2() );
    }

    @AfterEach
    public void cleanup() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
        server.shutdownDatabase();
    }

    public static Stream<Arguments> classProvider()
    {
        return Stream.of( Arguments.of( SocketConnection.class ), Arguments.of( WebSocketConnection.class ),
                Arguments.of( SecureSocketConnection.class ), Arguments.of( SecureWebSocketConnection.class ) );
    }

    private void initConnection( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        connection = connectionClass.getDeclaredConstructor().newInstance();
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldFailWhenPoint2DIsSentWithInvalidCrsId( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testFailureWithUnpackableValue( packer ->
        {
            packer.packStructHeader( 3, StructType.POINT_2D.signature() );
            packer.pack( Values.of( 5 ) );
            packer.pack( Values.of( 3.15 ) );
            packer.pack( Values.of( 4.012 ) );
        }, "Unable to construct Point value: `Unknown coordinate reference system code: 5`" );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldFailWhenPoint3DIsSentWithInvalidCrsId( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testFailureWithUnpackableValue( packer ->
        {
            packer.packStructHeader( 4, StructType.POINT_3D.signature() );
            packer.pack( Values.of( 1200 ) );
            packer.pack( Values.of( 3.15 ) );
            packer.pack( Values.of( 4.012 ) );
            packer.pack( Values.of( 5.905 ) );
        }, "Unable to construct Point value: `Unknown coordinate reference system code: 1200`" );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldFailWhenPoint2DDimensionsDoNotMatch( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testDisconnectWithUnpackableValue( packer ->
        {
            packer.packStructHeader( 3, StructType.POINT_3D.signature() );
            packer.pack( Values.of( CoordinateReferenceSystem.Cartesian_3D.getCode() ) );
            packer.pack( Values.of( 3.15 ) );
            packer.pack( Values.of( 4.012 ) );
        }, "Unable to construct Point value: `Cannot create point, CRS cartesian-3d expects 3 dimensions, but got coordinates [3.15, 4.012]`" );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldFailWhenPoint3DDimensionsDoNotMatch( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testFailureWithUnpackableValue( packer ->
        {
            packer.packStructHeader( 4, StructType.POINT_3D.signature() );
            packer.pack( Values.of( CoordinateReferenceSystem.Cartesian.getCode() ) );
            packer.pack( Values.of( 3.15 ) );
            packer.pack( Values.of( 4.012 ) );
            packer.pack( Values.of( 5.905 ) );
        }, "Unable to construct Point value: `Cannot create point, CRS cartesian expects 2 dimensions, but got coordinates [3.15, 4.012, 5.905]`" );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldFailWhenZonedDateTimeZoneIdIsNotKnown( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

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
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.defaultAuth() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createRunWith( valuePacker ) ) );

        assertThat( connection ).satisfies(
                util.eventuallyReceives( msgFailure( Status.Statement.TypeError, expectedMessage ) ) );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    private void testDisconnectWithUnpackableValue( ThrowingConsumer<Neo4jPack.Packer, IOException> valuePacker, String expectedMessage ) throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.defaultAuth() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createRunWith( valuePacker ) ) );

        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    private byte[] createRunWith( ThrowingConsumer<Neo4jPack.Packer, IOException> valuePacker ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPackV2().newPacker( out );

        packer.packStructHeader( 2, RunMessage.SIGNATURE );
        packer.pack( "RETURN $x" );
        packer.packMapHeader( 1 );
        packer.pack( "x" );
        valuePacker.accept( packer );

        return out.bytes();
    }
}
