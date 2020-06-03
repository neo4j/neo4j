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

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import org.neo4j.bolt.packstream.Neo4jPackV2;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SecureWebSocketConnection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.WebSocketConnection;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgRecord;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.StreamConditions.eqRecord;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.virtual.VirtualValues.map;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class SupportedStructTypesV2IT
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
    public void tearDown() throws IOException
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
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
    public void shouldNegotiateProtocolV4( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() );

        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldNegotiateProtocolV4WhenClientSupportsBothV4AndV3( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        connection.connect( address )
                .send( util.acceptedVersions( 4, 3, 0, 0 ) )
                .send( util.defaultAuth() );

        assertThat( connection ).satisfies( eventuallyReceives( new byte[]{0, 0, 0, 4} ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendPoint2D( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingOfBoltV2Value( pointValue( WGS84, 39.111748, -76.775635 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldReceivePoint2D( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testReceivingOfBoltV2Value( "RETURN point({x: 40.7624, y: 73.9738})", pointValue( Cartesian, 40.7624, 73.9738 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendAndReceivePoint2D( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingAndReceivingOfBoltV2Value( pointValue( WGS84, 38.8719, 77.0563 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendDuration( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingOfBoltV2Value( duration( 5, 3, 34, 0 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldReceiveDuration( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testReceivingOfBoltV2Value( "RETURN duration({months: 3, days: 100, seconds: 999, nanoseconds: 42})", duration( 3, 100, 999, 42 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendAndReceiveDuration( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingAndReceivingOfBoltV2Value( duration( 17, 9, 2, 1_000_000 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendDate( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingOfBoltV2Value( date( 1991, 8, 24 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldReceiveDate( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testReceivingOfBoltV2Value( "RETURN date('2015-02-18')", date( 2015, 2, 18 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendAndReceiveDate( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingAndReceivingOfBoltV2Value( date( 2005, 5, 22 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendLocalTime( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingOfBoltV2Value( localTime( 2, 35, 10, 1 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldReceiveLocalTime( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testReceivingOfBoltV2Value( "RETURN localtime('11:04:35')", localTime( 11, 04, 35, 0 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendAndReceiveLocalTime( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingAndReceivingOfBoltV2Value( localTime( 22, 10, 10, 99 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendTime( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingOfBoltV2Value( time( 424242, ZoneOffset.of( "+08:30" ) ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldReceiveTime( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testReceivingOfBoltV2Value( "RETURN time('14:30+0100')", time( 14, 30, 0, 0, ZoneOffset.ofHours( 1 ) ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendAndReceiveTime( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingAndReceivingOfBoltV2Value( time( 19, 22, 44, 100, ZoneOffset.ofHours( -5 ) ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendLocalDateTime( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingOfBoltV2Value( localDateTime( 2002, 5, 22, 15, 15, 25, 0 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldReceiveLocalDateTime( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testReceivingOfBoltV2Value( "RETURN localdatetime('20150202T19:32:24')", localDateTime( 2015, 2, 2, 19, 32, 24, 0 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendAndReceiveLocalDateTime( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingAndReceivingOfBoltV2Value( localDateTime( 1995, 12, 12, 10, 30, 0, 0 ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendDateTimeWithTimeZoneName( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingOfBoltV2Value( datetime( 1956, 9, 14, 11, 20, 25, 0, "Europe/Stockholm" ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldReceiveDateTimeWithTimeZoneName( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testReceivingOfBoltV2Value( "RETURN datetime({year:1984, month:10, day:11, hour:21, minute:30, timezone:'Europe/London'})",
                datetime( 1984, 10, 11, 21, 30, 0, 0, "Europe/London" ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendAndReceiveDateTimeWithTimeZoneName( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingAndReceivingOfBoltV2Value( datetime( 1984, 10, 11, 21, 30, 0, 0, "Europe/London" ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendDateTimeWithTimeZoneOffset( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingOfBoltV2Value( datetime( 424242, 0, ZoneOffset.ofHoursMinutes( -7, -15 ) ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldReceiveDateTimeWithTimeZoneOffset( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testReceivingOfBoltV2Value( "RETURN datetime({year:2022, month:3, day:2, hour:19, minute:10, timezone:'+02:30'})",
                datetime( 2022, 3, 2, 19, 10, 0, 0, ZoneOffset.ofHoursMinutes( 2, 30 ) ) );
    }

    @ParameterizedTest( name = "{displayName} {0}" )
    @MethodSource( "classProvider" )
    public void shouldSendAndReceiveDateTimeWithTimeZoneOffset( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        initConnection( connectionClass );

        testSendingAndReceivingOfBoltV2Value( datetime( 1899, 1, 1, 12, 12, 32, 0, ZoneOffset.ofHoursMinutes( -4, -15 ) ) );
    }

    private <T extends AnyValue> void testSendingOfBoltV2Value( T value ) throws Exception
    {
        handshakeAndAuth();

        connection.send( util.defaultRunAutoCommitTx( "CREATE (n:Node {value: $value}) RETURN 42", map( new String[]{"value"}, new AnyValue[]{value} ) ) );

        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord(new Condition<>( v -> v.equals( longValue( 42 ) ), "equals condition" ) ) ),
                msgSuccess() ) );
    }

    private <T extends AnyValue> void testReceivingOfBoltV2Value( String query, T expectedValue ) throws Exception
    {
        handshakeAndAuth();

        connection.send( util.defaultRunAutoCommitTx( query ) );

        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( new Condition<>( v -> v.equals( expectedValue ), "equals condition" ) ) ),
                msgSuccess() ) );
    }

    private <T extends AnyValue> void testSendingAndReceivingOfBoltV2Value( T value ) throws Exception
    {
        handshakeAndAuth();

        connection.send( util.defaultRunAutoCommitTx( "RETURN $value", map( new String[]{"value"}, new AnyValue[]{value} ) ) );

        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( new Condition<>( v -> v.equals( value ), "equals condition" ) ) ),
                msgSuccess() ) );
    }

    private void handshakeAndAuth() throws Exception
    {
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() );

        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }
}
