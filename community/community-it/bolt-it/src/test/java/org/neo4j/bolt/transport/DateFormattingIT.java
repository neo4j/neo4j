/*
 * Copyright (c) "Neo4j"
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.ResponseMessage;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.Neo4jPackV2;
import org.neo4j.bolt.packstream.Neo4jPackV3;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SecureWebSocketConnection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.WebSocketConnection;
import org.neo4j.bolt.v3.messaging.request.CommitMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.RollbackMessage;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.bolt.v4.messaging.PullMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId;
import org.neo4j.bolt.v44.messaging.request.RouteMessage;
import org.neo4j.bolt.v44.BoltProtocolV44;
import org.neo4j.bolt.v44.BoltProtocolV44ComponentFactory;
import org.neo4j.configuration.Config;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static java.rmi.Naming.list;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgRecord;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.MessageConditions.serialize;
import static org.neo4j.bolt.testing.StreamConditions.eqRecord;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.transport.BoltPatchListener.UTC_PATCH;
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class DateFormattingIT
{
    private static final String USER_AGENT = "TestClient/4.4";

    @Inject
    public Neo4jWithSocket server;

    private HostnamePort address;
    private TransportConnection connection;

    @BeforeEach
    public void setUp( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( withOptionalBoltEncryption() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
    }

    protected void init( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        connection = connectionClass.getDeclaredConstructor().newInstance();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    @ParameterizedTest
    @MethodSource( "zoneArguments" )
    public void shouldReturnUTCFormattedZoneDates( ZonedDateTime zonedDateTime )
            throws Exception
    {
        init( SocketConnection.class );

        var packV3Util = new TransportTestUtil( new Neo4jPackV3() );
        negotiateBoltV44WithPatch( packV3Util );

        connection.send( packV3Util.chunk( new RunMessage( "RETURN $p", parameterMap( zonedDateTime ) ) ) );
        connection.send( packV3Util.chunk( new PullMessage( asMapValue( singletonMap( "n", -1L ) ) ) ) );

        assertThat( connection ).satisfies( packV3Util.eventuallyReceives( msgSuccess(),
                                                                           msgRecord( eqRecord( dateTimeEquals( zonedDateTime ) ) ), msgSuccess() ) );
    }

    @ParameterizedTest
    @MethodSource( "offsetArguments" )
    public void shouldReturnUTCFormattedOffsetDates( ZonedDateTime zonedDateTime )
            throws Exception
    {
        init( SocketConnection.class );

        var packV3Util = new TransportTestUtil( new Neo4jPackV3() );
        negotiateBoltV44WithPatch( packV3Util );

        connection.send( packV3Util.chunk( new RunMessage( "RETURN $p", parameterMap( zonedDateTime ) ) ) );
        connection.send( packV3Util.chunk( new PullMessage( asMapValue( singletonMap( "n", -1L ) ) ) ) );

        assertThat( connection ).satisfies( packV3Util.eventuallyReceives( msgSuccess(),
                                                                           msgRecord( eqRecord( dateTimeEquals( zonedDateTime ) ) ), msgSuccess() ) );
    }

    @Test
    public void shouldErrorIfPatchNegotiatedButNonUTCDatesAreSent()
            throws Exception
    {
        init( SocketConnection.class );

        var packV2Util = new TransportTestUtil( new Neo4jPackV2() );
        negotiateBoltV44WithPatch( packV2Util );

        connection.send( packV2Util.chunk( new RunMessage( "RETURN $p", parameterMap( ZonedDateTime.now() ) ) ) );

        assertThat( connection ).satisfies( packV2Util.eventuallyReceives( msgFailure() ) );
    }

    @Test
    public void shouldErrorIfPatchedDatesAreSentButNotNegotiated()
            throws Exception
    {
        init( SocketConnection.class );

        var packV3Util = new TransportTestUtil( new Neo4jPackV3() );
        negotiateBoltV44WithoutPatch( packV3Util );

        connection.send( packV3Util.chunk( new RunMessage( "RETURN $p", parameterMap( ZonedDateTime.now() ) ) ) );

        assertThat( connection ).satisfies( packV3Util.eventuallyReceives( msgFailure() ) );
    }

    private static Stream<Arguments> zoneArguments()
    {
        return Stream.of(
                Arguments.of(
                        ZonedDateTime.of( 1978, 12, 16, 12, 35, 59, 128000987, ZoneId.of( "Europe/Istanbul" ) ) ),
                Arguments.of(
                        ZonedDateTime.of( 2022, 6, 14, 15, 21, 18, 183_000_000, ZoneId.of( "Europe/Berlin" ) ) ),
                Arguments.of(
                        ZonedDateTime.of( 2022, 6, 14, 22, 6, 18, 183_000_000, ZoneId.of( "Australia/Eucla" ) ) ),
                Arguments.of(
                        ZonedDateTime.of( 2020, 6, 15, 4, 30, 0, 183_000_000, ZoneId.of( "Pacific/Honolulu" ) ) ) );
    }

    private static Stream<Arguments> offsetArguments()
    {
        return Stream.of(
                Arguments.of(
                        ZonedDateTime.of( 1978, 12, 16, 10, 5, 59, 128000987, ZoneOffset.ofTotalSeconds( -150 * 60 ) ) ),
                Arguments.of(
                        ZonedDateTime.of( 2022, 6, 14, 15, 21, 18, 183_000_000, ZoneOffset.ofTotalSeconds( 120 * 60 ) ) ),
                Arguments.of(
                        ZonedDateTime.of( 2020, 6, 15, 12, 30, 0, 42, ZoneOffset.ofTotalSeconds( -2 * 60 * 60 ) ) ) );
    }

    private void negotiateBoltV44WithPatch( TransportTestUtil util ) throws Exception
    {
        connection.connect( address ).send( TransportTestUtil.acceptedVersions( BoltProtocolV44.VERSION.toInt(), 0, 0, 0 ) );
        assertThat( connection ).satisfies( eventuallyReceives( new byte[]{0, 0, 4, 4} ) );

        connection.send( util.chunk( new HelloMessage( map( "user_agent", USER_AGENT, "patch_bolt", List.of( UTC_PATCH ) ) ) ) );
        assertThat( connection ).satisfies(
                util.eventuallyReceives( msgSuccess( msg -> assertThat( msg ).containsEntry( "patch_bolt", List.of( UTC_PATCH ) ) ) ) );
    }

    private void negotiateBoltV44WithoutPatch( TransportTestUtil util ) throws Exception
    {
        connection.connect( address ).send( TransportTestUtil.acceptedVersions( BoltProtocolV44.VERSION.toInt(), 0, 0, 0 ) );
        assertThat( connection ).satisfies( eventuallyReceives( new byte[]{0, 0, 4, 4} ) );

        connection.send( util.chunk( new HelloMessage( map( "user_agent", USER_AGENT ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess( msg -> assertThat( msg ).doesNotContainKey( "patch_bolt" ) ) ) );
    }

    private static MapValue parameterMap( ZonedDateTime zonedDateTime )
    {
        ZonedDateTime[] dateTimes = new ZonedDateTime[]{zonedDateTime};
        return VirtualValues.map( new String[]{"p"}, new AnyValue[]{Values.dateTimeArray( dateTimes )} );
    }

    private static Condition<AnyValue> dateTimeEquals( ZonedDateTime expected )
    {
        return new Condition<>( value -> value.equals( Values.dateTimeArray( new ZonedDateTime[]{expected} ) ), "Temporal equals" );
    }
}
