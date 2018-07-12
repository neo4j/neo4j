/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgIgnored;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.v3.messaging.BoltProtocolV3ComponentFactory.newMessageEncoder;
import static org.neo4j.bolt.v3.messaging.BoltProtocolV3ComponentFactory.newNeo4jPack;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.RollbackMessage.ROLLBACK_MESSAGE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

@RunWith( Parameterized.class )
public class BoltV3TransportIT
{
    private static final String USER_AGENT = "TestClient/3.0";

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
        return asList( SocketConnection.class );
                //, WebSocketConnection.class, SecureSocketConnection.class, SecureWebSocketConnection.class );
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

    @Test
    public void shouldNegotiateProtocolV3() throws Exception
    {
        connection.connect( address ).send( util.acceptedVersions( 3, 0, 0, 0 ) ).send(
                util.chunk( new HelloMessage( MapUtil.map( "user_agent", USER_AGENT ) ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 3} ) );
        Matcher<Map<? extends String,?>> entryRoutingTableMatcher = hasEntry( is( "routing_table" ), equalTo( "dbms.cluster.routing.getRoutingTable" ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess( allOf( hasKey( "server" ), hasKey( "connection_id" ), entryRoutingTableMatcher ) ) ) );
    }

    @Test
    public void shouldNegotiateProtocolV2WhenClientSupportsBothV1V2AndV3() throws Exception
    {
        connection.connect( address )
                .send( util.acceptedVersions( 3, 2, 1, 0 ) )
                .send( util.chunk( new HelloMessage( MapUtil.map( "user_agent", USER_AGENT ) ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 3} ) );
    }

    @Test
    public void shouldRunSimpleStatement() throws Throwable
    {
        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        PullAllMessage.INSTANCE ) );

        // Then
        Matcher<Map<? extends String,?>> entryFieldMatcher = hasEntry( is( "fields" ), equalTo( asList( "a", "a_squared" ) ) );
        Matcher<Map<? extends String,?>> entryTypeMatcher = hasEntry( is( "type" ), equalTo( "r" ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( entryFieldMatcher, hasKey( "tx_id" ), hasKey( "result_available_after" ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ), equalTo( longValue( 1L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ), equalTo( longValue( 4L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 3L ) ), equalTo( longValue( 9L ) ) ) ),
                msgSuccess( allOf( entryTypeMatcher, hasKey( "result_consumed_after" ) ) ) ) );
    }

    @Test
    public void shouldRunSimpleStatementInTx() throws Throwable
    {
        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                new BeginMessage(),
                new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                PullAllMessage.INSTANCE,
                COMMIT_MESSAGE ) );

        // Then
        Matcher<Map<? extends String,?>> entryFieldMatcher = hasEntry( is( "fields" ), equalTo( asList( "a", "a_squared" ) ) );
        Matcher<Map<? extends String,?>> entryTypeMatcher = hasEntry( is( "type" ), equalTo( "r" ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( hasKey( "tx_id" ) ) ),
                msgSuccess( allOf( entryFieldMatcher, not( hasKey( "tx_id" ) ), hasKey( "result_available_after" ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ), equalTo( longValue( 1L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ), equalTo( longValue( 4L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 3L ) ), equalTo( longValue( 9L ) ) ) ),
                msgSuccess( allOf( entryTypeMatcher, hasKey( "result_consumed_after" ) ) ),
                msgSuccess( allOf( hasKey( "bookmark" ) ) ) ) );
    }

    @Test
    public void shouldAllowRollbackSimpleStatementInTx() throws Throwable
    {
        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                new BeginMessage(),
                new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                PullAllMessage.INSTANCE,
                ROLLBACK_MESSAGE ) );

        // Then
        Matcher<Map<? extends String,?>> entryFieldMatcher = hasEntry( is( "fields" ), equalTo( asList( "a", "a_squared" ) ) );
        Matcher<Map<? extends String,?>> entryTypeMatcher = hasEntry( is( "type" ), equalTo( "r" ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( hasKey( "tx_id" ) ) ),
                msgSuccess( allOf( entryFieldMatcher, not( hasKey( "tx_id" ) ), hasKey( "result_available_after" ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ), equalTo( longValue( 1L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ), equalTo( longValue( 4L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 3L ) ), equalTo( longValue( 9L ) ) ) ),
                msgSuccess( allOf( entryTypeMatcher, hasKey( "result_consumed_after" ) ) ),
                msgSuccess() ) );
    }

    @Test
    public void shouldRespondWithMetadataToDiscardAll() throws Throwable
    {
        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        DiscardAllMessage.INSTANCE ) );

        // Then
        Matcher<Map<? extends String,?>> entryTypeMatcher = hasEntry( is( "type" ), equalTo( "r" ) );
        Matcher<Map<? extends String,?>> entryFieldsMatcher = hasEntry( is( "fields" ), equalTo( asList( "a", "a_squared" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( entryFieldsMatcher, hasKey( "tx_id" ), hasKey( "result_available_after" ) ) ),
                msgSuccess( allOf( entryTypeMatcher, hasKey( "result_consumed_after" ) ) ) ) );
    }

    @Test
    public void shouldBeAbleToRunQueryAfterReset() throws Throwable
    {
        // Given
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "QINVALID" ),
                        PullAllMessage.INSTANCE ) );

        assertThat( connection, util.eventuallyReceives(
                msgFailure( Status.Statement.SyntaxError,
                        String.format( "Invalid input 'Q': expected <init> (line 1, column 1 (offset: 0))%n" +
                                "\"QINVALID\"%n" +
                                " ^" ) ),
                msgIgnored() ) );

        // When
        connection.send( util.chunk( ResetMessage.INSTANCE, new RunMessage( "RETURN 1" ), PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection, util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ) ) ),
                msgSuccess() ) );
    }

    @Test
    public void shouldRunProcedure() throws Throwable
    {
        // Given
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "CREATE (n:Test {age: 2}) RETURN n.age AS age" ),
                        PullAllMessage.INSTANCE ) );

        Matcher<Map<? extends String,?>> ageMatcher = hasEntry( is( "fields" ), equalTo( singletonList( "age" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( ageMatcher, hasKey( "tx_id" ), hasKey( "result_available_after" ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ) ) ),
                msgSuccess() ) );

        // When
        connection.send( util.chunk(
                new RunMessage( "CALL db.labels() YIELD label" ),
                PullAllMessage.INSTANCE ) );

        // Then
        Matcher<Map<? extends String,?>> entryFieldsMatcher = hasEntry( is( "fields" ), equalTo( singletonList( "label" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( entryFieldsMatcher, hasKey( "tx_id" ), hasKey( "result_available_after" ) ) ),
                msgRecord( eqRecord( Matchers.equalTo( stringValue( "Test" ) ) ) ),
                msgSuccess()
        ) );
    }

    @Test
    public void shouldHandleDeletedNodes() throws Throwable
    {
        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "CREATE (n:Test) DELETE n RETURN n" ),
                        PullAllMessage.INSTANCE ) );

        // Then
        Matcher<Map<? extends String,?>> entryFieldsMatcher = hasEntry( is( "fields" ), equalTo( singletonList( "n" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( entryFieldsMatcher, hasKey("tx_id"), hasKey( "result_available_after" ) ) ) ) );

        //
        //Record(0x71) {
        //    fields: [ Node(0x4E) {
        //                 id: 00
        //                 labels: [] (90)
        //                  props: {} (A)]
        //}
        assertThat( connection,
                eventuallyReceives( bytes( 0x00, 0x08, 0xB1, 0x71, 0x91,
                        0xB3, 0x4E, 0x00, 0x90, 0xA0, 0x00, 0x00 ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldHandleDeletedRelationships() throws Throwable
    {
        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "CREATE ()-[r:T {prop: 42}]->() DELETE r RETURN r" ),
                        PullAllMessage.INSTANCE ) );

        // Then
        Matcher<Map<? extends String,?>> entryFieldsMatcher = hasEntry( is( "fields" ), equalTo( singletonList( "r" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( entryFieldsMatcher, hasKey( "tx_id" ), hasKey( "result_available_after" ) ) ) ) );

        //
        //Record(0x71) {
        //    fields: [ Relationship(0x52) {
        //                 relId: 00
        //                 startId: 00
        //                 endId: 01
        //                 type: "T" (81 54)
        //                 props: {} (A0)]
        //}
        assertThat( connection,
                eventuallyReceives( bytes( 0x00, 0x0B, 0xB1, 0x71, 0x91,
                        0xB5, 0x52, 0x00, 0x00, 0x01, 0x81, 0x54, 0xA0, 0x00, 0x00 ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldNotLeakStatsToNextStatement() throws Throwable
    {
        // Given
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "CREATE (n)" ),
                        PullAllMessage.INSTANCE ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess(),
                msgSuccess() ) );

        // When
        connection.send(
                util.chunk(
                        new RunMessage( "RETURN 1" ),
                        PullAllMessage.INSTANCE ) );

        // Then
        Matcher<Map<? extends String,?>> typeMatcher = hasEntry( is( "type" ), equalTo( "r" ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ) ) ),
                msgSuccess( allOf( typeMatcher, hasKey( "result_consumed_after" ) ) ) ) );
    }

    private byte[] bytes( int... ints )
    {
        byte[] bytes = new byte[ints.length];
        for ( int i = 0; i < ints.length; i++ )
        {
            bytes[i] = (byte) ints[i];
        }
        return bytes;
    }

    @Test
    public void shouldFailNicelyOnNullKeysInMap() throws Throwable
    {
        //Given
        HashMap<String,Object> params = new HashMap<>();
        HashMap<String,Object> inner = new HashMap<>();
        inner.put( null, 42L );
        inner.put( "foo", 1337L );
        params.put( "p", inner );

        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "RETURN {p}", ValueUtils.asMapValue( params ) ),
                        PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection, util.eventuallyReceives(
                msgFailure( Status.Request.Invalid,
                        "Value `null` is not supported as key in maps, must be a non-nullable string." ),
                msgIgnored() ) );

        connection.send( util.chunk( ResetMessage.INSTANCE, new RunMessage( "RETURN 1" ), PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection, util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ) ) ),
                msgSuccess() ) );
    }

    @Test
    public void shouldFailNicelyWhenDroppingUnknownIndex() throws Throwable
    {
        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "DROP INDEX on :Movie12345(id)" ),
                        PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection, util.eventuallyReceives(
                msgFailure( Status.Schema.IndexDropFailed,
                        "Unable to drop index on :Movie12345(id): No such INDEX ON :Movie12345(id)." ),
                msgIgnored() ) );
    }

    private void negotiateBoltV3() throws Exception
    {
        connection.connect( address )
                .send( util.acceptedVersions( 3, 0, 0, 0 ) )
                .send( util.chunk( new HelloMessage( MapUtil.map( "user_agent", USER_AGENT ) ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 3} ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }
}
