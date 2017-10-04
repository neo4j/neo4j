/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashMap;

import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.ValueUtils;
import org.neo4j.kernel.api.exceptions.Status;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.neo4j.bolt.v1.messaging.message.AckFailureMessage.ackFailure;
import static org.neo4j.bolt.v1.messaging.message.DiscardAllMessage.discardAll;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.hasNotification;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgIgnored;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( "unchecked" )
@RunWith( Parameterized.class )
public class TransportSessionIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), settings ->
            settings.put( GraphDatabaseSettings.auth_enabled.name(), "false" ) );

    @Parameterized.Parameter( 0 )
    public Factory<TransportConnection> cf;

    private HostnamePort address;

    private TransportConnection client;

    @Parameterized.Parameters
    public static Collection<Factory<TransportConnection>> transports()
    {
        return asList( SocketConnection::new, WebSocketConnection::new, SecureSocketConnection::new,
                SecureWebSocketConnection::new );
    }

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
        this.address = server.lookupDefaultConnector();
    }

    @After
    public void tearDown() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    @Test
    public void shouldNegotiateProtocolVersion() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
    }

    @Test
    public void shouldReturnNilOnNoApplicableVersion() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1337, 0, 0, 0 ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 0} ) );
    }

    @Test
    public void shouldRunSimpleStatement() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgSuccess(
                        allOf( hasEntry( is( "fields" ), equalTo( asList( "a", "a_squared" ) ) ),
                                hasKey( "result_available_after" ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ), equalTo( longValue( 1L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ), equalTo( longValue( 4L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 3L ) ), equalTo( longValue( 9L ) ) ) ),
                msgSuccess( allOf( hasEntry( is( "type" ), equalTo( "r" ) ),
                        hasKey( "result_consumed_after" ) ) ) ) );
    }

    @Test
    public void shouldRespondWithMetadataToDiscardAll() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        discardAll() ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgSuccess(
                        allOf( hasEntry( is( "fields" ), equalTo( asList( "a", "a_squared" ) ) ),
                                hasKey( "result_available_after" ) ) ),
                msgSuccess( allOf( hasEntry( is( "type" ), equalTo( "r" ) ),
                        hasKey( "result_consumed_after" ) ) ) ) );
    }

    @Test
    public void shouldBeAbleToRunQueryAfterAckFailure() throws Throwable
    {
        // Given
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "QINVALID" ),
                        pullAll() ) );

        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgFailure( Status.Statement.SyntaxError,
                        String.format( "Invalid input 'Q': expected <init> (line 1, column 1 (offset: 0))%n" +
                                       "\"QINVALID\"%n" +
                                       " ^" ) ), msgIgnored() ) );

        // When
        client.send( TransportTestUtil.chunk( ackFailure(), run( "RETURN 1" ), pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ) ) ),
                msgSuccess() ) );
    }

    @Test
    public void shouldRunProcedure() throws Throwable
    {
        // Given
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "CREATE (n:Test {age: 2}) RETURN n.age AS age" ),
                        pullAll() ) );

        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgSuccess( allOf( hasEntry( is( "fields" ), equalTo( singletonList( "age" ) ) ),
                        hasKey( "result_available_after" ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ) ) ),
                msgSuccess() ) );

        // When
        client.send( TransportTestUtil.chunk(
                run( "CALL db.labels() YIELD label" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgSuccess( allOf( hasEntry( is( "fields" ), equalTo( singletonList( "label" ) ) ),
                        hasKey( "result_available_after" ) ) ),
                msgRecord( eqRecord( Matchers.equalTo( stringValue( "Test" ) ) ) ),
                msgSuccess()
        ) );
    }

    @Test
    public void shouldHandleDeletedNodes() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "CREATE (n:Test) DELETE n RETURN n" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgSuccess( allOf( hasEntry( is( "fields" ), equalTo( singletonList( "n" ) ) ),
                        hasKey( "result_available_after" ) ) ) ) );

        //
        //Record(0x71) {
        //    fields: [ Node(0x4E) {
        //                 id: 00
        //                 labels: [] (90)
        //                  props: {} (A)]
        //}
        assertThat( client,
                eventuallyReceives( bytes( 0x00, 0x08, 0xB1, 0x71, 0x91,
                        0xB3, 0x4E, 0x00, 0x90, 0xA0, 0x00, 0x00 ) ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldHandleDeletedRelationships() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "CREATE ()-[r:T {prop: 42}]->() DELETE r RETURN r" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgSuccess( allOf( hasEntry( is( "fields" ), equalTo( singletonList( "r" ) ) ),
                        hasKey( "result_available_after" ) ) ) ) );

        //
        //Record(0x71) {
        //    fields: [ Relationship(0x52) {
        //                 relId: 00
        //                 startId: 00
        //                 endId: 01
        //                 type: "T" (81 54)
        //                 props: {} (A0)]
        //}
        assertThat( client,
                eventuallyReceives( bytes( 0x00, 0x0B, 0xB1, 0x71, 0x91,
                        0xB5, 0x52, 0x00, 0x00, 0x01, 0x81, 0x54, 0xA0, 0x00, 0x00 ) ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldNotLeakStatsToNextStatement() throws Throwable
    {
        // Given
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "CREATE (n)" ),
                        pullAll() ) );
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgSuccess() ) );

        // When
        client.send(
                TransportTestUtil.chunk(
                        run( "RETURN 1" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ) ) ),
                msgSuccess( allOf( hasEntry( is( "type" ), equalTo( "r" ) ),
                        hasKey( "result_consumed_after" ) ) ) ) );
    }

    @Test
    public void shouldSendNotifications() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                hasNotification(
                        new TestNotification( "Neo.ClientNotification.Statement.UnknownLabelWarning",
                                "The provided label is not in the database.",
                                "One of the labels in your query is not available in the database, " +
                                "make sure you didn't misspell it or that the label is available when " +
                                "you run this statement in your application (the missing label name is: " +
                                "THIS_IS_NOT_A_LABEL)",
                                SeverityLevel.WARNING, new InputPosition( 17, 1, 18 ) ) ) ) );

    }

    @Test
    public void shouldFailNicelyOnPoints() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "RETURN point({x:13, y:37, crs:'cartesian'}) as p" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgSuccess( allOf( hasEntry( is( "fields" ), equalTo( singletonList( "p" ) ) ),
                        hasKey( "result_available_after" ) ) ),
                msgRecord( eqRecord( equalTo( NO_VALUE ) ) ),
                msgFailure( Status.Request.Invalid, "Point is not yet supported as a return type in Bolt" ) ) );
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
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "RETURN {p}", ValueUtils.asMapValue( params ) ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgFailure( Status.Request.Invalid,
                        "Value `null` is not supported as key in maps, must be a non-nullable string." ),
                msgIgnored() ) );

        client.send( TransportTestUtil.chunk( ackFailure(), run( "RETURN 1" ), pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ) ) ),
                msgSuccess() ) );
    }

    @Test
    public void shouldFailNicelyWhenDroppingUnknownIndex() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", emptyMap() ),
                        run( "DROP INDEX on :Movie12345(id)" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives(
                msgSuccess(),
                msgFailure( Status.Schema.IndexDropFailed,
                        "Unable to drop index on :Movie12345(id): No such INDEX ON :Movie12345(id)." ),
                msgIgnored() ) );
    }
}
