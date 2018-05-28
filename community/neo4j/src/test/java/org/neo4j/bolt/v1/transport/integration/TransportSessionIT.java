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
package org.neo4j.bolt.v1.transport.integration;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.messaging.message.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.message.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.message.InitMessage;
import org.neo4j.bolt.v1.messaging.message.PullAllMessage;
import org.neo4j.bolt.v1.messaging.message.RunMessage;
import org.neo4j.bolt.v1.messaging.util.MessageMatchers;
import org.neo4j.bolt.v1.runtime.spi.StreamMatchers;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assume.assumeThat;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

public class TransportSessionIT extends AbstractBoltTransportsTest
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), settings ->
            settings.put( GraphDatabaseSettings.auth_enabled.name(), "false" ) );

    private HostnamePort address;

    @Before
    public void setup()
    {
        address = server.lookupDefaultConnector();
    }

    @Test
    public void shouldNegotiateProtocolVersion() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
    }

    @Test
    public void shouldReturnNilOnNoApplicableVersion() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.acceptedVersions( 1337, 0, 0, 0 ) );

        // Then
        MatcherAssert.assertThat( connection, TransportTestUtil.eventuallyReceives( new byte[]{0, 0, 0, 0} ) );
    }

    @Test
    public void shouldRunSimpleStatement() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        PullAllMessage.pullAll() ) );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        Matcher<Map<? extends String,?>> entryTypeMatcher = hasEntry( is( "type" ), equalTo( "r" ) );
        Matcher<Map<? extends String,?>> entryFieldMatcher = hasEntry( is( "fields" ), equalTo( asList( "a", "a_squared" ) ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess( CoreMatchers.allOf( entryFieldMatcher, hasKey( "result_available_after" ) ) ),
                MessageMatchers.msgRecord( StreamMatchers.eqRecord( equalTo( longValue( 1L ) ), equalTo( longValue( 1L ) ) ) ),
                MessageMatchers.msgRecord( StreamMatchers.eqRecord( equalTo( longValue( 2L ) ), equalTo( longValue( 4L ) ) ) ),
                MessageMatchers.msgRecord( StreamMatchers.eqRecord( equalTo( longValue( 3L ) ), equalTo( longValue( 9L ) ) ) ),
                MessageMatchers.msgSuccess( CoreMatchers.allOf( entryTypeMatcher,
                        hasKey( "result_consumed_after" ) ) ) ) );
    }

    @Test
    public void shouldRespondWithMetadataToDiscardAll() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        DiscardAllMessage.discardAll() ) );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        Matcher<Map<? extends String,?>> entryFieldsMatcher = hasEntry( is( "fields" ), equalTo( asList( "a", "a_squared" ) ) );
        Matcher<Map<? extends String,?>> entryTypeMatcher = hasEntry( is( "type" ), equalTo( "r" ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess( CoreMatchers.allOf( entryFieldsMatcher, hasKey( "result_available_after" ) ) ),
                MessageMatchers.msgSuccess( CoreMatchers.allOf( entryTypeMatcher, hasKey( "result_consumed_after" ) ) ) ) );
    }

    @Test
    public void shouldBeAbleToRunQueryAfterAckFailure() throws Throwable
    {
        // Given
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "QINVALID" ),
                        PullAllMessage.pullAll() ) );

        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgFailure( Status.Statement.SyntaxError,
                        String.format( "Invalid input 'Q': expected <init> (line 1, column 1 (offset: 0))%n" +
                                       "\"QINVALID\"%n" +
                                       " ^" ) ), MessageMatchers.msgIgnored() ) );

        // When
        connection.send( util.chunk( AckFailureMessage.ackFailure(), RunMessage.run( "RETURN 1" ), PullAllMessage.pullAll() ) );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgRecord( StreamMatchers.eqRecord( equalTo( longValue( 1L ) ) ) ),
                MessageMatchers.msgSuccess() ) );
    }

    @Test
    public void shouldRunProcedure() throws Throwable
    {
        // Given
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "CREATE (n:Test {age: 2}) RETURN n.age AS age" ),
                        PullAllMessage.pullAll() ) );

        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        Matcher<Map<? extends String,?>> ageMatcher = hasEntry( is( "fields" ), equalTo( singletonList( "age" ) ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess( CoreMatchers.allOf( ageMatcher, hasKey( "result_available_after" ) ) ),
                MessageMatchers.msgRecord( StreamMatchers.eqRecord( equalTo( longValue( 2L ) ) ) ),
                MessageMatchers.msgSuccess() ) );

        // When
        connection.send( util.chunk(
                RunMessage.run( "CALL db.labels() YIELD label" ),
                PullAllMessage.pullAll() ) );

        // Then
        Matcher<Map<? extends String,?>> entryFieldsMatcher = hasEntry( is( "fields" ), equalTo( singletonList( "label" ) ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess( CoreMatchers.allOf( entryFieldsMatcher,
                        hasKey( "result_available_after" ) ) ),
                MessageMatchers.msgRecord( StreamMatchers.eqRecord( Matchers.equalTo( stringValue( "Test" ) ) ) ),
                MessageMatchers.msgSuccess()
        ) );
    }

    @Test
    public void shouldHandleDeletedNodes() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "CREATE (n:Test) DELETE n RETURN n" ),
                        PullAllMessage.pullAll() ) );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        Matcher<Map<? extends String,?>> entryFieldsMatcher = hasEntry( is( "fields" ), equalTo( singletonList( "n" ) ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess( CoreMatchers.allOf( entryFieldsMatcher,
                        hasKey( "result_available_after" ) ) ) ) );

        //
        //Record(0x71) {
        //    fields: [ Node(0x4E) {
        //                 id: 00
        //                 labels: [] (90)
        //                  props: {} (A)]
        //}
        MatcherAssert.assertThat( connection,
                TransportTestUtil.eventuallyReceives( bytes( 0x00, 0x08, 0xB1, 0x71, 0x91,
                        0xB3, 0x4E, 0x00, 0x90, 0xA0, 0x00, 0x00 ) ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceives( MessageMatchers.msgSuccess() ) );
    }

    @Test
    public void shouldHandleDeletedRelationships() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "CREATE ()-[r:T {prop: 42}]->() DELETE r RETURN r" ),
                        PullAllMessage.pullAll() ) );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        Matcher<Map<? extends String,?>> entryFieldsMatcher = hasEntry( is( "fields" ), equalTo( singletonList( "r" ) ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess( CoreMatchers.allOf( entryFieldsMatcher,
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
        MatcherAssert.assertThat( connection,
                TransportTestUtil.eventuallyReceives( bytes( 0x00, 0x0B, 0xB1, 0x71, 0x91,
                        0xB5, 0x52, 0x00, 0x00, 0x01, 0x81, 0x54, 0xA0, 0x00, 0x00 ) ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceives( MessageMatchers.msgSuccess() ) );
    }

    @Test
    public void shouldNotLeakStatsToNextStatement() throws Throwable
    {
        // Given
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "CREATE (n)" ),
                        PullAllMessage.pullAll() ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess() ) );

        // When
        connection.send(
                util.chunk(
                        RunMessage.run( "RETURN 1" ),
                        PullAllMessage.pullAll() ) );

        // Then
        Matcher<Map<? extends String,?>> typeMatcher = hasEntry( is( "type" ), equalTo( "r" ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgRecord( StreamMatchers.eqRecord( equalTo( longValue( 1L ) ) ) ),
                MessageMatchers.msgSuccess( CoreMatchers.allOf( typeMatcher, hasKey( "result_consumed_after" ) ) ) ) );
    }

    @Test
    public void shouldSendNotifications() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)" ),
                        PullAllMessage.pullAll() ) );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess(),
                MessageMatchers.hasNotification(
                        new TestNotification( "Neo.ClientNotification.Statement.UnknownLabelWarning",
                                "The provided label is not in the database.",
                                "One of the labels in your query is not available in the database, " +
                                "make sure you didn't misspell it or that the label is available when " +
                                "you run this statement in your application (the missing label name is: " +
                                "THIS_IS_NOT_A_LABEL)",
                                SeverityLevel.WARNING, new InputPosition( 17, 1, 18 ) ) ) ) );

    }

    @Test
    public void shouldFailNicelyOnPointsWhenProtocolDoesNotSupportThem() throws Throwable
    {
        // only V1 protocol does not support points
        assumeThat( neo4jPack.version(), equalTo( Neo4jPackV1.VERSION ) );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "RETURN point({x:13, y:37, crs:'cartesian'}) as p" ),
                        PullAllMessage.pullAll() ) );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        Matcher<Map<? extends String,?>> fieldsMatcher = hasEntry( is( "fields" ), equalTo( singletonList( "p" ) ) );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess( CoreMatchers.allOf( fieldsMatcher, hasKey( "result_available_after" ) ) ),
                MessageMatchers.msgFailure( Status.Request.Invalid, "Point is not yet supported as a return type in Bolt" ) ) );
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
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "RETURN {p}", ValueUtils.asMapValue( params ) ),
                        PullAllMessage.pullAll() ) );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgFailure( Status.Request.Invalid,
                        "Value `null` is not supported as key in maps, must be a non-nullable string." ),
                MessageMatchers.msgIgnored() ) );

        connection.send( util.chunk( AckFailureMessage.ackFailure(), RunMessage.run( "RETURN 1" ), PullAllMessage.pullAll() ) );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgRecord( StreamMatchers.eqRecord( equalTo( longValue( 1L ) ) ) ),
                MessageMatchers.msgSuccess() ) );
    }

    @Test
    public void shouldFailNicelyWhenDroppingUnknownIndex() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk(
                        InitMessage.init( "TestClient/1.1", emptyMap() ),
                        RunMessage.run( "DROP INDEX on :Movie12345(id)" ),
                        PullAllMessage.pullAll() ) );

        // Then
        MatcherAssert.assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        MatcherAssert.assertThat( connection, util.eventuallyReceives(
                MessageMatchers.msgSuccess(),
                MessageMatchers.msgFailure( Status.Schema.IndexDropFailed,
                        "Unable to drop index on :Movie12345(id): No such INDEX ON :Movie12345(id)." ),
                MessageMatchers.msgIgnored() ) );
    }
}
