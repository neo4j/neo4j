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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.testing.TestNotification;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.hasNotification;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgIgnored;
import static org.neo4j.bolt.testing.MessageConditions.msgRecord;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.StreamConditions.eqRecord;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceives;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class TransportSessionIT extends AbstractBoltTransportsTest
{
    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( getSettingsFunction() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldNegotiateProtocolVersion( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldReturnNilOnNoApplicableVersion( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.acceptedVersions( 1337, 0, 0, 0 ) );

        // Then
        assertThat( connection ).satisfies( eventuallyReceives( new byte[]{0, 0, 0, 0} ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldRunSimpleStatement( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() )
                .send( util.defaultRunAutoCommitTx( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess( message -> assertThat( message )
                        .containsKey( "t_first" ).containsEntry( "fields", asList( "a", "a_squared" ) ) ),
                msgRecord( eqRecord( longEquals( 1L ), longEquals( 1L ) ) ),
                msgRecord( eqRecord( longEquals( 2L ), longEquals( 4L ) ) ),
                msgRecord( eqRecord( longEquals( 3L ), longEquals( 9L ) ) ),
                msgSuccess( message -> assertThat( message )
                        .containsKey( "t_last" ).containsEntry( "type", "r" ) ) ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldRespondWithMetadataToDiscardAll( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() )
                .send( util.defaultRunAutoCommitTxWithoutResult( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess( message -> assertThat( message )
                        .containsKey("t_first" ).containsEntry( "fields", asList( "a", "a_squared" ) ) ),
                msgSuccess( message -> assertThat( message )
                        .containsKey("t_last" ).containsEntry( "type", "r" ) ) ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldBeAbleToRunQueryAfterAckFailure( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // Given
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() )
                .send( util.defaultRunAutoCommitTx( "QINVALID" ) );

        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgFailure( Status.Statement.SyntaxError,
                        String.format( "line 1, column 1" ) ), msgIgnored() ) );

        // When
        connection.send( util.defaultReset() ).send( util.defaultRunAutoCommitTx( "RETURN 1" ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( longEquals( 1L ) ) ),
                msgSuccess() ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldRunProcedure( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // Given
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() )
                .send( util.defaultRunAutoCommitTx( "CREATE (n:Test {age: 2}) RETURN n.age AS age" ) );

        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess( message -> assertThat( message )
                        .containsKey( "t_first" ).containsEntry( "fields", singletonList( "age" ) ) ),
                msgRecord( eqRecord( longEquals( 2L ) ) ),
                msgSuccess() ) );

        // When
        connection.send( util.defaultRunAutoCommitTx( "CALL db.labels() YIELD label" ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message )
                        .containsKey( "t_first" ).containsEntry( "fields", singletonList( "label" ) ) ),
                msgRecord( eqRecord( new Condition<>( v -> v.equals( stringValue( "Test" ) ), "Test value" ) ) ),
                msgSuccess()
        ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldHandleDeletedNodes( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() )
                .send( util.defaultRunAutoCommitTx( "CREATE (n:Test) DELETE n RETURN n" ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess( message -> assertThat( message )
                        .containsKey( "t_first" )
                        .containsEntry( "fields", singletonList( "n" ) ) ) ) );

        //
        //Record(0x71) {
        //    fields: [ Node(0x4E) {
        //                 id: 00
        //                 labels: [] (90)
        //                  props: {} (A)]
        //}
        assertThat( connection ).satisfies(
                eventuallyReceives( bytes( 0x00, 0x08, 0xB1, 0x71, 0x91,
                        0xB3, 0x4E, 0x00, 0x90, 0xA0, 0x00, 0x00 ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldHandleDeletedRelationships( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() )
                .send( util.defaultRunAutoCommitTx( "CREATE ()-[r:T {prop: 42}]->() DELETE r RETURN r" ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess( message -> assertThat( message )
                        .containsKey( "t_first" ).containsEntry( "fields", singletonList( "r" ) ) ) ) );

        //
        //Record(0x71) {
        //    fields: [ Relationship(0x52) {
        //                 relId: 00
        //                 startId: 00
        //                 endId: 01
        //                 type: "T" (81 54)
        //                 props: {} (A0)]
        //}
        assertThat( connection ).satisfies(
                eventuallyReceives( bytes( 0x00, 0x0B, 0xB1, 0x71, 0x91,
                        0xB5, 0x52, 0x00, 0x00, 0x01, 0x81, 0x54, 0xA0, 0x00, 0x00 ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldNotLeakStatsToNextStatement( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // Given
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() )
                .send( util.defaultRunAutoCommitTx( "CREATE (n)" ) );
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgSuccess() ) );

        // When
        connection.send( util.defaultRunAutoCommitTx( "RETURN 1" ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( longEquals( 1L ) ) ),
                msgSuccess( message -> assertThat( message )
                        .containsKey( "t_last" ).containsEntry( "type", "r" ) ) ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldSendNotifications( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() )
                .send( util.defaultRunAutoCommitTx( "EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)" ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
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

    private byte[] bytes( int... ints )
    {
        byte[] bytes = new byte[ints.length];
        for ( int i = 0; i < ints.length; i++ )
        {
            bytes[i] = (byte) ints[i];
        }
        return bytes;
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailNicelyOnNullKeysInMap( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        //Given
        Map<String,Object> params = new HashMap<>();
        Map<String,Object> inner = new HashMap<>();
        inner.put( null, 42L );
        inner.put( "foo", 1337L );
        params.put( "p", inner );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() )
                .send( util.defaultRunAutoCommitTx( "RETURN {p}", ValueUtils.asMapValue( params ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgFailure( Status.Request.Invalid,
                        "Value `null` is not supported as key in maps, must be a non-nullable string." ),
                msgIgnored() ) );

        connection.send( util.defaultReset() )
                .send( util.defaultRunAutoCommitTx( "RETURN 1" ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( longEquals( 1L ) ) ),
                msgSuccess() ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailNicelyWhenDroppingUnknownIndex(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() )
                .send( util.defaultRunAutoCommitTx( "DROP INDEX on :Movie12345(id)" ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgFailure( Status.Schema.IndexDropFailed,
                        "Unable to drop index on (:Movie12345 {id}). There is no such index." ),
                msgIgnored() ) );
    }

    private Condition<AnyValue> longEquals( long expected )
    {
        return new Condition<>( value -> value.equals( longValue( expected ) ), "long equals" );
    }
}
