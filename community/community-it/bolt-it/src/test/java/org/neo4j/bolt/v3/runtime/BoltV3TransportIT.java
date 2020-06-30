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
package org.neo4j.bolt.v3.runtime;

import org.assertj.core.api.Condition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackedOutputArray;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.PullAllMessage;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.bolt.v3.runtime.bookmarking.BookmarkWithPrefix;
import org.neo4j.common.DependencyResolver;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgIgnored;
import static org.neo4j.bolt.testing.MessageConditions.msgRecord;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.StreamConditions.eqRecord;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.RollbackMessage.ROLLBACK_MESSAGE;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmark;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class BoltV3TransportIT extends BoltV3TransportBase
{
    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldNegotiateProtocolV3( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        connection.connect( address ).send( util.acceptedVersions( 3, 0, 0, 0 ) ).send(
                util.chunk( new HelloMessage( map( "user_agent", USER_AGENT ) ) ) );

        assertThat( connection ).satisfies( eventuallyReceives( new byte[]{0, 0, 0, 3} ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                        msgSuccess( message -> assertThat( message ).containsKeys( "server", "connection_id" ) ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldNegotiateProtocolV3WhenClientSupportsBothV1V2AndV3( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        connection.connect( address )
                .send( util.acceptedVersions( 3, 2, 1, 0 ) )
                .send( util.chunk( new HelloMessage( map( "user_agent", USER_AGENT ) ) ) );

        assertThat( connection ).satisfies( eventuallyReceives( new byte[]{0, 0, 0, 3} ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldRunSimpleStatement( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsKey( "t_first" )
                                    .containsEntry( "fields", asList( "a", "a_squared" ) ) ),
                msgRecord( eqRecord( longEquals( 1L ), longEquals( 1L ) ) ),
                msgRecord( eqRecord( longEquals( 2L ), longEquals( 4L ) ) ),
                msgRecord( eqRecord( longEquals( 3L ), longEquals( 9L ) ) ),
                msgSuccess( message -> assertThat( message )
                        .containsKeys( "bookmark", "t_last" )
                        .containsEntry( "type", "r" ) ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldRespondWithMetadataToDiscardAll( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                DiscardAllMessage.INSTANCE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message )
                        .containsKey( "t_first" ).containsEntry( "fields", asList( "a", "a_squared" ) ) ),
                msgSuccess( message -> assertThat( message )
                        .containsKeys( "t_last", "bookmark" ).containsEntry( "type", "r" ) ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldRunSimpleStatementInTx( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                new BeginMessage(),
                new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                PullAllMessage.INSTANCE,
                COMMIT_MESSAGE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess( message -> assertThat( message )
                        .containsKey("t_first" ).containsEntry( "fields", asList( "a", "a_squared" ) ) ),
                msgRecord( eqRecord( longEquals( 1L ), longEquals( 1L ) ) ),
                msgRecord( eqRecord( longEquals( 2L ), longEquals( 4L ) ) ),
                msgRecord( eqRecord( longEquals( 3L ), longEquals( 9L ) ) ),
                msgSuccess( message -> assertThat( message )
                        .containsKey("t_last" ).doesNotContainKey("bookmark" )
                        .containsEntry( "type", "r" ) ),
                msgSuccess( message -> assertThat( message ).containsKey("bookmark" ) ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldAllowRollbackSimpleStatementInTx( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                new BeginMessage(),
                new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                PullAllMessage.INSTANCE,
                ROLLBACK_MESSAGE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess( message -> assertThat( message )
                        .containsKey("t_first" ).containsEntry( "fields", asList( "a", "a_squared" ) ) ),
                msgRecord( eqRecord( longEquals( 1L ), longEquals( 1L ) ) ),
                msgRecord( eqRecord( longEquals( 2L ), longEquals( 4L ) ) ),
                msgRecord( eqRecord( longEquals( 3L ), longEquals( 9L ) ) ),
                msgSuccess( message -> assertThat( message )
                        .containsKey("t_last" ).doesNotContainKey( "bookmark" )
                        .containsEntry( "type", "r" ) ),
                msgSuccess() ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldBeAbleToRunQueryAfterReset( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "QINVALID" ),
                        PullAllMessage.INSTANCE ) );

        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgFailure( Status.Statement.SyntaxError,
                        String.format( "line 1, column 1" ) ),
                msgIgnored() ) );

        // When
        connection.send( util.chunk( ResetMessage.INSTANCE, new RunMessage( "RETURN 1" ), PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( longEquals(1L ) ) ),
                msgSuccess() ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldRunProcedure( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "CREATE (n:Test {age: 2}) RETURN n.age AS age" ),
                        PullAllMessage.INSTANCE ) );

        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message )
                        .containsKey("t_first" ).containsEntry( "fields", singletonList( "age" ) ) ),
                msgRecord( eqRecord( longEquals( 2L ) ) ),
                msgSuccess() ) );

        // When
        connection.send( util.chunk(
                new RunMessage( "CALL db.labels() YIELD label" ),
                PullAllMessage.INSTANCE ) );

        // Then
        final Condition<AnyValue> stringEquality = new Condition<>( value -> value.equals( stringValue( "Test" ) ), "String equals" );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message )
                        .containsKey("t_first" ).containsEntry( "fields", singletonList( "label" ) ) ),
                msgRecord( eqRecord( stringEquality ) ),
                msgSuccess()
        ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldHandleDeletedNodes( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "CREATE (n:Test) DELETE n RETURN n" ),
                        PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message )
                        .containsKey("t_first" )
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

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldHandleDeletedRelationships( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "CREATE ()-[r:T {prop: 42}]->() DELETE r RETURN r" ),
                        PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message )
                            .containsKey("t_first" )
                            .containsEntry( "fields", singletonList( "r" ) ) ) ) );

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

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldNotLeakStatsToNextStatement( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "CREATE (n)" ),
                        PullAllMessage.INSTANCE ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess() ) );

        // When
        connection.send(
                util.chunk(
                        new RunMessage( "RETURN 1" ),
                        PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgRecord( eqRecord( longEquals( 1L ) ) ),
                msgSuccess( message -> assertThat( message )
                                                .containsKey( "t_last" )
                                                .containsEntry( "type", "r" ) ) ) );
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

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailNicelyOnNullKeysInMap( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        //Given
        Map<String,Object> params = new HashMap<>();
        Map<String,Object> inner = new HashMap<>();
        inner.put( null, 42L );
        inner.put( "foo", 1337L );
        params.put( "p", inner );

        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                new RunMessage( "RETURN $p", asMapValue( params ) ),
                        PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgFailure( Status.Request.Invalid,
                        "Value `null` is not supported as key in maps, must be a non-nullable string." ),
                msgIgnored() ) );

        connection.send( util.chunk( ResetMessage.INSTANCE, new RunMessage( "RETURN 1" ), PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( longEquals( 1L ) ) ),
                msgSuccess() ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailNicelyWhenDroppingUnknownIndex( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // When
        negotiateBoltV3();
        connection.send( util.chunk(
                        new RunMessage( "DROP INDEX on :Movie12345(id)" ),
                        PullAllMessage.INSTANCE ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgFailure( Status.Schema.IndexDropFailed,
                        "Unable to drop index on (:Movie12345 {id}). There is no such index." ),
                msgIgnored() ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldSetTxMetadata( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        negotiateBoltV3();
        Map<String,Object> txMetadata = map( "who-is-your-boss", "Molly-mostly-white" );
        Map<String,Object> msgMetadata = map( "tx_metadata", txMetadata );
        MapValue meta = asMapValue( msgMetadata );

        connection.send( util.chunk(
                new BeginMessage( meta, List.of(), null, AccessMode.WRITE, txMetadata ),
                new RunMessage( "RETURN 1" ),
                PullAllMessage.INSTANCE ) );

        // When
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( longEquals( 1L ) ) ),
                msgSuccess() ) );

        // Then
        GraphDatabaseAPI gdb = (GraphDatabaseAPI) server.graphDatabaseService();
        Set<KernelTransactionHandle> txHandles = gdb.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
        assertThat( txHandles.size() ).isEqualTo( 1 );
        for ( KernelTransactionHandle txHandle: txHandles )
        {
            assertThat( txHandle.getMetaData() ).isEqualTo( txMetadata );
        }
        connection.send( util.chunk( ROLLBACK_MESSAGE ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldSendFailureMessageForBeginWithInvalidBookmark( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        negotiateBoltV3();
        String bookmarkString = "Not a good bookmark for BEGIN";
        Map<String,Object> metadata = map( "bookmarks", singletonList( bookmarkString ) );

        connection.send( util.chunk( 32, beginMessage( metadata ) ) );

        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( InvalidBookmark, bookmarkString ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldSendFailureMessageForBeginWithInvalidTransactionTimeout( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        negotiateBoltV3();
        String txTimeout = "Tx timeout can't be a string for BEGIN";
        Map<String,Object> metadata = map( "tx_timeout", txTimeout );

        connection.send( util.chunk( 32, beginMessage( metadata ) ) );

        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Request.Invalid, txTimeout ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldSendFailureMessageForBeginWithInvalidTransactionMetadata( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        negotiateBoltV3();
        String txMetadata = "Tx metadata can't be a string for BEGIN";
        Map<String,Object> metadata = map( "tx_metadata", txMetadata );

        connection.send( util.chunk( 32, beginMessage( metadata ) ) );

        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Request.Invalid, txMetadata ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldSendFailureMessageForRunWithInvalidBookmark( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        negotiateBoltV3();
        String bookmarkString = "Not a good bookmark for RUN";
        Map<String,Object> metadata = map( "bookmarks", singletonList( bookmarkString ) );

        connection.send( util.chunk( 32, runMessage( metadata ) ) );

        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( InvalidBookmark, bookmarkString ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldSendFailureMessageForRunWithInvalidTransactionTimeout( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        negotiateBoltV3();
        String txTimeout = "Tx timeout can't be a string for RUN";
        Map<String,Object> metadata = map( "tx_timeout", txTimeout );

        connection.send( util.chunk( 32, runMessage( metadata ) ) );

        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Request.Invalid, txTimeout ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldSendFailureMessageForRunWithInvalidTransactionMetadata( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        negotiateBoltV3();
        String txMetadata = "Tx metadata can't be a string for RUN";
        Map<String,Object> metadata = map( "tx_metadata", txMetadata );

        connection.send( util.chunk( 32, runMessage( metadata ) ) );

        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Request.Invalid, txMetadata ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldReturnUpdatedBookmarkAfterAutoCommitTransaction( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        assumeFalse( FabricDatabaseManager.fabricByDefault() );

        negotiateBoltV3();

        // bookmark is expected to advance once the auto-commit transaction is committed
        long lastClosedTransactionId = getLastClosedTransactionId();
        String expectedBookmark = new BookmarkWithPrefix( lastClosedTransactionId + 1 ).toString();

        connection.send( util.chunk(
                new RunMessage( "CREATE ()" ),
                PullAllMessage.INSTANCE ) );

        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess( message -> assertThat( message ).containsEntry("bookmark", expectedBookmark ) ) ) );
    }

    private Condition<AnyValue> longEquals( long expected )
    {
        return new Condition<>( value -> value.equals( longValue( expected ) ), "long equals" );
    }

    private byte[] beginMessage( Map<String,Object> metadata ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = util.getNeo4jPack().newPacker( out );

        packer.packStructHeader( 1, BeginMessage.SIGNATURE );
        packer.pack( asMapValue( metadata ) );

        return out.bytes();
    }

    private byte[] runMessage( Map<String,Object> metadata ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = util.getNeo4jPack().newPacker( out );

        packer.packStructHeader( 3, RunMessage.SIGNATURE );
        packer.pack( "RETURN 1" );
        packer.pack( EMPTY_MAP );
        packer.pack( asMapValue( metadata ) );

        return out.bytes();
    }

    private long getLastClosedTransactionId()
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver();
        TransactionIdStore txIdStore = resolver.resolveDependency( TransactionIdStore.class );
        return txIdStore.getLastClosedTransactionId();
    }
}
