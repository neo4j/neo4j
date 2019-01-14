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
package org.neo4j.bolt.v3.runtime.integration;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.virtual.MapValue;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgIgnored;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.RollbackMessage.ROLLBACK_MESSAGE;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class BoltV3TransportIT extends BoltV3TransportBase
{
    @Test
    public void shouldNegotiateProtocolV3() throws Exception
    {
        connection.connect( address ).send( util.acceptedVersions( 3, 0, 0, 0 ) ).send(
                util.chunk( new HelloMessage( map( "user_agent", USER_AGENT ) ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 3} ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess( allOf( hasKey( "server" ), hasKey( "connection_id" ) ) ) ) );
    }

    @Test
    public void shouldNegotiateProtocolV3WhenClientSupportsBothV1V2AndV3() throws Exception
    {
        connection.connect( address )
                .send( util.acceptedVersions( 3, 2, 1, 0 ) )
                .send( util.chunk( new HelloMessage( map( "user_agent", USER_AGENT ) ) ) );

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
                msgSuccess( allOf( entryFieldMatcher, hasKey( "t_first" ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ), equalTo( longValue( 1L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ), equalTo( longValue( 4L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 3L ) ), equalTo( longValue( 9L ) ) ) ),
                msgSuccess( allOf( entryTypeMatcher, hasKey( "t_last" ), hasKey( "bookmark" ) ) ) ) );
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
                msgSuccess( allOf( entryFieldsMatcher, hasKey( "t_first" ) ) ),
                msgSuccess( allOf( entryTypeMatcher, hasKey( "t_last" ), hasKey( "bookmark" ) ) ) ) );
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
                msgSuccess(),
                msgSuccess( allOf( entryFieldMatcher, hasKey( "t_first" ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ), equalTo( longValue( 1L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ), equalTo( longValue( 4L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 3L ) ), equalTo( longValue( 9L ) ) ) ),
                msgSuccess( allOf( entryTypeMatcher, hasKey( "t_last" ), not( hasKey( "bookmark" ) ) ) ),
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
                msgSuccess(),
                msgSuccess( allOf( entryFieldMatcher, hasKey( "t_first" ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ), equalTo( longValue( 1L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ), equalTo( longValue( 4L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 3L ) ), equalTo( longValue( 9L ) ) ) ),
                msgSuccess( allOf( entryTypeMatcher, hasKey( "t_last" ), not( hasKey( "bookmark" ) ) ) ),
                msgSuccess() ) );
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
                msgSuccess( allOf( ageMatcher, hasKey( "t_first" ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ) ) ),
                msgSuccess() ) );

        // When
        connection.send( util.chunk(
                new RunMessage( "CALL db.labels() YIELD label" ),
                PullAllMessage.INSTANCE ) );

        // Then
        Matcher<Map<? extends String,?>> entryFieldsMatcher = hasEntry( is( "fields" ), equalTo( singletonList( "label" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( entryFieldsMatcher, hasKey( "t_first" ) ) ),
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
                msgSuccess( allOf( entryFieldsMatcher, hasKey( "t_first" ) ) ) ) );

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
                msgSuccess( allOf( entryFieldsMatcher, hasKey( "t_first" ) ) ) ) );

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
                msgSuccess( allOf( typeMatcher, hasKey( "t_last" ) ) ) ) );
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
                new RunMessage( "RETURN {p}", asMapValue( params ) ),
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

    @Test
    public void shouldSetTxMetadata() throws Throwable
    {
        // Given
        negotiateBoltV3();
        Map<String,Object> txMetadata = map( "who-is-your-boss", "Molly-mostly-white" );
        Map<String,Object> msgMetadata = map( "tx_metadata", txMetadata );
        MapValue meta = asMapValue( msgMetadata );

        connection.send( util.chunk(
                new BeginMessage( meta ),
                new RunMessage( "RETURN 1" ),
                PullAllMessage.INSTANCE ) );

        // When
        assertThat( connection, util.eventuallyReceives(
                msgSuccess(),
                msgSuccess(),
                msgRecord( eqRecord( equalTo( longValue( 1L ) ) ) ),
                msgSuccess() ) );

        // Then
        GraphDatabaseAPI gdb = (GraphDatabaseAPI) server.graphDatabaseService();
        Set<KernelTransactionHandle> txHandles = gdb.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
        assertThat( txHandles.size(), equalTo( 1 ) );
        for ( KernelTransactionHandle txHandle: txHandles )
        {
            assertThat( txHandle.getMetaData(), equalTo( txMetadata ) );
        }
        connection.send( util.chunk( ROLLBACK_MESSAGE ) );
    }

    @Test
    public void shouldSendFailureMessageForBeginWithInvalidBookmark() throws Exception
    {
        negotiateBoltV3();
        String bookmarkString = "Not a good bookmark for BEGIN";
        Map<String,Object> metadata = map( "bookmarks", singletonList( bookmarkString ) );

        connection.send( util.chunk( 32, beginMessage( metadata ) ) );

        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Transaction.InvalidBookmark, bookmarkString ) ) );
    }

    @Test
    public void shouldSendFailureMessageForBeginWithInvalidTransactionTimeout() throws Exception
    {
        negotiateBoltV3();
        String txTimeout = "Tx timeout can't be a string for BEGIN";
        Map<String,Object> metadata = map( "tx_timeout", txTimeout );

        connection.send( util.chunk( 32, beginMessage( metadata ) ) );

        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Request.Invalid, txTimeout ) ) );
    }

    @Test
    public void shouldSendFailureMessageForBeginWithInvalidTransactionMetadata() throws Exception
    {
        negotiateBoltV3();
        String txMetadata = "Tx metadata can't be a string for BEGIN";
        Map<String,Object> metadata = map( "tx_metadata", txMetadata );

        connection.send( util.chunk( 32, beginMessage( metadata ) ) );

        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Request.Invalid, txMetadata ) ) );
    }

    @Test
    public void shouldSendFailureMessageForRunWithInvalidBookmark() throws Exception
    {
        negotiateBoltV3();
        String bookmarkString = "Not a good bookmark for RUN";
        Map<String,Object> metadata = map( "bookmarks", singletonList( bookmarkString ) );

        connection.send( util.chunk( 32, runMessage( metadata ) ) );

        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Transaction.InvalidBookmark, bookmarkString ) ) );
    }

    @Test
    public void shouldSendFailureMessageForRunWithInvalidTransactionTimeout() throws Exception
    {
        negotiateBoltV3();
        String txTimeout = "Tx timeout can't be a string for RUN";
        Map<String,Object> metadata = map( "tx_timeout", txTimeout );

        connection.send( util.chunk( 32, runMessage( metadata ) ) );

        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Request.Invalid, txTimeout ) ) );
    }

    @Test
    public void shouldSendFailureMessageForRunWithInvalidTransactionMetadata() throws Exception
    {
        negotiateBoltV3();
        String txMetadata = "Tx metadata can't be a string for RUN";
        Map<String,Object> metadata = map( "tx_metadata", txMetadata );

        connection.send( util.chunk( 32, runMessage( metadata ) ) );

        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Request.Invalid, txMetadata ) ) );
    }

    @Test
    public void shouldReturnUpdatedBookmarkAfterAutoCommitTransaction() throws Throwable
    {
        negotiateBoltV3();

        // bookmark is expected to advance once the auto-commit transaction is committed
        long lastClosedTransactionId = getLastClosedTransactionId();
        String expectedBookmark = new Bookmark( lastClosedTransactionId + 1 ).toString();

        connection.send( util.chunk(
                new RunMessage( "CREATE ()" ),
                PullAllMessage.INSTANCE ) );

        assertThat( connection, util.eventuallyReceives(
                msgSuccess(),
                msgSuccess( allOf( hasEntry( "bookmark", expectedBookmark ) ) ) ) );
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
