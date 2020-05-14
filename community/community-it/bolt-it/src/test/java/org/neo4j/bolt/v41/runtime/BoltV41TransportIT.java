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
package org.neo4j.bolt.v41.runtime;

import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SecureWebSocketConnection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.WebSocketConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.v3.messaging.request.CommitMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.RollbackMessage;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.bolt.v4.messaging.PullMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId;
import org.neo4j.bolt.v41.BoltProtocolV41;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.values.AnyValue;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.neo4j.bolt.testing.MessageConditions.msgRecord;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.StreamConditions.eqRecord;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newMessageEncoder;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.storable.Values.longValue;

@RunWith( Parameterized.class )
public class BoltV41TransportIT
{
    private static final String USER_AGENT = "TestClient/4.1";

    @Rule
    public final Neo4jWithSocket server = new Neo4jWithSocket( getClass(), withOptionalBoltEncryption() );

    private HostnamePort address;
    private TransportConnection connection;
    private TransportTestUtil util;

    @Parameterized.Parameter
    public Class<? extends TransportConnection> connectionClass;

    @Parameterized.Parameters( name = "{0}" )
    public static List<Class<? extends TransportConnection>> transports()
    {
        return asList( SocketConnection.class, WebSocketConnection.class, SecureSocketConnection.class, SecureWebSocketConnection.class );
    }

    @Before
    public void setUp() throws Exception
    {
        address = server.lookupDefaultConnector();
        connection = connectionClass.newInstance();
        util = new TransportTestUtil( newMessageEncoder() );
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
    public void shouldReturnUpdatedBookmarkAfterAutoCommitTransaction() throws Throwable
    {
        assumeFalse( FabricDatabaseManager.fabricByDefault() );

        negotiateBoltV41();

        // bookmark is expected to advance once the auto-commit transaction is committed
        var lastClosedTransactionId = getLastClosedTransactionId();
        var expectedBookmark = new BookmarkWithDatabaseId( lastClosedTransactionId + 1, getDatabaseId() ).toString();

        connection.send( util.chunk( new RunMessage( "CREATE ()" ), new PullMessage( asMapValue( map( "n", -1L ) ) ) ) );

        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess(),
                msgSuccess( responseMessage -> assertThat( responseMessage )
                        .containsEntry( "bookmark", expectedBookmark ) ) ) );
    }

    @Test
    public void shouldReturnUpdatedBookmarkAfterExplicitTransaction() throws Throwable
    {
        assumeFalse( FabricDatabaseManager.fabricByDefault() );

        negotiateBoltV41();

        // bookmark is expected to advance once the auto-commit transaction is committed
        var lastClosedTransactionId = getLastClosedTransactionId();
        var expectedBookmark = new BookmarkWithDatabaseId( lastClosedTransactionId + 1, getDatabaseId() ).toString();

        connection.send( util.chunk( new BeginMessage() ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( new RunMessage( "CREATE ()" ), new PullMessage( asMapValue( map( "n", -1L ) ) ) ) );

        assertThat( connection )
                .satisfies( util.eventuallyReceives( msgSuccess(),
                                                     msgSuccess( message -> assertThat( message ).doesNotContainEntry( "bookmark", expectedBookmark ) ) ) );

        connection.send( util.chunk( CommitMessage.COMMIT_MESSAGE ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsEntry( "bookmark", expectedBookmark ) ) ) );
    }

    @Test
    public void shouldStreamWhenStatementIdNotProvided() throws Exception
    {
        negotiateBoltV41();

        // begin a transaction
        connection.send( util.chunk( new BeginMessage() ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        // execute a query
        connection.send( util.chunk( new RunMessage( "UNWIND range(30, 40) AS x RETURN x" ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message )
                        .containsEntry( "qid", 0L )
                        .containsKeys( "fields", "t_first" ) ) ) );

        // request 5 records but do not provide qid
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 5L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( longValueCondition( 30L ) ) ),
                msgRecord( eqRecord( longValueCondition( 31L ) ) ),
                msgRecord( eqRecord( longValueCondition( 32L ) ) ),
                msgRecord( eqRecord( longValueCondition( 33L ) ) ),
                msgRecord( eqRecord( longValueCondition( 34L ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 2 more records but do not provide qid
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 2L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( longValueCondition( 35L ) ) ),
                msgRecord( eqRecord( longValueCondition( 36L ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 3 more records and provide qid
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 3L, "qid", 0L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( longValueCondition( 37L ) ) ),
                msgRecord( eqRecord( longValueCondition( 38L ) ) ),
                msgRecord( eqRecord( longValueCondition( 39L ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 10 more records but do not provide qid, only 1 more record is available
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 10L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( longValueCondition( 40L ) ) ),
                msgSuccess( message -> assertThat( message ).containsKey("t_last")
                                                            .doesNotContainKey( "has_more" )) ) );

        // rollback the transaction
        connection.send( util.chunk( RollbackMessage.ROLLBACK_MESSAGE ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldSendAndReceiveStatementIds() throws Exception
    {
        negotiateBoltV41();

        // begin a transaction
        connection.send( util.chunk( new BeginMessage() ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        // execute query #0
        connection.send( util.chunk( new RunMessage( "UNWIND range(1, 10) AS x RETURN x" ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsEntry( "qid", 0L ).containsKeys( "fields", "t_first" ) ) ) );

        // request 3 records for query #0
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 3L, "qid", 0L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( longValueCondition( 1L ) ) ),
                msgRecord( eqRecord( longValueCondition( 2L ) ) ),
                msgRecord( eqRecord( longValueCondition( 3L ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // execute query #1
        connection.send( util.chunk( new RunMessage( "UNWIND range(11, 20) AS x RETURN x" ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsEntry( "qid", 1L ).containsKeys( "fields", "t_first" ) ) ) );

        // request 2 records for query #1
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 2L, "qid", 1L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( longValueCondition( 11L ) ) ),
                msgRecord( eqRecord( longValueCondition( 12L ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // execute query #2
        connection.send( util.chunk( new RunMessage( "UNWIND range(21, 30) AS x RETURN x" ) ) );
        assertThat( connection ).satisfies(  util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsEntry( "qid", 2L ).containsKeys( "fields", "t_first" ) ) ) );

        // request 4 records for query #2
        // no qid - should use the statement from the latest RUN
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 4L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( longValueCondition( 21L ) ) ),
                msgRecord( eqRecord( longValueCondition( 22L ) ) ),
                msgRecord( eqRecord( longValueCondition( 23L ) ) ),
                msgRecord( eqRecord( longValueCondition( 24L ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // execute query #3
        connection.send( util.chunk( new RunMessage( "UNWIND range(31, 40) AS x RETURN x" ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsEntry( "qid", 3L ).containsKeys( "fields", "t_first" ) ) ) );

        // request 1 record for query #3
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 1L, "qid", 3L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( longValueCondition( 31L ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 2 records for query #0
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 2L, "qid", 0L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( longValueCondition( 4L ) ) ),
                msgRecord( eqRecord( longValueCondition( 5L ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 9 records for query #3
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 9L, "qid", 3L ) ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgRecord( eqRecord( longValueCondition( 32L ) ) ),
                msgRecord( eqRecord( longValueCondition( 33L ) ) ),
                msgRecord( eqRecord( longValueCondition( 34L ) ) ),
                msgRecord( eqRecord( longValueCondition( 35L ) ) ),
                msgRecord( eqRecord( longValueCondition( 36L ) ) ),
                msgRecord( eqRecord( longValueCondition( 37L ) ) ),
                msgRecord( eqRecord( longValueCondition( 38L ) ) ),
                msgRecord( eqRecord( longValueCondition( 39L ) ) ),
                msgRecord( eqRecord( longValueCondition( 40L ) ) ),
                msgSuccess( message -> assertThat( message ).containsKey( "t_last" ).doesNotContainKey( "has_more" ) ) ) );

        // commit the transaction
        connection.send( util.chunk( CommitMessage.COMMIT_MESSAGE ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    private void negotiateBoltV41() throws Exception
    {
        connection.connect( address ).send( util.acceptedVersions( BoltProtocolV41.VERSION.toInt(), 0, 0, 0 ) );
        assertThat( connection ).satisfies( eventuallyReceives( new byte[]{0, 0, 1, 4} ) );

        connection.send( util.chunk( new HelloMessage( map( "user_agent", USER_AGENT ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    private long getLastClosedTransactionId()
    {
        var resolver = ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver();
        var txIdStore = resolver.resolveDependency( TransactionIdStore.class );
        return txIdStore.getLastClosedTransactionId();
    }

    private NamedDatabaseId getDatabaseId()
    {
        var resolver = ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver();
        var database = resolver.resolveDependency( Database.class );
        return database.getNamedDatabaseId();
    }

    private static Condition<AnyValue> longValueCondition( long expected )
    {
        return new Condition<>( value -> value.equals( longValue( expected ) ), "equals" );
    }
}
