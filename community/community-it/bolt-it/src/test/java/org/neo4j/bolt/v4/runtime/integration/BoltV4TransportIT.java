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
package org.neo4j.bolt.v4.runtime.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.bolt.v3.messaging.request.CommitMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.RollbackMessage;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.bolt.v4.messaging.PullMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newMessageEncoder;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newNeo4jPack;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.storable.Values.longValue;

@RunWith( Parameterized.class )
public class BoltV4TransportIT
{
    private static final String USER_AGENT = "TestClient/4.0";

    @Rule
    public final Neo4jWithSocket server = new Neo4jWithSocket( getClass(), settings -> settings.put( auth_enabled.name(), "false" ) );

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
    public void shouldStreamWhenStatementIdNotProvided() throws Exception
    {
        negotiateBoltV4();

        // begin a transaction
        connection.send( util.chunk( new BeginMessage( VirtualValues.EMPTY_MAP ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        // execute a query
        connection.send( util.chunk( new RunMessage( "UNWIND range(30, 40) AS x RETURN x" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( hasEntry( is( "qid" ), equalTo( 0L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

        // request 5 records but do not provide qid
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 5L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( longValue( 30L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 31L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 32L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 33L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 34L ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 2 more records but do not provide qid
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 2L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( longValue( 35L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 36L ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 3 more records and provide qid
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 3L, "qid", 0L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( longValue( 37L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 38L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 39L ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 10 more records but do not provide qid, only 1 more record is available
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 10L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( longValue( 40L ) ) ) ),
                msgSuccess( allOf( not( hasKey( "has_more" ) ), hasKey( "t_last" ) ) ) ) );

        // rollback the transaction
        connection.send( util.chunk( RollbackMessage.ROLLBACK_MESSAGE ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldSendAndReceiveStatementIds() throws Exception
    {
        negotiateBoltV4();

        // begin a transaction
        connection.send( util.chunk( new BeginMessage( VirtualValues.EMPTY_MAP ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        // execute query #0
        connection.send( util.chunk( new RunMessage( "UNWIND range(1, 10) AS x RETURN x" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( hasEntry( is( "qid" ), equalTo( 0L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

        // request 3 records for query #0
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 3L, "qid", 0L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( longValue( 1L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 2L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 3L ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // execute query #1
        connection.send( util.chunk( new RunMessage( "UNWIND range(11, 20) AS x RETURN x" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( hasEntry( is( "qid" ), equalTo( 1L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

        // request 2 records for query #1
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 2L, "qid", 1L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( longValue( 11L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 12L ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // execute query #2
        connection.send( util.chunk( new RunMessage( "UNWIND range(21, 30) AS x RETURN x" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( hasEntry( is( "qid" ), equalTo( 2L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

        // request 4 records for query #2
        // no qid - should use the statement from the latest RUN
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 4L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( longValue( 21L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 22L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 23L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 24L ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // execute query #3
        connection.send( util.chunk( new RunMessage( "UNWIND range(31, 40) AS x RETURN x" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgSuccess( allOf( hasEntry( is( "qid" ), equalTo( 3L ) ), hasKey( "fields" ), hasKey( "t_first" ) ) ) ) );

        // request 1 record for query #3
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 1L, "qid", 3L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( longValue( 31L ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 2 records for query #0
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 2L, "qid", 0L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( longValue( 4L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 5L ) ) ) ),
                msgSuccess( singletonMap( "has_more", true ) ) ) );

        // request 9 records for query #3
        connection.send( util.chunk( new PullMessage( asMapValue( map( "n", 9L, "qid", 3L ) ) ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgRecord( eqRecord( equalTo( longValue( 32L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 33L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 34L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 35L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 36L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 37L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 38L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 39L ) ) ) ),
                msgRecord( eqRecord( equalTo( longValue( 40L ) ) ) ),
                msgSuccess( allOf( not( hasKey( "has_more" ) ), hasKey( "t_last" ) ) ) ) );

        // commit the transaction
        connection.send( util.chunk( CommitMessage.COMMIT_MESSAGE ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    private void negotiateBoltV4() throws Exception
    {
        connection.connect( address ).send( util.acceptedVersions( 4, 0, 0, 0 ) );
        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 4} ) );

        connection.send( util.chunk( new HelloMessage( map( "user_agent", USER_AGENT ) ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }
}
