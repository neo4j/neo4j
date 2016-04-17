/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.Messages.init;
import static org.neo4j.bolt.v1.messaging.message.Messages.pullAll;
import static org.neo4j.bolt.v1.messaging.message.Messages.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.hasNotification;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyRecieves;
import static org.neo4j.helpers.collection.MapUtil.map;

@RunWith(Parameterized.class)
public class TransportSessionIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket(settings ->
            settings.put( GraphDatabaseSettings.auth_enabled, "false"  ));

    @Parameterized.Parameter(0)
    public Factory<Connection> cf;

    @Parameterized.Parameter(1)
    public HostnamePort address;

    private Connection client;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
                new Object[]{
                        (Factory<Connection>) SocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<Connection>) WebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<Connection>) SecureSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<Connection>) SecureWebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                } );
    }

    @Test
    public void shouldNegotiateProtocolVersion() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
    }

    @Test
    public void shouldReturnNilOnNoApplicableVersion() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1337, 0, 0, 0 ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 0} ) );
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
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves(
                msgSuccess(),
                msgSuccess( map( "fields", asList( "a", "a_squared" ) ) ),
                msgRecord( eqRecord( equalTo( 1L ), equalTo( 1L ) ) ),
                msgRecord( eqRecord( equalTo( 2L ), equalTo( 4L ) ) ),
                msgRecord( eqRecord( equalTo( 3L ), equalTo( 9L ) ) ),
                msgSuccess() ) );
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
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves(
                msgSuccess(),
                msgSuccess( map("fields", singletonList( "n" )))));

        //
        //Record(0x71) {
        //    fields: [ Node(0x4E) {
        //                 id: 00
        //                 labels: [] (90)
        //                  props: {} (A)]
        //}
        assertThat( client,
                eventuallyRecieves(bytes(0x00, 0x08, 0xB1, 0x71,  0x91,
                        0xB3, 0x4E,  0x00, 0x90, 0xA0, 0x00, 0x00) ));
        assertThat(client, eventuallyRecieves( msgSuccess()));
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
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves(
                msgSuccess(),
                msgSuccess( map( "fields", singletonList( "r" ) ) ) ) );

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
                eventuallyRecieves( bytes( 0x00, 0x0B, 0xB1, 0x71, 0x91,
                        0xB5, 0x52, 0x00, 0x00, 0x01, 0x81, 0x54, 0xA0, 0x00,0x00  ) ) );
        assertThat( client, eventuallyRecieves( msgSuccess() ) );
    }

    private byte[] bytes( int...ints )
    {
        byte[] bytes = new byte[ints.length];
        for ( int i = 0; i < ints.length; i++ )
        {
            bytes[i] = (byte) ints[i];
        }
        return bytes;
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
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves(
                msgSuccess(),
                msgSuccess(),
                msgSuccess() ) );

        // When
        client.send(
                TransportTestUtil.chunk(
                        run( "RETURN 1" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves(
                msgSuccess(),
                msgRecord( eqRecord( equalTo( 1l ) ) ),
                msgSuccess( map( "type", "r" ) ) ) );
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
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves(
                msgSuccess(),
                msgSuccess(),
                hasNotification(
                        new TestNotification( "Neo.ClientNotification.Statement.UnknownLabelWarning",
                                "The provided label is not in the database.",
                                "One of the labels in your query is not available in the database, " +
                                "make sure you didn't misspell it or that the label is available when " +
                                "you run this statement in your application (the missing label name is is: " +
                                "THIS_IS_NOT_A_LABEL)",
                                SeverityLevel.WARNING, new InputPosition( 9, 1, 10 ) ) ) ) );

    }

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
    }

    @After
    public void teardown() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }
}
