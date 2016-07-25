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
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.Messages.init;
import static org.neo4j.bolt.v1.messaging.message.Messages.pullAll;
import static org.neo4j.bolt.v1.messaging.message.Messages.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyRecieves;
import static org.neo4j.helpers.collection.MapUtil.map;

@RunWith( Parameterized.class )
public class AuthenticationIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getTestGraphDatabaseFactory(), getSettingsFunction() );

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestGraphDatabaseFactory();
    }

    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return settings -> settings.put( GraphDatabaseSettings.auth_enabled, "true" );
    }

    @Parameterized.Parameter( 0 )
    public Factory<Connection> cf;

    @Parameterized.Parameter( 1 )
    public HostnamePort address;

    protected Connection client;

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
    public void shouldRespondWithCredentialsExpiredOnFirstUse() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess( Collections.singletonMap( "credentials_expired", true )) ) );
    }

    @Test
    public void shouldFailIfWrongCredentials() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "wrong", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );
    }

    @Test
    public void shouldFailIfMalformedAuthTokenWrongType() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", singletonList( "neo4j" ), "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgFailure( Status.Security.Unauthorized,
                String.format( "The value associated with the key `principal` must be a String but was: ArrayList") ) ) );
    }

    @Test
    public void shouldFailIfMalformedAuthTokenMissingKey() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "this-should-have-been-credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgFailure( Status.Security.Unauthorized,
                String.format( "The value associated with the key `credentials` must be a String but was: null") ) ) );
    }

    @Test
    public void shouldBeAbleToUpdateCredentials() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", map( "principal", "neo4j",
                                "credentials", "neo4j", "new_credentials", "secret", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess() ) );

        // If I reconnect I cannot use the old password
        reconnect();
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        // But the new password works fine
        reconnect();
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "secret", "scheme", "basic" ) ) ) );
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess() ) );
    }

    @Test
    public void shouldBeAuthenticatedAfterUpdatingCredentials() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", map( "principal", "neo4j",
                                "credentials", "neo4j", "new_credentials", "secret", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess() ) );

        // When
        client.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( msgSuccess(), msgSuccess() ) );
    }

    @Test
    public void shouldBeAbleToChangePasswordUsingBuiltInProcedure() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess( Collections.singletonMap( "credentials_expired", true )) ) );

        // When
        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.changePassword", Collections.singletonMap( "password", "secret" ) ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( msgSuccess() ) );

        // If I reconnect I cannot use the old password
        reconnect();
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        // But the new password works fine
        reconnect();
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "secret", "scheme", "basic" ) ) ) );
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess() ) );
    }

    @Test
    public void shouldBeAuthenticatedAfterChangePasswordUsingBuiltInProcedure() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess( Collections.singletonMap( "credentials_expired", true )) ) );

        // When
        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.changePassword", Collections.singletonMap( "password", "secret" ) ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( msgSuccess(), msgSuccess() ) );

        // When
        client.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( msgSuccess(), msgSuccess() ) );
    }

    @Test
    public void shouldFailWhenReusingTheSamePassword() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess( Collections.singletonMap( "credentials_expired", true )) ) );

        // When
        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.changePassword", Collections.singletonMap( "password", "neo4j" ) ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( msgFailure(Status.Security.InvalidArguments,
                "Old password and new password cannot be the same.") ) );
    }

    @Test
    public void shouldFailWhenSubmittingEmptyPassword() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess( Collections.singletonMap( "credentials_expired", true )) ) );

        // When
        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.changePassword", Collections.singletonMap( "password", "" ) ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( msgFailure(Status.Security.InvalidArguments,
                "Password cannot be empty.") ) );
    }

    @Test
    public void shouldNotBeAbleToReadWhenPasswordChangeRequired() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess( Collections.singletonMap( "credentials_expired", true )) ) );

        // When
        client.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( msgFailure( Status.Security.CredentialsExpired,
                "The credentials you provided were valid, but must be changed before you can use this instance." ) ) );
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

    private void reconnect() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
        this.client = cf.newInstance();
    }
}
