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

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.messaging.message.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.message.FailureMessage;
import org.neo4j.bolt.v1.messaging.message.InitMessage;
import org.neo4j.bolt.v1.messaging.message.PullAllMessage;
import org.neo4j.bolt.v1.messaging.message.ResetMessage;
import org.neo4j.bolt.v1.messaging.message.ResponseMessage;
import org.neo4j.bolt.v1.messaging.message.RunMessage;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.ValueUtils;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.Version;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgIgnored;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.helpers.collection.MapUtil.map;

@RunWith( Parameterized.class )
public class AuthenticationIT
{
    protected EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    protected Neo4jWithSocket server =
            new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule, getSettingsFunction() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule ).around( server );

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestGraphDatabaseFactory();
    }

    protected Consumer<Map<String,String>> getSettingsFunction()
    {
        return settings -> settings.put( GraphDatabaseSettings.auth_enabled.name(), "true" );
    }

    @Parameterized.Parameter
    public Factory<TransportConnection> cf;

    private HostnamePort address;
    private TransportConnection client;
    private final String version = "Neo4j/" + Version.getNeo4jVersion();

    @Parameterized.Parameters
    public static Collection<Factory<TransportConnection>> transports()
    {
        return asList( SocketConnection::new, WebSocketConnection::new, SecureSocketConnection::new,
                SecureWebSocketConnection::new );
    }

    @Before
    public void setup() throws IOException
    {
        this.client = cf.newInstance();
        this.address = server.lookupDefaultConnector();
    }

    @After
    public void teardown() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    @Test
    public void shouldRespondWithCredentialsExpiredOnFirstUse() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess( map( "credentials_expired", true, "server", version ) ) ) );

        verifyConnectionOpen();
    }

    private void verifyConnectionOpen() throws IOException
    {
        client.send( TransportTestUtil.chunk( ResetMessage.reset() ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldFailIfWrongCredentials() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "wrong", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        assertThat( client, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailIfWrongCredentialsFollowingSuccessfulLogin() throws Throwable
    {
        // When change password
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1", map( "principal", "neo4j",
                                "credentials", "neo4j", "new_credentials", "secret", "scheme", "basic" ) ) ) );
        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );

        // When login again with the new password
        reconnect();
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "secret", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );

        // When login again with the wrong password
        reconnect();
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "wrong", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        assertThat( client, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailIfMalformedAuthTokenWrongType() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", singletonList( "neo4j" ), "credentials", "neo4j", "scheme",
                                        "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, the value associated with the key `principal` " +
                "must be a String but was: ArrayList" ) ) );

        assertThat( client, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailIfMalformedAuthTokenMissingKey() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "this-should-have-been-credentials", "neo4j", "scheme",
                                        "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, missing key `credentials`" ) ) );

        assertThat( client, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailIfMalformedAuthTokenMissingScheme() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, missing key `scheme`" ) ) );

        assertThat( client, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailIfMalformedAuthTokenUnknownScheme() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j",
                                        "scheme", "unknown" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, scheme 'unknown' is not supported." ) ) );

        assertThat( client, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailDifferentlyIfTooManyFailedAuthAttempts() throws Exception
    {
        // Given
        final long timeout = System.currentTimeMillis() + 30_000;
        final FailureMsgMatcher failureMatcher = new FailureMsgMatcher();

        // When
        while ( System.currentTimeMillis() < timeout && !failureMatcher.gotSpecialMessage() )
        {
            // Done in a loop because we're racing with the clock to get enough failed requests in 5 seconds
            client.connect( address )
                    .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                    .send( TransportTestUtil.chunk(
                            InitMessage.init( "TestClient/1.1",
                                    map( "principal", "neo4j", "credentials", "WHAT_WAS_THE_PASSWORD_AGAIN",
                                            "scheme", "basic" ) ) ) );

            assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
            assertThat( client, eventuallyReceives( failureMatcher ) );

            assertThat( client, eventuallyDisconnects() );
            reconnect();
        }

        // Then
        assertThat( failureMatcher.specialMessage.status(), equalTo( Status.Security.AuthenticationRateLimit ) );
        assertThat( failureMatcher.specialMessage.message(),
                containsString( "The client has provided incorrect authentication details too many times in a row." ) );
    }

    @Test
    public void shouldBeAbleToUpdateCredentials() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1", map( "principal", "neo4j",
                                "credentials", "neo4j", "new_credentials", "secret", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );

        // If I reconnect I cannot use the old password
        reconnect();
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        // But the new password works fine
        reconnect();
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "secret", "scheme", "basic" ) ) ) );
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldBeAuthenticatedAfterUpdatingCredentials() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1", map( "principal", "neo4j",
                                "credentials", "neo4j", "new_credentials", "secret", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );

        // When
        client.send( TransportTestUtil.chunk(
                RunMessage.run( "MATCH (n) RETURN n" ),
                PullAllMessage.pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    @Test
    public void shouldBeAbleToChangePasswordUsingBuiltInProcedure() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess( map( "credentials_expired", true, "server", version ) ) ) );

        // When
        client.send( TransportTestUtil.chunk(
                RunMessage.run( "CALL dbms.security.changePassword", singletonMap( "password", "secret" ) ),
                PullAllMessage.pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess() ) );

        // If I reconnect I cannot use the old password
        reconnect();
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        // But the new password works fine
        reconnect();
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "secret", "scheme", "basic" ) ) ) );
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldBeAuthenticatedAfterChangePasswordUsingBuiltInProcedure() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess( map( "credentials_expired", true, "server", version ) ) ) );

        // When
        client.send( TransportTestUtil.chunk(
                RunMessage.run( "CALL dbms.security.changePassword", singletonMap( "password", "secret" ) ),
                PullAllMessage.pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );

        // When
        client.send( TransportTestUtil.chunk(
                RunMessage.run( "MATCH (n) RETURN n" ),
                PullAllMessage.pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    @Test
    public void shouldFailWhenReusingTheSamePassword() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess( map( "credentials_expired", true, "server", version ) ) ) );

        // When
        client.send( TransportTestUtil.chunk(
                RunMessage.run( "CALL dbms.security.changePassword", singletonMap( "password", "neo4j" ) ),
                PullAllMessage.pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgFailure( Status.General.InvalidArguments,
                "Old password and new password cannot be the same." ) ) );

        // However you should also be able to recover
        client.send( TransportTestUtil.chunk(
                AckFailureMessage.ackFailure(),
                RunMessage.run( "CALL dbms.security.changePassword", singletonMap( "password", "abc" ) ),
                PullAllMessage.pullAll() ) );
        assertThat( client, eventuallyReceives( msgIgnored(), msgSuccess(), msgSuccess(), msgSuccess() ) );
    }

    @Test
    public void shouldFailWhenSubmittingEmptyPassword() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess( map( "credentials_expired", true, "server", version ) ) ) );

        // When
        client.send( TransportTestUtil.chunk(
                RunMessage.run( "CALL dbms.security.changePassword", singletonMap( "password", "" ) ),
                PullAllMessage.pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgFailure( Status.General.InvalidArguments,
                "A password cannot be empty." ) ) );

        // However you should also be able to recover
        client.send( TransportTestUtil.chunk(
                AckFailureMessage.ackFailure(),
                RunMessage.run( "CALL dbms.security.changePassword", singletonMap( "password", "abc" ) ),
                PullAllMessage.pullAll() ) );
        assertThat( client, eventuallyReceives( msgIgnored(), msgSuccess(), msgSuccess(), msgSuccess() ) );
    }

    @Test
    public void shouldNotBeAbleToReadWhenPasswordChangeRequired() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        InitMessage.init( "TestClient/1.1",
                                map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) ) );

        // Then
        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess( map( "credentials_expired", true, "server", version ) ) ) );

        // When
        client.send( TransportTestUtil.chunk(
                RunMessage.run( "MATCH (n) RETURN n" ),
                PullAllMessage.pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives( msgFailure( Status.Security.CredentialsExpired,
                "The credentials you provided were valid, but must be changed before you can use this instance." ) ) );

        assertThat( client, eventuallyDisconnects() );
    }

    private void reconnect() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
        this.client = cf.newInstance();
    }

    class FailureMsgMatcher extends TypeSafeMatcher<ResponseMessage>
    {
        FailureMessage specialMessage;

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "FAILURE" );
        }

        @Override
        protected boolean matchesSafely( ResponseMessage t )
        {
            Assert.assertThat( t, instanceOf( FailureMessage.class ) );
            FailureMessage msg = (FailureMessage) t;
            if ( !msg.status().equals( Status.Security.Unauthorized ) ||
                 !msg.message().contains( "The client is unauthorized due to authentication failure." ) )
            {
                specialMessage = msg;
            }
            return true;
        }

        public boolean gotSpecialMessage()
        {
            return specialMessage != null;
        }
    }

    private MapValue singletonMap( String key, Object value )
    {
        return VirtualValues.map( Collections.singletonMap( key, ValueUtils.of( value ) ) );
    }
}
