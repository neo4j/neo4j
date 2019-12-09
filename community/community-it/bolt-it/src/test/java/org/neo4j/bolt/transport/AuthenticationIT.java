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
package org.neo4j.bolt.transport;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.BoltServer;
import org.neo4j.bolt.messaging.ResponseMessage;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.v3.messaging.response.FailureMessage;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.fail;
import static org.neo4j.bolt.testing.MessageMatchers.msgFailure;
import static org.neo4j.bolt.testing.MessageMatchers.msgIgnored;
import static org.neo4j.bolt.testing.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.ResponseMatcherOptionality.OPTIONAL;
import static org.neo4j.bolt.testing.TransportTestUtil.ResponseMatcherOptionality.REQUIRED;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class AuthenticationIT extends AbstractBoltTransportsTest
{
    protected EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    protected final AssertableLogProvider logProvider = new AssertableLogProvider();
    protected Neo4jWithSocket server =
            new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule, getSettingsFunction() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule ).around( server );

    protected TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory()
    {
        return new TestDatabaseManagementServiceBuilder().setUserLogProvider( logProvider );
    }

    @Override
    protected Consumer<Map<Setting<?>,Object>> getSettingsFunction()
    {
        return settings -> {
            super.getSettingsFunction().accept( settings );
            settings.put( GraphDatabaseSettings.auth_enabled, true );
        };
    }

    private HostnamePort address;

    @Before
    public void setup()
    {
        address = server.lookupDefaultConnector();
    }

    @Test
    public void shouldRespondWithCredentialsExpiredOnFirstUse() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgSuccess(
                allOf( hasEntry( Matchers.is( "credentials_expired" ), Matchers.equalTo( true ) ), hasKey( "server" ), hasKey( "connection_id" ) ) ) ) );

        verifyConnectionOpen();
    }

    private void verifyConnectionOpen() throws IOException
    {
        connection.send( util.defaultReset() );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldFailIfWrongCredentials() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "wrong", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        assertThat( connection, eventuallyDisconnects() );
        assertEventually( () -> "Matching log call not found in\n" + logProvider.serialize(), this::authFailureLoggedToUserLog, is( true ), 30, SECONDS );
    }

    private boolean authFailureLoggedToUserLog()
    {
        String boltPackageName = BoltServer.class.getPackage().getName();
        return logProvider.containsMatchingLogCall( inLog( containsString( boltPackageName ) )
                .warn( containsString( "The client is unauthorized due to authentication failure." ) ) );
    }

    @Test
    public void shouldFailIfWrongCredentialsFollowingSuccessfulLogin() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        // change password
        connection.send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "secret" ),
                SYSTEM_DATABASE_NAME ) );

        // Then
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        // When login again with the new password
        reconnect();
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth(
                                map( "principal", "neo4j", "credentials", "secret", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        // When login again with the wrong password
        reconnect();
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth(
                                map( "principal", "neo4j", "credentials", "wrong", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailIfMalformedAuthTokenWrongType() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", singletonList( "neo4j" ), "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, the value associated with the key `principal` " +
                "must be a String but was: ArrayList" ) ) );

        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailIfMalformedAuthTokenMissingKey() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "this-should-have-been-credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, missing key `credentials`" ) ) );

        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailIfMalformedAuthTokenMissingScheme() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, missing key `scheme`" ) ) );

        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailIfMalformedAuthTokenUnknownScheme() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "unknown" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, scheme 'unknown' is not supported." ) ) );

        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailDifferentlyIfTooManyFailedAuthAttempts() throws Exception
    {
        // Given
        final long timeout = System.currentTimeMillis() + 60_000;
        FailureMessage failureMessage = null;

        // When
        while ( failureMessage == null )
        {
            if ( System.currentTimeMillis() > timeout )
            {
                fail( "Timed out waiting for the authentication failure to occur." );
            }

            ExecutorService executor = Executors.newFixedThreadPool( 10 );

            // Fire up some parallel connections that all send wrong authentication tokens
            List<CompletableFuture<FailureMessage>> futures = new ArrayList<>();
            for ( int i = 0; i < 10; i++ )
            {
                futures.add( CompletableFuture.supplyAsync( this::collectAuthFailureOnFailedAuth, executor ) );
            }

            try
            {
                // Wait for all tasks to complete
                CompletableFuture.allOf( futures.toArray( new CompletableFuture[0] ) ).get( 30, SECONDS );

                // We want at least one of the futures to fail with our expected code
                for ( int i = 0; i < futures.size(); i++ )
                {
                    FailureMessage recordedMessage = futures.get( i ).get();

                    if ( recordedMessage != null )
                    {
                        failureMessage = recordedMessage;

                        break;
                    }
                }
            }
            catch ( TimeoutException ex )
            {
                // if jobs did not complete, let's try again
                // do nothing
            }
            finally
            {
                executor.shutdown();
            }
        }

        assertThat( failureMessage.status(), equalTo( Status.Security.AuthenticationRateLimit ) );
        assertThat( failureMessage.message(),
                containsString( "The client has provided incorrect authentication details too many times in a row." ) );
    }

    @Test
    public void shouldBeAbleToChangePasswordUsingSystemCommand() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgSuccess(
                allOf( hasEntry( Matchers.is( "credentials_expired" ), Matchers.equalTo( true ) ), hasKey( "server" ), hasKey( "connection_id" ) ) ) ) );

        // When
        connection.send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "secret" ),
                SYSTEM_DATABASE_NAME ) );

        // Then
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        // If I reconnect I cannot use the old password
        reconnect();
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        // But the new password works fine
        reconnect();
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "secret", "scheme", "basic" ) ) );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    @Test
    public void shouldFailWhenReusingTheSamePassword() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgSuccess(
                allOf( hasEntry( Matchers.is( "credentials_expired" ), Matchers.equalTo( true ) ), hasKey( "server" ), hasKey( "connection_id" ) ) ) ) );

        // When
        connection.send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "neo4j" ),
                SYSTEM_DATABASE_NAME ) );

        // Then
        assertThat( connection, util.eventuallyReceives( msgFailure( Status.General.InvalidArguments,
                "Old password and new password cannot be the same." ) ) );

        // However you should also be able to recover
        connection.send( util.defaultReset() )
                .send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "abc" ),
                        SYSTEM_DATABASE_NAME ) );
        assertThat( connection, util.eventuallyReceives( msgIgnored(), msgSuccess(), msgSuccess(), msgSuccess() ) );
    }

    @Test
    public void shouldFailWhenSubmittingEmptyPassword() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgSuccess(
                allOf( hasEntry( Matchers.is( "credentials_expired" ), Matchers.equalTo( true ) ), hasKey( "server" ), hasKey( "connection_id" ) ) ) ) );

        // When
        connection.send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "" ),
                SYSTEM_DATABASE_NAME ) );

        // Then
        assertThat( connection, util.eventuallyReceives( msgFailure( Status.General.InvalidArguments,
                "A password cannot be empty." ) ) );

        // However you should also be able to recover
        connection.send( util.defaultReset() )
                .send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "abc" ),
                        SYSTEM_DATABASE_NAME ) );
        assertThat( connection, util.eventuallyReceives( msgIgnored(), msgSuccess(), msgSuccess(), msgSuccess() ) );
    }

    @Test
    public void shouldNotBeAbleToReadWhenPasswordChangeRequired() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection, util.eventuallyReceives( msgSuccess(
                allOf( hasEntry( Matchers.is( "credentials_expired" ), Matchers.equalTo( true ) ), hasKey( "server" ), hasKey( "connection_id" ) ) ) ) );

        // When
        connection.send( util.defaultRunAutoCommitTx( "MATCH (n) RETURN n" ) );

        // Then
        Matcher<ResponseMessage> expectedFailureMessage = msgFailure( Status.Security.CredentialsExpired,
                "The credentials you provided were valid, but must be changed before you can use this instance." );
        assertThat( connection,
                // Compiled runtime triggers the AuthorizationViolation exception on the PULL_N message, which means the RUN message will
                // give a Success response. This should not matter much since RUN + PULL_N are always sent together.
                util.eventuallyReceivesWithOptionalPrecedingMessages(
                        Pair.of( msgSuccess(), OPTIONAL ),
                        Pair.of( expectedFailureMessage, REQUIRED )
                )
        );

        assertThat( connection, eventuallyDisconnects() );
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
            assertThat( t, instanceOf( FailureMessage.class ) );
            FailureMessage msg = (FailureMessage) t;
            if ( !msg.status().equals( Status.Security.Unauthorized ) ||
                 !msg.message().contains( "The client is unauthorized due to authentication failure." ) )
            {
                specialMessage = msg;
            }
            return true;
        }
    }

    private MapValue singletonMap( String key, Object value )
    {
        return VirtualValues.map( new String[]{key}, new AnyValue[]{ValueUtils.of( value )}  );
    }

    private FailureMessage collectAuthFailureOnFailedAuth()
    {
        FailureMsgMatcher failureRecorder = new FailureMsgMatcher();

        TransportConnection connection = null;
        try
        {
            connection = newConnection();

            connection.connect( address ).send( util.defaultAcceptedVersions() )
                    .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "WHAT_WAS_THE_PASSWORD_AGAIN", "scheme", "basic" ) ) );

            assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
            assertThat( connection, util.eventuallyReceives( failureRecorder ) );
            assertThat( connection, eventuallyDisconnects() );
        }
        catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }
        finally
        {
            if ( connection != null )
            {
                try
                {
                    connection.disconnect();
                }
                catch ( IOException ex )
                {
                    throw new RuntimeException( ex );
                }
            }
        }

        return failureRecorder.specialMessage;
    }
}
