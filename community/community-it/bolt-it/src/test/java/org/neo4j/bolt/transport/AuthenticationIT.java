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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
import org.neo4j.bolt.messaging.ResponseMessage;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.runtime.DefaultBoltConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.v3.messaging.response.FailureMessage;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgIgnored;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.ResponseMatcherOptionality.OPTIONAL;
import static org.neo4j.bolt.testing.TransportTestUtil.ResponseMatcherOptionality.REQUIRED;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class AuthenticationIT extends AbstractBoltTransportsTest
{
    protected final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Inject
    private Neo4jWithSocket server;

    protected TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory()
    {
        return new TestDatabaseManagementServiceBuilder().setUserLogProvider( logProvider );
    }

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setGraphDatabaseFactory( getTestGraphDatabaseFactory() );
        server.setConfigure( getSettingsFunction() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
    }

    @Override
    protected Consumer<Map<Setting<?>,Object>> getSettingsFunction()
    {
        return settings -> {
            super.getSettingsFunction().accept( settings );
            settings.put( GraphDatabaseSettings.auth_enabled, true );
        };
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldRespondWithCredentialsExpiredOnFirstUse(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message )
                            .containsEntry( "credentials_expired", true )
                            .containsKeys( "server", "connection_id" )
                ) ) );

        verifyConnectionOpen();
    }

    private void verifyConnectionOpen() throws IOException
    {
        connection.send( util.defaultReset() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailIfWrongCredentials(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "wrong", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        assertThat( connection ).satisfies( eventuallyDisconnects() );
        assertEventually( () -> "Matching log call not found in\n" + logProvider.serialize(), this::authFailureLoggedToUserLog, TRUE, 30, SECONDS );
    }

    private boolean authFailureLoggedToUserLog()
    {
        try
        {
            assertThat( logProvider ).forClass( DefaultBoltConnection.class ).forLevel( WARN )
                    .containsMessages( "The client is unauthorized due to authentication failure." );
            return true;
        }
        catch ( AssertionError e )
        {
            return false;
        }
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailIfWrongCredentialsFollowingSuccessfulLogin(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        // change password
        connection.send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "secret" ),
                SYSTEM_DATABASE_NAME ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        // When login again with the new password
        reconnect();
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth(
                                map( "principal", "neo4j", "credentials", "secret", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        // When login again with the wrong password
        reconnect();
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth(
                                map( "principal", "neo4j", "credentials", "wrong", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailIfMalformedAuthTokenWrongType(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", singletonList( "neo4j" ), "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, the value associated with the key `principal` " +
                "must be a String but was: ArrayList" ) ) );

        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailIfMalformedAuthTokenMissingKey(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "this-should-have-been-credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, missing key `credentials`" ) ) );

        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailIfMalformedAuthTokenMissingScheme(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, missing key `scheme`" ) ) );

        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailIfMalformedAuthTokenUnknownScheme(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "unknown" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "Unsupported authentication token, scheme 'unknown' is not supported." ) ) );

        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailDifferentlyIfTooManyFailedAuthAttempts(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

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

        assertThat( failureMessage.status() ).isEqualTo( Status.Security.AuthenticationRateLimit );
        assertThat( failureMessage.message() ).contains( "The client has provided incorrect authentication details too many times in a row." );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldBeAbleToChangePasswordUsingSystemCommand(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsEntry( "credentials_expired", true )
                .containsKeys( "server", "connection_id" ) ) ) );

        // When
        connection.send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "secret" ),
                SYSTEM_DATABASE_NAME ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        // If I reconnect I cannot use the old password
        reconnect();
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Security.Unauthorized,
                "The client is unauthorized due to authentication failure." ) ) );

        // But the new password works fine
        reconnect();
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "secret", "scheme", "basic" ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailWhenReusingTheSamePassword( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsEntry( "credentials_expired", true )
                                .containsKeys( "server", "connection_id" ) ) ) );

        // When
        connection.send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "neo4j" ),
                SYSTEM_DATABASE_NAME ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.General.InvalidArguments,
                "Old password and new password cannot be the same." ) ) );

        // However you should also be able to recover
        connection.send( util.defaultReset() )
                .send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "abc" ),
                        SYSTEM_DATABASE_NAME ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgIgnored(), msgSuccess(), msgSuccess(), msgSuccess() ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailWhenSubmittingEmptyPassword( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsEntry( "credentials_expired", true )
                                .containsKeys( "server", "connection_id" ) ) ) );

        // When
        connection.send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "" ),
                SYSTEM_DATABASE_NAME ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.General.InvalidArguments,
                "A password cannot be empty." ) ) );

        // However you should also be able to recover
        connection.send( util.defaultReset() )
                .send( util.defaultRunAutoCommitTx( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", singletonMap( "password", "abc" ),
                        SYSTEM_DATABASE_NAME ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgIgnored(), msgSuccess(), msgSuccess(), msgSuccess() ) );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldNotBeAbleToReadWhenPasswordChangeRequired(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( map( "principal", "neo4j", "credentials", "neo4j", "scheme", "basic" ) ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgSuccess( message -> assertThat( message ).containsEntry( "credentials_expired", true )
                                .containsKeys( "server", "connection_id" ) ) ) );

        // When
        connection.send( util.defaultRunAutoCommitTx( "MATCH (n) RETURN n" ) );

        // Then
        Consumer<ResponseMessage> expectedFailureMessage = msgFailure( Status.Security.CredentialsExpired,
                "The credentials you provided were valid, but must be changed before you can use this instance." );
        assertThat( connection ).satisfies(
                // Compiled runtime triggers the AuthorizationViolation exception on the PULL_N message, which means the RUN message will
                // give a Success response. This should not matter much since RUN + PULL_N are always sent together.
                util.eventuallyReceivesWithOptionalPrecedingMessages(
                        Pair.of( msgSuccess(), OPTIONAL ),
                        Pair.of( expectedFailureMessage, REQUIRED )
                )
        );

        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    static class FailureMsgMatcher implements Consumer<ResponseMessage>
    {
        FailureMessage specialMessage;

        @Override
        public void accept( ResponseMessage responseMessage )
        {
            assertThat( responseMessage ).isInstanceOf( FailureMessage.class );
            FailureMessage msg = (FailureMessage) responseMessage;
            if ( !msg.status().equals( Status.Security.Unauthorized ) ||
                    !msg.message().contains( "The client is unauthorized due to authentication failure." ) )
            {
                specialMessage = msg;
            }
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

            assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
            assertThat( connection ).satisfies( util.eventuallyReceives( failureRecorder ) );
            assertThat( connection ).satisfies( eventuallyDisconnects() );
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
