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
package org.neo4j.bolt.security.auth;


import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.function.Supplier;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.auth.AuthenticationResult;
import org.neo4j.server.security.auth.BasicAuthManager;
import org.neo4j.server.security.auth.BasicAuthSubject;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;

public class BasicAuthenticationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Supplier<String> identifier = () -> "UNIQUE";

    @Test
    public void shouldNotDoAnythingOnSuccess() throws AuthenticationException
    {
        // Given
        BasicAuthManager manager = mock( BasicAuthManager.class );
        BasicAuthSubject authSubject = mock( BasicAuthSubject.class );
        BasicAuthentication authentication = new BasicAuthentication( manager, mock( LogProvider.class ), identifier  );
        when( manager.login( anyString(), anyString() ) ).thenReturn( authSubject );
        when( authSubject.getAuthenticationResult() ).thenReturn( AuthenticationResult.SUCCESS );

        //Expect nothing

        // When
        authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "secret" ) );
    }

    @Test
    public void shouldThrowAndLogOnFailure() throws AuthenticationException
    {
        // Given
        BasicAuthManager manager = mock( BasicAuthManager.class );
        BasicAuthSubject authSubject = mock( BasicAuthSubject.class );
        Log log = mock( Log.class );
        LogProvider logProvider = mock( LogProvider.class );
        when( logProvider.getLog( BasicAuthentication.class ) ).thenReturn( log );
        BasicAuthentication authentication = new BasicAuthentication( manager, logProvider, identifier );
        when( manager.login( anyString(), anyString() ) ).thenReturn( authSubject );
        when( authSubject.getAuthenticationResult() ).thenReturn( AuthenticationResult.FAILURE );

        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.Unauthorized ) );
        exception.expectMessage( "The client is unauthorized due to authentication failure." );

        // When
        authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "secret" ) );

        //Then
        verify( log ).warn( "Failed authentication attempt for 'bob')" );
    }

    @Test
    public void shouldIndicateThatCredentialsExpired() throws AuthenticationException
    {
        // Given
        BasicAuthManager manager = mock( BasicAuthManager.class );
        BasicAuthSubject authSubject = mock( BasicAuthSubject.class );
        BasicAuthentication authentication = new BasicAuthentication( manager, mock( LogProvider.class ), identifier );
        when( manager.login( anyString(), anyString() ) ).thenReturn( authSubject );
        when( authSubject.getAuthenticationResult() ).thenReturn( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );

        // Expect
        // TODO: For now the server just returns OK when a password change is required, but this should be changed to an appropriate message
        //exception.expect( AuthenticationException.class );
        //exception.expect( hasStatus( Status.Security.CredentialsExpired ) );
        //exception.expectMessage( "The credentials have expired and need to be updated." );

        // When
        authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "secret" ) );
    }

    @Test
    public void shouldFailWhenTooManyAttempts() throws AuthenticationException
    {
        // Given
        BasicAuthManager manager = mock( BasicAuthManager.class );
        BasicAuthSubject authSubject = mock( BasicAuthSubject.class );
        BasicAuthentication authentication = new BasicAuthentication( manager, mock( LogProvider.class ), identifier );
        when( manager.login( anyString(), anyString() ) ).thenReturn( authSubject );
        when( authSubject.getAuthenticationResult() ).thenReturn( AuthenticationResult.TOO_MANY_ATTEMPTS );

        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.AuthenticationRateLimit ) );
        exception.expectMessage( "The client has provided incorrect authentication details too many times in a row." );

        // When
        authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "secret" ) );
    }

    @Test
    public void shouldBeAbleToUpdateCredentials() throws AuthenticationException
    {
        // Given
        BasicAuthManager manager = mock( BasicAuthManager.class );
        BasicAuthSubject authSubject = mock( BasicAuthSubject.class );
        BasicAuthentication authentication = new BasicAuthentication( manager, mock( LogProvider.class ), identifier );
        when( manager.login( anyString(), anyString() ) ).thenReturn( authSubject );
        when( authSubject.getAuthenticationResult() ).thenReturn( AuthenticationResult.SUCCESS );

        //Expect nothing

        // When
        authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "secret",
                "new_credentials", "secret2" ) );
    }

    @Test
    public void shouldBeAbleToUpdateExpiredCredentials() throws AuthenticationException
    {
        // Given
        BasicAuthManager manager = mock( BasicAuthManager.class );
        BasicAuthSubject authSubject = mock( BasicAuthSubject.class );
        BasicAuthentication authentication = new BasicAuthentication( manager, mock( LogProvider.class ), identifier );
        when( manager.login( anyString(), anyString() ) ).thenReturn( authSubject );
        when( authSubject.getAuthenticationResult() ).thenReturn( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );

        //Expect nothing

        // When
        authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "secret",
                "new_credentials", "secret2" ) );
    }

    @Test
    public void shouldNotBeAbleToUpdateCredentialsIfOldCredentialsAreInvalid() throws AuthenticationException
    {
        // Given
        BasicAuthManager manager = mock( BasicAuthManager.class );
        BasicAuthSubject authSubject = mock( BasicAuthSubject.class );
        BasicAuthentication authentication = new BasicAuthentication( manager, mock( LogProvider.class ), identifier );
        when( manager.login( anyString(), anyString() ) ).thenReturn( authSubject );
        when( authSubject.getAuthenticationResult() ).thenReturn( AuthenticationResult.FAILURE );

        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.Unauthorized ) );
        exception.expectMessage( "The client is unauthorized due to authentication failure." );

        // When
        // When
        authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "secret",
                "new_credentials", "secret2" ) );
    }

    @Test
    public void shouldFailOnUnknownScheme() throws AuthenticationException
    {
        // Given
        BasicAuthManager manager = mock( BasicAuthManager.class );
        BasicAuthSubject authSubject = mock( BasicAuthSubject.class );
        BasicAuthentication authentication = new BasicAuthentication( manager, mock( LogProvider.class ), identifier );
        when( manager.login( anyString(), anyString() ) ).thenReturn( authSubject );
        when( authSubject.getAuthenticationResult() ).thenReturn( AuthenticationResult.SUCCESS );

        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.Unauthorized ) );
        exception.expectMessage( "Authentication token must contain: 'scheme : basic'" );

        // When
        authentication.authenticate( map( "scheme", "UNKNOWN", "principal", "bob", "credentials", "secret" ) );
    }

    @Test
    public void shouldFailOnMalformedToken() throws AuthenticationException
    {
        // Given
        BasicAuthManager manager = mock( BasicAuthManager.class );
        BasicAuthSubject authSubject = mock( BasicAuthSubject.class );
        BasicAuthentication authentication = new BasicAuthentication( manager, mock( LogProvider.class ), identifier );
        when( manager.login( anyString(), anyString() ) ).thenReturn( authSubject );
        when( authSubject.getAuthenticationResult() ).thenReturn( AuthenticationResult.SUCCESS );

        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.Unauthorized ) );
        exception.expectMessage(
                "The value associated with the key `principal` must be a String but was: SingletonList" );

        // When
        authentication
                .authenticate( map( "scheme", "basic", "principal", singletonList( "bob" ), "credentials", "secret" ) );
    }

    private HasStatus hasStatus( Status status )
    {
        return new HasStatus( status );
    }

    static class HasStatus extends TypeSafeMatcher<Status.HasStatus>
    {
        private Status status;

        public HasStatus( Status status )
        {
            this.status = status;
        }

        @Override
        protected boolean matchesSafely( Status.HasStatus item )
        {
            return item.status() == status;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "expects status " )
                    .appendValue( status );
        }

        @Override
        protected void describeMismatchSafely( Status.HasStatus item, Description mismatchDescription )
        {
            mismatchDescription.appendText( "was " )
                    .appendValue( item.status() );
        }
    }

}