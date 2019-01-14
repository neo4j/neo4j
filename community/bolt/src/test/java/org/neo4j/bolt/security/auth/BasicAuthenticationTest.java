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
package org.neo4j.bolt.security.auth;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.security.auth.BasicAuthManager;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.time.Clocks;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.MapUtil.map;

public class BasicAuthenticationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Authentication authentication;

    @Test
    public void shouldNotDoAnythingOnSuccess() throws Exception
    {
        // When
        AuthenticationResult result =
                authentication.authenticate( map( "scheme", "basic", "principal", "mike", "credentials", "secret2" ) );

        // Then
        assertThat( result.getLoginContext().subject().username(), equalTo( "mike" ) );
    }

    @Test
    public void shouldThrowAndLogOnFailure() throws Exception
    {
        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.Unauthorized ) );
        exception.expectMessage( "The client is unauthorized due to authentication failure." );

        // When
        authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "banana" ) );
    }

    @Test
    public void shouldIndicateThatCredentialsExpired() throws Exception
    {
        // When
        AuthenticationResult result =
                authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "secret" ) );

        // Then
        assertTrue( result.credentialsExpired() );
    }

    @Test
    public void shouldFailWhenTooManyAttempts() throws Exception
    {
        // Given
        int maxFailedAttempts = ThreadLocalRandom.current().nextInt( 1, 10 );
        Authentication auth = createAuthentication( maxFailedAttempts );

        for ( int i = 0; i < maxFailedAttempts; ++i )
        {
            try
            {
                auth.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "gelato" ) );
            }
            catch ( AuthenticationException e )
            {
                assertThat( e.status(), equalTo( Status.Security.Unauthorized ) );
            }
        }

        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.AuthenticationRateLimit ) );
        exception.expectMessage( "The client has provided incorrect authentication details too many times in a row." );

        //When
        auth.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "gelato" ) );
    }

    @Test
    public void shouldBeAbleToUpdateCredentials() throws Exception
    {
        // When
        authentication.authenticate(
                map( "scheme", "basic", "principal", "mike", "credentials", "secret2", "new_credentials", "secret" ) );

        // Then
        authentication.authenticate( map( "scheme", "basic", "principal", "mike", "credentials", "secret" ) );
    }

    @Test
    public void shouldBeAbleToUpdateExpiredCredentials() throws Exception
    {
        // When
        AuthenticationResult result = authentication.authenticate(
                map( "scheme", "basic", "principal", "bob", "credentials", "secret", "new_credentials", "secret2" ) );

        // Then
        assertThat(result.credentialsExpired(), equalTo( false ));
    }

    @Test
    public void shouldNotBeAbleToUpdateCredentialsIfOldCredentialsAreInvalid() throws Exception
    {
        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.Unauthorized ) );
        exception.expectMessage( "The client is unauthorized due to authentication failure." );

        // When
        authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", "gelato",
                "new_credentials", "secret2" ) );
    }

    @Test
    public void shouldThrowWithNoScheme() throws Exception
    {
        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.Unauthorized ) );

        // When
        authentication.authenticate( map( "principal", "bob", "credentials", "secret" ) );
    }

    @Test
    public void shouldFailOnInvalidAuthToken() throws Exception
    {
        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.Unauthorized ) );

        // When
        authentication.authenticate( map( "this", "does", "not", "matter", "for", "test" ) );
    }

    @Test
    public void shouldFailOnMalformedToken() throws Exception
    {
        // Expect
        exception.expect( AuthenticationException.class );
        exception.expect( hasStatus( Status.Security.Unauthorized ) );
        exception.expectMessage( "Unsupported authentication token, the value associated with the key `principal` " +
                "must be a String but was: SingletonList" );

        // When
        authentication
                .authenticate( map( "scheme", "basic", "principal", singletonList( "bob" ), "credentials", "secret" ) );
    }

    @Before
    public void setup() throws Throwable
    {
        authentication = createAuthentication( 3 );
    }

    private static Authentication createAuthentication( int maxFailedAttempts ) throws Exception
    {
        UserRepository users = new InMemoryUserRepository();
        PasswordPolicy policy = mock( PasswordPolicy.class );

        Config config = Config.defaults( GraphDatabaseSettings.auth_max_failed_attempts, String.valueOf( maxFailedAttempts ) );

        BasicAuthManager manager = new BasicAuthManager( users, policy, Clocks.systemClock(), users, config );
        Authentication authentication = new BasicAuthentication( manager, manager );
        manager.newUser( "bob", "secret", true );
        manager.newUser( "mike", "secret2", false );

        return authentication;
    }

    private HasStatus hasStatus( Status status )
    {
        return new HasStatus( status );
    }

    static class HasStatus extends TypeSafeMatcher<Status.HasStatus>
    {
        private Status status;

        HasStatus( Status status )
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
