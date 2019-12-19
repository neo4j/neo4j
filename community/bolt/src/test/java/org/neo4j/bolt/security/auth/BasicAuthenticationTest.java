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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SecurityGraphInitializer;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;
import org.neo4j.time.Clocks;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

class BasicAuthenticationTest
{

    private Authentication authentication;

    @Test
    void shouldNotDoAnythingOnSuccess() throws Exception
    {
        // When
        AuthenticationResult result = authentication.authenticate( map( "scheme", "basic", "principal", "mike", "credentials", password( "secret2" ) ) );

        // Then
        assertThat( result.getLoginContext().subject().username(), equalTo( "mike" ) );
    }

    @Test
    void shouldThrowAndLogOnFailure()
    {
        var e = assertThrows( AuthenticationException.class,
                () -> authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", password( "banana" ) ) ) );
        assertEquals( Status.Security.Unauthorized, e.status() );
        assertEquals( "The client is unauthorized due to authentication failure.", e.getMessage() );
    }

    @Test
    void shouldIndicateThatCredentialsExpired() throws Exception
    {
        // When
        AuthenticationResult result = authentication.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", password( "secret" ) ) );

        // Then
        assertTrue( result.credentialsExpired() );
    }

    @Test
    void shouldFailWhenTooManyAttempts() throws Exception
    {
        // Given
        int maxFailedAttempts = ThreadLocalRandom.current().nextInt( 1, 10 );
        Authentication auth = createAuthentication( maxFailedAttempts );

        for ( int i = 0; i < maxFailedAttempts; ++i )
        {
            try
            {
                auth.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", password( "gelato" ) ) );
            }
            catch ( AuthenticationException e )
            {
                assertThat( e.status(), equalTo( Status.Security.Unauthorized ) );
            }
        }

        var e = assertThrows( AuthenticationException.class,
                () -> auth.authenticate( map( "scheme", "basic", "principal", "bob", "credentials", password( "gelato" ) ) ) );
        assertEquals( Status.Security.AuthenticationRateLimit, e.status() );
        assertEquals( "The client has provided incorrect authentication details too many times in a row.", e.getMessage() );
    }

    @Test
    void shouldClearCredentialsAfterUse() throws Exception
    {
        // When
        byte[] password = password( "secret2" );

        authentication.authenticate( map( "scheme", "basic", "principal", "mike", "credentials", password ) );

        // Then
        assertThat( password, isCleared() );
    }

    @Test
    void shouldThrowWithNoScheme()
    {
        var e = assertThrows( AuthenticationException.class,
                () -> authentication.authenticate( map( "principal", "bob", "credentials", password( "secret" ) ) ) );
        assertEquals( Status.Security.Unauthorized, e.status() );
    }

    @Test
    void shouldFailOnInvalidAuthToken()
    {
        var e = assertThrows( AuthenticationException.class, () -> authentication.authenticate( map( "this", "does", "not", "matter", "for", "test" ) ) );
        assertEquals( Status.Security.Unauthorized, e.status() );
    }

    @Test
    void shouldFailOnMalformedToken()
    {
        var e = assertThrows( AuthenticationException.class, () -> authentication
                .authenticate( map( "scheme", "basic", "principal", singletonList( "bob" ), "credentials", password( "secret" ) ) ) );
        assertEquals( Status.Security.Unauthorized, e.status() );
        assertEquals( "Unsupported authentication token, the value associated with the key `principal` " +
                "must be a String but was: SingletonList", e.getMessage() );
    }

    @BeforeEach
    void setup() throws Throwable
    {
        authentication = createAuthentication( 3 );
    }

    private static Authentication createAuthentication( int maxFailedAttempts ) throws Exception
    {
        Config config = Config.defaults( GraphDatabaseSettings.auth_max_failed_attempts, maxFailedAttempts );
        SystemGraphRealmHelper realmHelper = spy( new SystemGraphRealmHelper( null, new SecureHasher() ) );
        BasicSystemGraphRealm realm = new BasicSystemGraphRealm( SecurityGraphInitializer.NO_OP, realmHelper,
                new RateLimitedAuthenticationStrategy( Clocks.systemClock(), config ) );
        Authentication authentication = new BasicAuthentication( realm );
        doReturn( new User.Builder( "bob", credentialFor( "secret" ) ).withRequiredPasswordChange( true ).build() ).when( realmHelper ).getUser( "bob" );
        doReturn( new User.Builder( "mike", credentialFor( "secret2" ) ).build() ).when( realmHelper ).getUser( "mike" );

        return authentication;
    }

    static class HasStatus extends TypeSafeMatcher<Status.HasStatus>
    {
        private final Status status;

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

    private static CredentialsClearedMatcher isCleared()
    {
        return new CredentialsClearedMatcher();
    }

    static class CredentialsClearedMatcher extends BaseMatcher<byte[]>
    {
        @Override
        public boolean matches( Object o )
        {
            if ( o instanceof byte[] )
            {
                byte[] bytes = (byte[]) o;
                for ( byte aByte : bytes )
                {
                    if ( aByte != (byte) 0 )
                    {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "Byte array should contain only zeroes" );
        }
    }
}
