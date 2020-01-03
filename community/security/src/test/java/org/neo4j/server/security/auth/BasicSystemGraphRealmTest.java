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
package org.neo4j.server.security.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SecurityGraphInitializer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.TOO_MANY_ATTEMPTS;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;
import static org.neo4j.test.assertion.Assert.assertException;

public class BasicSystemGraphRealmTest
{
    private AuthenticationStrategy authStrategy;
    private BasicSystemGraphRealm realm;

    @BeforeEach
    void setUp() throws Exception
    {
        authStrategy = mock( AuthenticationStrategy.class );
        realm = spy( new BasicSystemGraphRealm( SecurityGraphInitializer.NO_OP, null, new SecureHasher(), authStrategy, true ) );
    }

    @Test
    void shouldFindAndAuthenticateUserSuccessfully() throws Throwable
    {
        // Given
        User user = new User.Builder( "jake", credentialFor( "abc123" ) ).build();
        doReturn( user ).when( realm ).getUser( "jake" );

        // When
        setMockAuthenticationStrategyResult( user, "abc123", SUCCESS );

        // Then
        assertLoginGivesResult( "jake", "abc123", SUCCESS );
    }

    @Test
    void shouldFindAndAuthenticateUserAndReturnAuthStrategyResult() throws Throwable
    {
        // Given
        User user = new User.Builder( "jake", credentialFor( "abc123" ) ).build();
        doReturn( user ).when( realm ).getUser( "jake" );

        // When
        setMockAuthenticationStrategyResult( user, "abc123", TOO_MANY_ATTEMPTS );

        // Then
        assertLoginGivesResult( "jake", "abc123", TOO_MANY_ATTEMPTS );
    }

    @Test
    void shouldFindAndAuthenticateUserAndReturnPasswordChangeIfRequired() throws Throwable
    {
        // Given
        User user = new User.Builder( "jake", credentialFor( "abc123" ) ).withRequiredPasswordChange( true ).build();
        doReturn( user ).when( realm ).getUser( "jake" );

        // When
        setMockAuthenticationStrategyResult( user, "abc123", SUCCESS );

        // Then
        assertLoginGivesResult( "jake", "abc123", PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldFailAuthenticationIfUserIsNotFound() throws Throwable
    {
        // Given
        doThrow( new InvalidArgumentsException( "User 'unknown' does not exist." ) ).when( realm ).getUser( "unknown" );

        // Then
        assertLoginGivesResult( "unknown", "abc123", FAILURE );
    }

    @Test
    void shouldClearPasswordOnLogin() throws Throwable
    {
        // Given
        when( authStrategy.authenticate( any(), any() ) ).thenReturn( AuthenticationResult.SUCCESS );

        User user = new User.Builder( "jake", credentialFor( "abc123" ) ).withRequiredPasswordChange( true ).build();
        doReturn( user ).when( realm ).getUser( "jake" );

        byte[] password = password( "abc123" );
        Map<String,Object> authToken = AuthToken.newBasicAuthToken( "jake", password );

        // When
        realm.login( authToken );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
        assertThat( authToken.get( AuthToken.CREDENTIALS ), equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
    }

    @Test
    void shouldClearPasswordOnInvalidAuthToken()
    {
        // Given
        byte[] password = password( "abc123" );
        Map<String,Object> authToken = AuthToken.newBasicAuthToken( "jake", password );
        authToken.put( AuthToken.SCHEME_KEY, null ); // Null is not a valid scheme

        // When
        try
        {
            realm.login( authToken );
            fail( "exception expected" );
        }
        catch ( InvalidAuthTokenException e )
        {
            // expected
        }
        assertThat( password, equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
        assertThat( authToken.get( AuthToken.CREDENTIALS ), equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
    }

    @Test
    void shouldFailWhenAuthTokenIsInvalid()
    {
        assertException(
                () -> realm.login( map( AuthToken.SCHEME_KEY, "supercool", AuthToken.PRINCIPAL, "neo4j" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, scheme 'supercool' is not supported." );

        assertException(
                () -> realm.login( map( AuthToken.SCHEME_KEY, "none" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, scheme 'none' is only allowed when auth is disabled" );

        assertException(
                () -> realm.login( map( "key", "value" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, missing key `scheme`" );

        assertException(
                () -> realm.login( map( AuthToken.SCHEME_KEY, "basic", AuthToken.PRINCIPAL, "neo4j" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, missing key `credentials`" );

        assertException(
                () -> realm.login( map( AuthToken.SCHEME_KEY, "basic", AuthToken.CREDENTIALS, "very-secret" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, missing key `principal`" );
    }

    private void assertLoginGivesResult( String username, String password, AuthenticationResult expectedResult ) throws InvalidAuthTokenException
    {
        LoginContext securityContext = realm.login( authToken( username, password ) );
        assertThat( securityContext.subject().getAuthenticationResult(), equalTo( expectedResult ) );
    }

    private void setMockAuthenticationStrategyResult( User user, String password, AuthenticationResult result )
    {
        when( authStrategy.authenticate( user, password( password ) ) ).thenReturn( result );
    }

    public static byte[] clearedPasswordWithSameLengthAs( String passwordString )
    {
        byte[] password = passwordString.getBytes( StandardCharsets.UTF_8 );
        Arrays.fill( password, (byte) 0 );
        return password;
    }
}
