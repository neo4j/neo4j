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
package org.neo4j.server.security.auth;

import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.systemgraph.BasicInMemorySystemGraphOperations;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.TOO_MANY_ATTEMPTS;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;
import static org.neo4j.test.assertion.Assert.assertException;

public class BasicSystemGraphRealmTest
{
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    protected Config config;
    protected UserRepository users;

    private AuthenticationStrategy authStrategy;
    private BasicSystemGraphRealm manager;

    @BeforeEach
    void setUp() throws Exception
    {
        SecureHasher secureHasher = new SecureHasher();
        BasicInMemorySystemGraphOperations operations = new BasicInMemorySystemGraphOperations(  );
        authStrategy = mock( AuthenticationStrategy.class );

        manager = new BasicSystemGraphRealm( operations, null, false, secureHasher, new BasicPasswordPolicy(), authStrategy, true );
        manager.init();
        manager.start();
    }

    @AfterEach
    void tearDown()
    {
        manager.stop();
        manager.shutdown();
    }

    @Test
    void shouldFindAndAuthenticateUserSuccessfully() throws Throwable
    {
        // Given
        manager.newUser( "jake", password( "abc123" ), false );

        // When
        setMockAuthenticationStrategyResult( "jake", "abc123", SUCCESS );

        // Then
        assertLoginGivesResult( "jake", "abc123", SUCCESS );
    }

    @Test
    void shouldFindAndAuthenticateUserAndReturnAuthStrategyResult() throws Throwable
    {
        // Given
        manager.newUser( "jake", password( "abc123" ), false );

        // When
        setMockAuthenticationStrategyResult( "jake", "abc123", TOO_MANY_ATTEMPTS );

        // Then
        assertLoginGivesResult( "jake", "abc123", TOO_MANY_ATTEMPTS );
    }

    @Test
    void shouldFindAndAuthenticateUserAndReturnPasswordChangeIfRequired() throws Throwable
    {
        // Given
        manager.newUser( "jake", password( "abc123" ), true );

        // When
        setMockAuthenticationStrategyResult( "jake", "abc123", SUCCESS );

        // Then
        assertLoginGivesResult( "jake", "abc123", PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldFailAuthenticationIfUserIsNotFound() throws Throwable
    {
        // Given
        manager.newUser( "jake", password( "abc123" ), true );

        // Then
        assertLoginGivesResult( "unknown", "abc123", FAILURE );
    }

    @Test
    void shouldCreateUser() throws Throwable
    {
        // When
        manager.newUser( "foo", password( "bar" ), true );

        // Then
        User user = manager.getUser( "foo" );
        assertNotNull( user );
        assertTrue( user.passwordChangeRequired() );
        assertTrue( user.credentials().matchesPassword( password( "bar" ) ) );
    }

    @Test
    void shouldDeleteUser() throws Throwable
    {
        // Given
        manager.newUser( "jake", password( "abc123" ), true );

        // When
        manager.deleteUser( "jake" );

        // Then
        assertNull( manager.silentlyGetUser( "jake" ) );
    }

    @Test
    void shouldFailToDeleteUnknownUser() throws Throwable
    {
        // Given
        manager.newUser( "jake", password( "abc123" ), true );

        try
        {
            // When
            manager.deleteUser( "nonExistentUser" );
            fail("User 'nonExistentUser' should no longer exist, expected exception.");
        }
        catch ( InvalidArgumentsException e )
        {
            assertThat( e.getMessage(), containsString( "User 'nonExistentUser' does not exist." ) );
        }
        catch ( Throwable t )
        {
            assertThat( t.getClass(), IsEqual.equalTo( InvalidArgumentsException.class ) );
        }

        // Then
        assertNotNull( manager.silentlyGetUser( "jake" ) );
    }

    @Test
    void shouldSetPassword() throws Throwable
    {
        // Given
        manager.newUser( "jake", password( "abc123" ), true );

        // When
        manager.setUserPassword( "jake", password( "hello, world!" ), false );

        // Then
        User user = manager.getUser( "jake" );
        assertTrue( user.credentials().matchesPassword( password( "hello, world!" ) ) );
    }

    @Test
    void shouldClearPasswordOnLogin() throws Throwable
    {
        // Given
        when( authStrategy.authenticate( any(), any() ) ).thenReturn( AuthenticationResult.SUCCESS );

        manager.newUser( "jake", password( "abc123" ), true );
        byte[] password = password( "abc123" );
        Map<String,Object> authToken = AuthToken.newBasicAuthToken( "jake", password );

        // When
        manager.login( authToken );

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
            manager.login( authToken );
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
    void shouldClearPasswordOnNewUser() throws Throwable
    {
        // Given
        byte[] password = password( "abc123" );

        // When
        manager.newUser( "jake", password, true );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
        User user = manager.getUser( "jake" );
        assertTrue( user.credentials().matchesPassword( password( "abc123" ) ) );
    }

    @Test
   void shouldClearPasswordOnNewUserAlreadyExists() throws Throwable
    {
        // Given
        manager.newUser( "jake", password( "abc123" ), true );
        byte[] password = password( "abc123" );

        // When
        try
        {
            manager.newUser( "jake", password, true );
            fail( "exception expected" );
        }
        catch ( InvalidArgumentsException e )
        {
            // expected
        }

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
    }

    @Test
    void shouldClearPasswordOnSetUserPassword() throws Throwable
    {
        // Given
        manager.newUser( "jake", password( "old" ), false );
        byte[] newPassword = password( "abc123" );

        // When
        manager.setUserPassword( "jake", newPassword, false );

        // Then
        assertThat( newPassword, equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
        User user = manager.getUser( "jake" );
        assertTrue( user.credentials().matchesPassword( password( "abc123" ) ) );
    }

    @Test
    void shouldClearPasswordOnSetUserPasswordWithInvalidPassword() throws Throwable
    {
        // Given
        manager.newUser( "jake", password( "abc123" ), false );
        byte[] newPassword = password( "abc123" );

        // When
        try
        {
            manager.setUserPassword( "jake", newPassword, false );
            fail( "exception expected" );
        }
        catch ( InvalidArgumentsException e )
        {
            // expected
        }

        // Then
        assertThat( newPassword, equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
    }

    @Test
    void shouldReturnNullWhenSettingPasswordForUnknownUser()
    {
        // When
        try
        {
            manager.setUserPassword( "unknown", password( "hello, world!" ), false );
            fail( "exception expected" );
        }
        catch ( InvalidArgumentsException e )
        {
            // expected
        }
    }

    @Test
     void shouldFailWhenAuthTokenIsInvalid()
    {
        assertException(
                () -> manager.login( map( AuthToken.SCHEME_KEY, "supercool", AuthToken.PRINCIPAL, "neo4j" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, scheme 'supercool' is not supported." );

        assertException(
                () -> manager.login( map( AuthToken.SCHEME_KEY, "none" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, scheme 'none' is only allowed when auth is disabled" );

        assertException(
                () -> manager.login( map( "key", "value" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, missing key `scheme`" );

        assertException(
                () -> manager.login( map( AuthToken.SCHEME_KEY, "basic", AuthToken.PRINCIPAL, "neo4j" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, missing key `credentials`" );

        assertException(
                () -> manager.login( map( AuthToken.SCHEME_KEY, "basic", AuthToken.CREDENTIALS, "very-secret" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, missing key `principal`" );
    }

    private void assertLoginGivesResult( String username, String password, AuthenticationResult expectedResult )
            throws InvalidAuthTokenException
    {
        LoginContext securityContext = manager.login( authToken( username, password ) );
        assertThat( securityContext.subject().getAuthenticationResult(), equalTo( expectedResult ) );
    }

    private void setMockAuthenticationStrategyResult( String username, String password, AuthenticationResult result ) throws InvalidArgumentsException
    {
        final User user = manager.getUser( username );
        when( authStrategy.authenticate( user, password( password ) ) ).thenReturn( result );
    }

    public static byte[] clearedPasswordWithSameLengthAs( String passwordString )
    {
        byte[] password = passwordString.getBytes( StandardCharsets.UTF_8 );
        Arrays.fill( password, (byte) 0 );
        return password;
    }
}
