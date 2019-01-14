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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.TOO_MANY_ATTEMPTS;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.test.assertion.Assert.assertException;

public class BasicAuthManagerTest extends InitialUserTest
{
    private BasicAuthManager manager;
    private AuthenticationStrategy authStrategy = mock( AuthenticationStrategy.class );

    @Before
    public void setup() throws Throwable
    {
        config = Config.defaults();
        users = CommunitySecurityModule.getUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        UserRepository initUserRepository =
                CommunitySecurityModule.getInitialUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        manager = new BasicAuthManager( users, mock( PasswordPolicy.class ), authStrategy, initUserRepository );
        manager.init();
    }

    @After
    public void teardown() throws Throwable
    {
        manager.stop();
    }

    @Test
    public void shouldFindAndAuthenticateUserSuccessfully() throws Throwable
    {
        // Given
        manager.start();
        User user1 = newUser( "jake", "abc123", false );
        users.create( user1 );
        final User user = user1;

        // When
        when( authStrategy.authenticate( user, "abc123" )).thenReturn( SUCCESS );

        // Then
        assertLoginGivesResult( "jake", "abc123", SUCCESS );
    }

    @Test
    public void shouldFindAndAuthenticateUserAndReturnAuthStrategyResult() throws Throwable
    {
        // Given
        manager.start();
        User user1 = newUser( "jake", "abc123", true );
        users.create( user1 );
        final User user = user1;

        // When
        when( authStrategy.authenticate( user, "abc123" )).thenReturn( TOO_MANY_ATTEMPTS );

        // Then
        assertLoginGivesResult( "jake", "abc123", TOO_MANY_ATTEMPTS );
    }

    @Test
    public void shouldFindAndAuthenticateUserAndReturnPasswordChangeIfRequired() throws Throwable
    {
        // Given
        manager.start();
        User user1 = newUser( "jake", "abc123", true );
        users.create( user1 );
        final User user = user1;

        // When
        when( authStrategy.authenticate( user, "abc123" )).thenReturn( SUCCESS );

        // Then
        assertLoginGivesResult( "jake", "abc123", PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    public void shouldFailAuthenticationIfUserIsNotFound() throws Throwable
    {
        // Given
        manager.start();
        User user = newUser( "jake", "abc123", true );
        users.create( user );

        // Then
        assertLoginGivesResult( "unknown", "abc123", FAILURE );
    }

    @Test
    public void shouldCreateUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        manager.newUser( "foo", "bar", true );

        // Then
        User user = users.getUserByName( "foo" );
        assertNotNull( user );
        assertTrue( user.passwordChangeRequired() );
        assertTrue( user.credentials().matchesPassword( "bar" ) );
    }

    @Test
    public void shouldDeleteUser() throws Throwable
    {
        // Given
        manager.start();
        manager.newUser( "jake", "abc123", true );

        // When
        manager.deleteUser( "jake" );

        // Then
        assertNull( users.getUserByName( "jake" ) );
    }

    @Test
    public void shouldFailToDeleteUnknownUser() throws Throwable
    {
        // Given
        manager.start();
        manager.newUser( "jake", "abc123", true );

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
        assertNotNull( users.getUserByName( "jake" ) );
    }

    @Test
    public void shouldSetPassword() throws Throwable
    {
        // Given
        manager.start();
        manager.newUser( "jake", "abc123", true );

        // When
        manager.setUserPassword( "jake", "hello, world!", false );

        // Then
        User user = manager.getUser( "jake" );
        assertTrue( user.credentials().matchesPassword( "hello, world!" ) );
        assertThat( users.getUserByName( "jake" ), equalTo( user ) );
    }

    @Test
    public void shouldReturnNullWhenSettingPasswordForUnknownUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        try
        {
            manager.setUserPassword( "unknown", "hello, world!", false );
            fail( "exception expected" );
        }
        catch ( InvalidArgumentsException e )
        {
            // expected
        }
    }

    @Test
    public void shouldFailWhenAuthTokenIsInvalid() throws Throwable
    {
        manager.start();

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

    @Override
    protected AuthManager authManager()
    {
        return manager;
    }
}
