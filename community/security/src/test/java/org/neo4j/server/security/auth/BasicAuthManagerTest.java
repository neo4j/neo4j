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
package org.neo4j.server.security.auth;

import junit.framework.TestCase;
import org.hamcrest.core.IsEqual;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.security.AuthenticationResult.*;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

public class BasicAuthManagerTest
{
    private InMemoryUserRepository users;
    private BasicAuthManager manager;
    private AuthenticationStrategy authStrategy = mock( AuthenticationStrategy.class );;

    @Before
    public void setup() throws Throwable
    {
        users = new InMemoryUserRepository();
        manager = new BasicAuthManager( users, mock( PasswordPolicy.class ), authStrategy );
        manager.start();
    }

    @After
    public void teardown() throws Throwable
    {
        manager.stop();
    }

    @Test
    public void shouldCreateDefaultUserIfNoneExist()
    {
        // When
        final User user = users.getUserByName( "neo4j" );

        // Then
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( "neo4j" ) );
        assertTrue( user.passwordChangeRequired() );
    }

    @Test
    public void shouldFindAndAuthenticateUserSuccessfully() throws Throwable
    {
        // Given
        final User user = createUser( "jake", "abc123", false );

        // When
        when( authStrategy.authenticate( user, "abc123" )).thenReturn( SUCCESS );

        // Then
        assertLoginGivesResult( "jake", "abc123", SUCCESS );
    }

    @Test
    public void shouldFindAndAuthenticateUserAndReturnAuthStrategyResult() throws Throwable
    {
        // Given
        final User user = createUser( "jake", "abc123", true );

        // When
        when( authStrategy.authenticate( user, "abc123" )).thenReturn( TOO_MANY_ATTEMPTS );

        // Then
        assertLoginGivesResult( "jake", "abc123", TOO_MANY_ATTEMPTS );
    }

    @Test
    public void shouldFindAndAuthenticateUserAndReturnPasswordChangeIfRequired() throws Throwable
    {
        // Given
        final User user = createUser( "jake", "abc123", true );

        // When
        when( authStrategy.authenticate( user, "abc123" )).thenReturn( SUCCESS );

        // Then
        assertLoginGivesResult( "jake", "abc123", PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    public void shouldFailAuthenticationIfUserIsNotFound() throws Throwable
    {
        // Given
        createUser( "jake", "abc123", true );

        // Then
        assertLoginGivesResult( "unknown", "abc123", FAILURE );
    }

    @Test
    public void shouldCreateUser() throws Throwable
    {
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
        manager.newUser( "jake", "abc123", true );

        try
        {
            // When
            manager.deleteUser( "nonExistentUser" );
            TestCase.fail("User 'nonExistentUser' should no longer exist, expected exception.");
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
        manager.newUser( "jake", "abc123", true );

        // When
        manager.setUserPassword( "jake", "hello, world!" );

        // Then
        User user = manager.getUser( "jake" );
        assertTrue( user.credentials().matchesPassword( "hello, world!" ) );
        assertThat( users.getUserByName( "jake" ), equalTo( user ) );
    }

    @Test
    public void shouldReturnNullWhenSettingPasswordForUnknownUser() throws Throwable
    {
        // When
        try
        {
            manager.setUserPassword( "unknown", "hello, world!" );
            fail( "exception expected" );
        }
        catch ( InvalidArgumentsException e )
        {
            // expected
        }
    }

    private User createUser( String username, String password, boolean pwd_change )
            throws IOException, InvalidArgumentsException
    {
        User user = new User.Builder( username, Credential.forPassword( password ))
            .withRequiredPasswordChange( pwd_change ).build();
        users.create(user);
        return user;
    }

    private void assertLoginGivesResult( String username, String password, AuthenticationResult expectedResult )
            throws InvalidAuthTokenException
    {
        AuthSubject authSubject = manager.login( authToken( username, password ) );
        assertThat( authSubject.getAuthenticationResult(), equalTo( expectedResult ) );
    }
}
