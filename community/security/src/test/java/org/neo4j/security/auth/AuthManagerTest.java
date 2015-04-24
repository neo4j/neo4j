/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.security.auth;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AuthManagerTest
{
    @Test
    public void shouldCreateDefaultUserIfNoneExist() throws Throwable
    {
        // Given
        final InMemoryUserRepository users = new InMemoryUserRepository();
        final AuthManager manager = new AuthManager( users, mock( AuthenticationStrategy.class ) );

        // When
        manager.start();

        // Then
        final User user = users.findByName( "neo4j" );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( "neo4j" ) );
        assertTrue( user.passwordChangeRequired() );
    }

    @Test
    public void shouldFindAndAuthenticateUserSuccessfully() throws Throwable
    {
        // Given
        final InMemoryUserRepository users = new InMemoryUserRepository();
        final User user = new User( "jake", Credential.forPassword( "abc123" ), false );
        users.create( user );
        final AuthenticationStrategy authStrategy = mock( AuthenticationStrategy.class );
        final AuthManager manager = new AuthManager( users, authStrategy );
        manager.start();
        Mockito.when( authStrategy.authenticate( user, "abc123" ) ).thenReturn( AuthenticationResult.SUCCESS );

        // When
        AuthenticationResult result = manager.authenticate( "jake", "abc123" );

        // Then
        verify( authStrategy ).authenticate( user, "abc123" );
        assertThat( result, equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Test
    public void shouldFindAndAuthenticateUserAndReturnAuthStrategyResult() throws Throwable
    {
        // Given
        final InMemoryUserRepository users = new InMemoryUserRepository();
        final User user = new User( "jake", Credential.forPassword( "abc123" ), true );
        users.create( user );
        final AuthenticationStrategy authStrategy = mock( AuthenticationStrategy.class );
        final AuthManager manager = new AuthManager( users, authStrategy );
        manager.start();
        Mockito.when( authStrategy.authenticate( user, "abc123" ) ).thenReturn( AuthenticationResult.TOO_MANY_ATTEMPTS );

        // When
        AuthenticationResult result = manager.authenticate( "jake", "abc123" );

        // Then
        verify( authStrategy ).authenticate( user, "abc123" );
        assertThat( result, equalTo( AuthenticationResult.TOO_MANY_ATTEMPTS ) );
    }

    @Test
    public void shouldFindAndAuthenticateUserAndReturnPasswordChangeIfRequired() throws Throwable
    {
        // Given
        final InMemoryUserRepository users = new InMemoryUserRepository();
        final User user = new User( "jake", Credential.forPassword( "abc123" ), true );
        users.create( user );
        final AuthenticationStrategy authStrategy = mock( AuthenticationStrategy.class );
        final AuthManager manager = new AuthManager( users, authStrategy );
        manager.start();
        Mockito.when( authStrategy.authenticate( user, "abc123" ) ).thenReturn( AuthenticationResult.SUCCESS );

        // When
        AuthenticationResult result = manager.authenticate( "jake", "abc123" );

        // Then
        verify( authStrategy ).authenticate( user, "abc123" );
        assertThat( result, equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );
    }

    @Test
    public void shouldFailAuthenticationIfUserIsNotFound() throws Throwable
    {
        // Given
        final InMemoryUserRepository users = new InMemoryUserRepository();
        final User user = new User( "jake", Credential.forPassword( "abc123" ), true );
        users.create( user );
        final AuthenticationStrategy authStrategy = mock( AuthenticationStrategy.class );
        final AuthManager manager = new AuthManager( users, authStrategy );
        manager.start();

        // When
        AuthenticationResult result = manager.authenticate( "unknown", "abc123" );

        // Then
        Mockito.verifyNoMoreInteractions( authStrategy );
        assertThat( result, equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldCreateUser() throws Throwable
    {
        // Given
        final InMemoryUserRepository users = new InMemoryUserRepository();
        final AuthManager manager = new AuthManager( users, mock( AuthenticationStrategy.class ) );
        manager.start();

        // When
        manager.newUser( "foo", "bar", true );

        // Then
        User user = users.findByName( "foo" );
        Assert.assertNotNull( user );
        Assert.assertTrue( user.passwordChangeRequired() );
        Assert.assertTrue( user.credentials().matchesPassword( "bar" ) );
    }

    @Test
    public void shouldDeleteUser() throws Throwable
    {
        // Given
        final InMemoryUserRepository users = new InMemoryUserRepository();
        final User user = new User( "jake", Credential.forPassword( "abc123" ), true );
        users.create( user );
        final AuthManager manager = new AuthManager( users, mock( AuthenticationStrategy.class ) );
        manager.start();

        // When
        manager.deleteUser( "jake" );

        // Then
        Assert.assertNull( users.findByName( "jake" ) );
    }

    @Test
    public void shouldDeleteUnknownUser() throws Throwable
    {
        // Given
        final InMemoryUserRepository users = new InMemoryUserRepository();
        final User user = new User( "jake", Credential.forPassword( "abc123" ), true );
        users.create( user );
        final AuthManager manager = new AuthManager( users, mock( AuthenticationStrategy.class ) );
        manager.start();

        // When
        manager.deleteUser( "unknown" );

        // Then
        Assert.assertNotNull( users.findByName( "jake" ) );
    }

    @Test
    public void shouldSetPassword() throws Throwable
    {
        // Given
        final InMemoryUserRepository users = new InMemoryUserRepository();
        users.create( new User( "jake", Credential.forPassword( "abc123" ), true ) );
        final AuthManager manager = new AuthManager( users, mock( AuthenticationStrategy.class ) );
        manager.start();

        // When
        User user = manager.setPassword( "jake", "hello, world!" );

        // Then
        Assert.assertTrue( user.credentials().matchesPassword( "hello, world!" ) );
        assertThat( users.findByName( "jake" ), equalTo( user ) );
    }

    @Test
    public void shouldReturnNullWhenSettingPasswordForUnknownUser() throws Throwable
    {
        // Given
        final InMemoryUserRepository users = new InMemoryUserRepository();
        final AuthManager manager = new AuthManager( users, mock( AuthenticationStrategy.class ) );
        manager.start();

        // When
        User user = manager.setPassword( "unknown", "hello, world!" );

        // Then
        Assert.assertNull( user );
    }

    @Test
    public void shouldThrowWhenAuthIsDisabled() throws Throwable
    {
        final InMemoryUserRepository users = new InMemoryUserRepository();
        final AuthManager manager = new AuthManager( users, mock( AuthenticationStrategy.class ), false );
        manager.start();

        try
        {
            manager.authenticate( "foo", "bar" );
            Assert.fail( "exception expected" );
        } catch ( IllegalStateException e )
        {
            // expected
        }

        try
        {
            manager.newUser( "foo", "bar", true );
            Assert.fail( "exception expected" );
        } catch ( IllegalStateException e )
        {
            // expected
        }

        try
        {
            manager.deleteUser( "foo" );
            Assert.fail( "exception expected" );
        } catch ( IllegalStateException e )
        {
            // expected
        }

        try
        {
            manager.getUser( "foo" );
            Assert.fail( "exception expected" );
        } catch ( IllegalStateException e )
        {
            // expected
        }

        try
        {
            manager.setPassword( "foo", "bar" );
            Assert.fail( "exception expected" );
        } catch ( IllegalStateException e )
        {
            // expected
        }

        Assert.assertTrue( users.numberOfUsers() == 0 );
    }
}
