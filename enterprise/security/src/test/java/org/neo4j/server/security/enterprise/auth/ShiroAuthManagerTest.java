/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.Credential;
import org.neo4j.server.security.auth.PasswordPolicy;
import org.neo4j.server.security.auth.User;

import org.neo4j.server.security.auth.InMemoryUserRepository;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShiroAuthManagerTest
{
    private InMemoryUserRepository users;
    private InMemoryRoleRepository roles;
    private AuthenticationStrategy authStrategy;
    private PasswordPolicy passwordPolicy;
    private ShiroAuthManager manager;

    @Before
    public void setUp() throws Throwable
    {
        users = new InMemoryUserRepository();
        roles = new InMemoryRoleRepository();
        authStrategy = mock( AuthenticationStrategy.class );
        passwordPolicy = mock( PasswordPolicy.class );
        manager = new ShiroAuthManager( users, roles, passwordPolicy, authStrategy, true );
        manager.init();
    }

    @After
    public void tearDown() throws Throwable
    {
        manager.stop();
        manager.shutdown();
    }

    @Test
    public void shouldCreateDefaultUserIfNoneExist() throws Throwable
    {
        // Given

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
        final User user = new User( "jake", Credential.forPassword( "abc123" ), false );
        users.create( user );
        manager.start();
        when( authStrategy.isAuthenticationPermitted( user.name() )).thenReturn( true );

        // When
        AuthenticationResult result = manager.login( "jake", "abc123" ).getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Test
    public void shouldFindAndAuthenticateUserAndReturnAuthStrategyResult() throws Throwable
    {
        // Given
        final User user = new User( "jake", Credential.forPassword( "abc123" ), true );
        users.create( user );
        manager.start();

        // When
        AuthSubject authSubject = manager.login( "jake", "abc123" );
        AuthenticationResult result = authSubject.getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.TOO_MANY_ATTEMPTS ) );
    }

    @Test
    public void shouldFindAndAuthenticateUserAndReturnPasswordChangeIfRequired() throws Throwable
    {
        // Given
        final User user = new User( "jake", Credential.forPassword( "abc123" ), true );
        users.create( user );
        manager.start();
        when( authStrategy.isAuthenticationPermitted( user.name() )).thenReturn( true );

        // When
        AuthenticationResult result = manager.login( "jake", "abc123" ).getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );
    }

    @Test
    public void shouldFailAuthenticationIfUserIsNotFound() throws Throwable
    {
        // Given
        final User user = new User( "jake", Credential.forPassword( "abc123" ), true );
        users.create( user );
        manager.start();
        when( authStrategy.isAuthenticationPermitted( "unknown" )).thenReturn( true );

        // When
        AuthSubject authSubject = manager.login( "unknown", "abc123" );
        AuthenticationResult result = authSubject.getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldCreateUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        manager.newUser( "foo", "bar", true );

        // Then
        User user = users.findByName( "foo" );
        assertNotNull( user );
        assertTrue( user.passwordChangeRequired() );
        assertTrue( user.credentials().matchesPassword( "bar" ) );
    }

    @Test
    public void shouldDeleteUser() throws Throwable
    {
        System.out.println("shouldDeleteUser");

        // Given
        final User user = new User( "jake", Credential.forPassword( "abc123" ), true );
        users.create( user );
        manager.start();

        // When
        manager.deleteUser( "jake" );

        // Then
        assertNull( users.findByName( "jake" ) );
    }

    @Test
    public void shouldDeleteUnknownUser() throws Throwable
    {
        // Given
        final User user = new User( "jake", Credential.forPassword( "abc123" ), true );
        users.create( user );
        manager.start();

        // When
        manager.deleteUser( "unknown" );

        // Then
        assertNotNull( users.findByName( "jake" ) );
    }

    @Test
    public void shouldSetPassword() throws Throwable
    {
        // Given
        users.create( new User( "jake", Credential.forPassword( "abc123" ), true ) );
        manager.start();

        // When
        manager.setUserPassword( "jake", "hello, world!" );

        // Then
        User user = manager.getUser( "jake" );
        assertTrue( user.credentials().matchesPassword( "hello, world!" ) );
        assertThat( users.findByName( "jake" ), equalTo( user ) );
    }

    @Test
    public void shouldSetPasswordThroughAuthSubject() throws Throwable
    {
        // Given
        users.create( new User( "neo", Credential.forPassword( "abc123" ), true ) );
        manager.start();
        when( authStrategy.isAuthenticationPermitted( "neo" )).thenReturn( true );

        // When
        AuthSubject authSubject = manager.login( "neo", "abc123" );
        assertThat( authSubject.getAuthenticationResult(), equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );

        authSubject.setPassword( "hello, world!" );

        // Then
        User user = manager.getUser( "neo" );
        assertTrue( user.credentials().matchesPassword( "hello, world!" ) );
        assertThat( users.findByName( "neo" ), equalTo( user ) );

        authSubject.logout();
        authSubject = manager.login( "neo", "hello, world!" );
        assertThat( authSubject.getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Test
    public void shouldReturnNullWhenSettingPasswordForUnknownUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        try
        {
            manager.setUserPassword( "unknown", "hello, world!" );
            fail( "exception expected" );
        }
        catch ( IllegalCredentialsException e )
        {
            // expected
        }
    }

    private void createTestUsers() throws Throwable
    {
        manager.newUser( "morpheus", "abc123", false );
        manager.newRole( "admin", "morpheus" );
        manager.newUser( "trinity", "abc123", false );
        manager.newRole( "architect", "trinity" );
        manager.newUser( "tank", "abc123", false );
        manager.newRole( "publisher", "tank" );
        manager.newUser( "neo", "abc123", false );
        manager.newRole( "reader", "neo" );
        manager.newUser( "smith", "abc123", false );
        manager.newRole( "agent", "smith" );
        when( authStrategy.isAuthenticationPermitted( anyString() ) ).thenReturn( true );
    }

    @Test
    public void defaultUserShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        when( authStrategy.isAuthenticationPermitted( anyString() ) ).thenReturn( true );
        manager.start();

        // When
        AuthSubject subject = manager.login( "neo4j", "neo4j");
        manager.setUserPassword( "neo4j", "1234" );
        subject.logout();
        subject = manager.login( "neo4j", "1234");

        // Then
        assertTrue( subject.allowsReads() );
        assertTrue( subject.allowsWrites() );
        assertTrue( subject.allowsSchemaWrites() );
    }

    @Test
    public void userWithAdminRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        AuthSubject subject = manager.login( "morpheus", "abc123");

        // Then
        assertTrue( subject.allowsReads() );
        assertTrue( subject.allowsWrites() );
        assertTrue( subject.allowsSchemaWrites() );
    }

    @Test
    public void userWithArchitectRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        AuthSubject subject = manager.login( "trinity", "abc123");

        // Then
        assertTrue( subject.allowsReads() );
        assertTrue( subject.allowsWrites() );
        assertTrue( subject.allowsSchemaWrites() );
    }

    @Test
    public void userWithPublisherRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        AuthSubject subject = manager.login( "tank", "abc123");

        // Then
        assertTrue( "should allow reads", subject.allowsReads() );
        assertTrue( "should allow writes", subject.allowsWrites() );
        assertFalse( "should _not_ allow schema writes", subject.allowsSchemaWrites() );
    }

    @Test
    public void userWithReaderRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        AuthSubject subject = manager.login( "neo", "abc123");

        // Then
        assertTrue( subject.allowsReads() );
        assertFalse( subject.allowsWrites() );
        assertFalse( subject.allowsSchemaWrites() );
    }

    @Test
    public void userWithNonPredefinedRoleShouldHaveNoPermissions() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        AuthSubject subject = manager.login( "smith", "abc123");

        // Then
        assertFalse( subject.allowsReads() );
        assertFalse( subject.allowsWrites() );
        assertFalse( subject.allowsSchemaWrites() );
    }

    @Test
    public void shouldHaveNoPermissionsAfterLogout() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        AuthSubject subject = manager.login( "morpheus", "abc123");
        assertTrue( subject.allowsReads() );
        assertTrue( subject.allowsWrites() );
        assertTrue( subject.allowsSchemaWrites() );

        subject.logout();

        // Then
        assertFalse( subject.allowsReads() );
        assertFalse( subject.allowsWrites() );
        assertFalse( subject.allowsSchemaWrites() );
    }

    @Test
    public void shouldThrowWhenAuthIsDisabled() throws Throwable
    {
        // Restart with auth disabled
        manager.stop();
        manager.shutdown();
        manager = new ShiroAuthManager( users, roles, passwordPolicy, authStrategy, false );
        manager.start();

        try
        {
            manager.login( "foo", "bar" );
            fail( "exception expected" );
        } catch ( IllegalStateException e )
        {
            // expected
        }

        try
        {
            manager.newUser( "foo", "bar", true );
            fail( "exception expected" );
        } catch ( IllegalStateException e )
        {
            // expected
        }

        try
        {
            manager.deleteUser( "foo" );
            fail( "exception expected" );
        } catch ( IllegalStateException e )
        {
            // expected
        }

        try
        {
            manager.getUser( "foo" );
            fail( "exception expected" );
        } catch ( IllegalStateException e )
        {
            // expected
        }

        try
        {
            manager.setUserPassword( "foo", "bar" );
            fail( "exception expected" );
        } catch ( IllegalStateException e )
        {
            // expected
        }

        assertTrue( users.numberOfUsers() == 0 );
    }
}
