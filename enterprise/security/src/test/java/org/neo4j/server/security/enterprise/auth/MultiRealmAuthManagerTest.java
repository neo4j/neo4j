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

import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.security.enterprise.log.SecurityLog;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.Credential;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.PasswordPolicy;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.Strings.escape;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.test.assertion.Assert.assertException;

public class MultiRealmAuthManagerTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private Config config;
    private UserRepository users;
    private AuthenticationStrategy authStrategy;
    private MultiRealmAuthManager manager;
    private EnterpriseUserManager userManager;
    private AssertableLogProvider logProvider;

    @Before
    public void setUp() throws Throwable
    {
        config = Config.defaults();
        users = CommunitySecurityModule.getUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        authStrategy = mock( AuthenticationStrategy.class );
        logProvider = new AssertableLogProvider();

        manager = createAuthManager( true );
        userManager = manager.getUserManager();
    }

    private MultiRealmAuthManager createAuthManager( boolean logSuccessfulAuthentications ) throws Throwable
    {
        Log log = logProvider.getLog( this.getClass() );

        InternalFlatFileRealm internalFlatFileRealm =
                new InternalFlatFileRealm(
                        users,
                        new InMemoryRoleRepository(),
                        mock( PasswordPolicy.class ),
                        authStrategy,
                        mock( JobScheduler.class ),
                        CommunitySecurityModule.getInitialUserRepository(
                                config, NullLogProvider.getInstance(), fsRule.get() )
                    );

        manager = new MultiRealmAuthManager( internalFlatFileRealm, Collections.singleton( internalFlatFileRealm ),
                new MemoryConstrainedCacheManager(), new SecurityLog( log ), logSuccessfulAuthentications );

        manager.init();
        return manager;
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
        // When
        manager.start();

        // Then
        final User user = users.getUserByName( "neo4j" );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( "neo4j" ) );
        assertTrue( user.passwordChangeRequired() );
    }

    @Test
    public void shouldLoadInitialUserIfNoneExist() throws Throwable
    {
        // Given
        FileUserRepository initialUserRepository =
                CommunitySecurityModule.getInitialUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        initialUserRepository.start();
        initialUserRepository.create(
                new User.Builder( "initUser", Credential.forPassword( "123" ))
                        .withRequiredPasswordChange( false )
                        .build()
        );
        initialUserRepository.shutdown();

        // When
        manager.start();

        // Then
        final User user = users.getUserByName( "initUser" );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( "123" ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    public void shouldAddInitialUserIfUsersExist() throws Throwable
    {
        // Given
        FileUserRepository initialUserRepository =
                CommunitySecurityModule.getInitialUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        initialUserRepository.start();
        initialUserRepository.create( newUser( "initUser", "123", false ) );
        initialUserRepository.shutdown();
        users.start();
        users.create( newUser( "oldUser", "321", false ) );
        users.shutdown();

        // When
        manager.start();

        // Then
        final User initUser = users.getUserByName( "initUser" );
        assertNotNull( initUser );
        assertTrue( initUser.credentials().matchesPassword( "123" ) );
        assertFalse( initUser.passwordChangeRequired() );

        final User oldUser = users.getUserByName( "oldUser" );
        assertNotNull( oldUser );
        assertTrue( oldUser.credentials().matchesPassword( "321" ) );
        assertFalse( oldUser.passwordChangeRequired() );
    }

    @Test
    public void shouldUpdateUserIfInitialUserExist() throws Throwable
    {
        // Given
        FileUserRepository initialUserRepository =
                CommunitySecurityModule.getInitialUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        initialUserRepository.start();
        initialUserRepository.create( newUser( "oldUser", "newPassword", false ) );
        initialUserRepository.shutdown();
        users.start();
        users.create( newUser( "oldUser", "oldPassword", true ) );
        users.shutdown();

        // When
        manager.start();

        // Then
        final User oldUser = users.getUserByName( "oldUser" );
        assertNotNull( oldUser );
        assertTrue( oldUser.credentials().matchesPassword( "newPassword" ) );
        assertFalse( oldUser.passwordChangeRequired() );
    }

    @Test
    public void shouldFindAndAuthenticateUserSuccessfully() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123" , false ) );
        manager.start();
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // When
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.SUCCESS ) );
        logProvider.assertExactly( info( "[jake]: logged in" ) );
    }

    @Test
    public void shouldNotLogAuthenticationIfFlagSaysNo() throws Throwable
    {
        // Given
        manager.shutdown();
        manager = createAuthManager( false );

        users.create( newUser( "jake", "abc123" , false ) );
        manager.start();
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // When
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.SUCCESS ) );
        logProvider.assertNone( info( "[jake]: logged in" ) );
    }

    @Test
    public void shouldReturnTooManyAttemptsWhenThatIsAppropriate() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123" , true ) );
        manager.start();
        setMockAuthenticationStrategyResult( "jake", "wrong password", AuthenticationResult.TOO_MANY_ATTEMPTS );

        // When
        AuthSubject authSubject = manager.login( authToken( "jake", "wrong password" ) );
        AuthenticationResult result = authSubject.getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.TOO_MANY_ATTEMPTS ) );
        logProvider.assertExactly(
                error( "[%s]: failed to log in: too many failed attempts", "jake" ) );
    }

    @Test
    public void shouldFindAndAuthenticateUserAndReturnPasswordChangeIfRequired() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123" , true ) );
        manager.start();
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // When
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );
        logProvider.assertExactly( info( "[jake]: logged in" ) );
    }

    @Test
    public void shouldFailWhenAuthTokenIsInvalid() throws Throwable
    {
        manager.start();

        assertException( () -> manager.login( map( AuthToken.SCHEME_KEY, "supercool", AuthToken.PRINCIPAL, "neo4j" ) ),
                InvalidAuthTokenException.class, "does not support authentication token" );
        assertException( () -> manager.login( map( "key", "value" ) ),
                InvalidAuthTokenException.class, "The value associated with the key `scheme` must be a String but was: null" );
        assertException( () -> manager.login( map( AuthToken.SCHEME_KEY, "basic", AuthToken.PRINCIPAL, "neo4j" ) ),
                InvalidAuthTokenException.class, "The value associated with the key `credentials` must be a String but was: null" );
    }

    @Test
    public void shouldFailAuthenticationIfUserIsNotFound() throws Throwable
    {
        // Given
        manager.start();

        // When
        AuthSubject authSubject = manager.login( authToken( "unknown", "abc123" ) );
        AuthenticationResult result = authSubject.getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.FAILURE ) );
        logProvider.assertExactly( error( "[%s]: failed to log in: invalid principal or credentials", "unknown" ) );
    }

    @Test
    public void shouldFailAuthenticationAndEscapeIfUserIsNotFound() throws Throwable
    {
        // Given
        manager.start();

        // When
        AuthSubject authSubject = manager.login( authToken( "unknown\n\t\r\"haxx0r\"", "abc123" ) );
        AuthenticationResult result = authSubject.getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.FAILURE ) );
        logProvider.assertExactly( error( "[%s]: failed to log in: invalid principal or credentials",
                escape( "unknown\n\t\r\"haxx0r\"" ) ) );
    }

    @Test
    public void shouldCreateUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        userManager.newUser( "foo", "bar", true );

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
        final User user = newUser( "jake", "abc123" , true );
        final User user2 = newUser( "craig", "321cba" , true );
        users.create( user );
        users.create( user2 );
        manager.start();

        // When
        userManager.deleteUser( "jake" );

        // Then
        assertNull( users.getUserByName( "jake" ) );
        assertNotNull( users.getUserByName( "craig" ) );
    }

    @Test
    public void shouldFailDeletingUnknownUser() throws Throwable
    {
        // Given
        final User user = newUser( "jake", "abc123" , true );
        users.create( user );
        manager.start();

        // When
        assertException( () -> userManager.deleteUser( "unknown" ),
                InvalidArgumentsException.class, "User 'unknown' does not exist" );

        // Then
        assertNotNull( users.getUserByName( "jake" ) );
    }

    @Test
    public void shouldSuspendExistingUser() throws Throwable
    {
        // Given
        final User user = newUser( "jake", "abc123" , true );
        users.create( user );
        manager.start();

        // When
        userManager.suspendUser( "jake" );

        // Then
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );
        AuthSubject authSubject = manager.login( authToken( "jake", "abc123" ) );
        assertThat( authSubject.getAuthenticationResult(), equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldActivateExistingUser() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123", false ) );
        manager.start();

        userManager.suspendUser( "jake" );

        // When
        userManager.activateUser( "jake", false );
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // Then
        AuthSubject authSubject = manager.login( authToken( "jake", "abc123" ) );
        assertThat( authSubject.getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Test
    public void shouldSuspendSuspendedUser() throws Throwable
    {
        // Given
        final User user = newUser( "jake", "abc123", false );
        users.create( user );
        manager.start();
        userManager.suspendUser( "jake" );

        // When
        userManager.suspendUser( "jake" );
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // Then
        AuthSubject authSubject = manager.login( authToken( "jake", "abc123" ) );
        assertThat( authSubject.getAuthenticationResult(), equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldActivateActiveUser() throws Throwable
    {
        // Given
        final User user = newUser( "jake", "abc123", false );
        users.create( user );
        manager.start();
        when( authStrategy.authenticate( user, "abc123" ) ).thenReturn( AuthenticationResult.SUCCESS );

        // When
        userManager.activateUser( "jake", false );
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // Then
        AuthSubject authSubject = manager.login( authToken( "jake", "abc123" ) );
        assertThat( authSubject.getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Test
    public void shouldFailToSuspendNonExistingUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        try
        {
            userManager.suspendUser( "jake" );
            fail( "Should throw exception on suspending unknown user" );
        }
        catch ( InvalidArgumentsException e )
        {
            // Then
            assertThat(e.getMessage(), containsString("User 'jake' does not exist"));
        }
    }

    @Test
    public void shouldFailToActivateNonExistingUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        try
        {
            userManager.activateUser( "jake", false );
            fail( "Should throw exception on activating unknown user" );
        }
        catch ( InvalidArgumentsException e )
        {
            // Then
            assertThat(e.getMessage(), containsString("User 'jake' does not exist"));
        }
    }

    @Test
    public void shouldSetPassword() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123", true ) );
        manager.start();

        // When
        userManager.setUserPassword( "jake", "hello, world!", false );

        // Then
        User user = userManager.getUser( "jake" );
        assertTrue( user.credentials().matchesPassword( "hello, world!" ) );
        assertThat( users.getUserByName( "jake" ), equalTo( user ) );
    }

    @Test
    public void shouldSetPasswordThroughAuthSubject() throws Throwable
    {
        // Given
        users.create( newUser( "neo", "abc123", true ) );
        manager.start();
        setMockAuthenticationStrategyResult( "neo", "abc123", AuthenticationResult.SUCCESS );

        // When
        AuthSubject authSubject = manager.login( authToken( "neo", "abc123" ) );
        assertThat( authSubject.getAuthenticationResult(), equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );

        authSubject.setPassword( "hello, world!", false );
        setMockAuthenticationStrategyResult( "neo", "hello, world!", AuthenticationResult.SUCCESS );

        // Then
        final User updatedUser = userManager.getUser( "neo" );

        assertTrue( updatedUser.credentials().matchesPassword( "hello, world!" ) );
        assertThat( users.getUserByName( "neo" ), equalTo( updatedUser ) );

        authSubject.logout();
        authSubject = manager.login( authToken( "neo", "hello, world!" ) );
        assertThat( authSubject.getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );
        logProvider.assertExactly(
                info( "[neo]: logged in" ),
                info( "[neo]: changed password%s", "" ),
                info( "[neo]: logged in" ) );
    }

    @Test
    public void shouldNotRequestPasswordChangeWithInvalidCredentials() throws Throwable
    {
        // Given
        users.create( newUser( "neo", "abc123", true ) );
        manager.start();
        setMockAuthenticationStrategyResult( "neo", "abc123", AuthenticationResult.SUCCESS );
        setMockAuthenticationStrategyResult( "neo", "wrong", AuthenticationResult.FAILURE );

        // When
        AuthSubject authSubject = manager.login( authToken( "neo", "wrong" ) );

        // Then
        assertThat( authSubject.getAuthenticationResult(), equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldReturnNullWhenSettingPasswordForUnknownUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        try
        {
            userManager.setUserPassword( "unknown", "hello, world!", false );
            fail( "exception expected" );
        }
        catch ( InvalidArgumentsException e )
        {
            // expected
        }
    }

    private void createTestUsers() throws Throwable
    {
        userManager.newUser( "morpheus", "abc123", false );
        userManager.newRole( "admin", "morpheus" );
        setMockAuthenticationStrategyResult( "morpheus", "abc123", AuthenticationResult.SUCCESS );

        userManager.newUser( "trinity", "abc123", false );
        userManager.newRole( "architect", "trinity" );
        setMockAuthenticationStrategyResult( "trinity", "abc123", AuthenticationResult.SUCCESS );

        userManager.newUser( "tank", "abc123", false );
        userManager.newRole( "publisher", "tank" );
        setMockAuthenticationStrategyResult( "tank", "abc123", AuthenticationResult.SUCCESS );

        userManager.newUser( "neo", "abc123", false );
        userManager.newRole( "reader", "neo" );
        setMockAuthenticationStrategyResult( "neo", "abc123", AuthenticationResult.SUCCESS );

        userManager.newUser( "smith", "abc123", false );
        userManager.newRole( "agent", "smith" );
        setMockAuthenticationStrategyResult( "smith", "abc123", AuthenticationResult.SUCCESS );
    }

    @Test
    public void defaultUserShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        manager.start();
        setMockAuthenticationStrategyResult( "neo4j", "neo4j", AuthenticationResult.SUCCESS );

        // When
        AuthSubject subject = manager.login( authToken( "neo4j", "neo4j" ) );
        userManager.setUserPassword( "neo4j", "1234", false );
        subject.logout();

        setMockAuthenticationStrategyResult( "neo4j", "1234", AuthenticationResult.SUCCESS );
        subject = manager.login( authToken( "neo4j", "1234" ) );

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
        AuthSubject subject = manager.login( authToken( "morpheus", "abc123" ) );

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
        AuthSubject subject = manager.login( authToken( "trinity", "abc123" ) );

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
        AuthSubject subject = manager.login( authToken( "tank", "abc123" ) );

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
        AuthSubject subject = manager.login( authToken( "neo", "abc123" ) );

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
        AuthSubject subject = manager.login( authToken( "smith", "abc123" ) );

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
        AuthSubject subject = manager.login( authToken( "morpheus", "abc123" ) );
        assertTrue( subject.allowsReads() );
        assertTrue( subject.allowsWrites() );
        assertTrue( subject.allowsSchemaWrites() );

        subject.logout();

        // Then
        assertFalse( subject.allowsReads() );
        assertFalse( subject.allowsWrites() );
        assertFalse( subject.allowsSchemaWrites() );
    }

    private AssertableLogProvider.LogMatcher info( String message )
    {
        return inLog( this.getClass() ).info( message );
    }

    private AssertableLogProvider.LogMatcher info( String message, String... arguments )
    {
        return inLog( this.getClass() ).info( message, (Object[]) arguments );
    }

    private AssertableLogProvider.LogMatcher error( String message, String... arguments )
    {
        return inLog( this.getClass() ).error( message, (Object[]) arguments );
    }

    private User newUser( String userName, String password, boolean pwdChange )
    {
        return new User.Builder( userName, Credential.forPassword( password ))
                    .withRequiredPasswordChange( pwdChange )
                    .build();
    }

    private void setMockAuthenticationStrategyResult( String username, String password, AuthenticationResult result )
    {
        final User user = users.getUserByName( username );
        when( authStrategy.authenticate( user, password ) ).thenReturn( result );
    }
}
