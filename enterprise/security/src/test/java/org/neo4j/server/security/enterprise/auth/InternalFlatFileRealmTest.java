/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.PrincipalCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.enterprise.auth.AuthTestUtil.listOf;

public class InternalFlatFileRealmTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private MultiRealmAuthManager authManager;
    private TestRealm testRealm;
    private Function<String, Integer> token = s -> -1;

    @Before
    public void setup() throws Throwable
    {
        testRealm = new TestRealm(
                        new InMemoryUserRepository(),
                        new InMemoryRoleRepository(),
                        new BasicPasswordPolicy(),
                        newRateLimitedAuthStrategy(),
                        mock( JobScheduler.class ),
                        new InMemoryUserRepository(),
                        new InMemoryUserRepository()
                    );

        List<Realm> realms = listOf( testRealm );

        authManager = new MultiRealmAuthManager( testRealm, realms, new MemoryConstrainedCacheManager(),
                mock( SecurityLog.class ), true, false, Collections.emptyMap() );

        authManager.init();
        authManager.start();

        authManager.getUserManager().newUser( "mike", "123", false );
    }

    @Test
    public void shouldNotCacheAuthenticationInfo() throws InvalidAuthTokenException
    {
        // Given
        EnterpriseLoginContext mike = authManager.login( authToken( "mike", "123" ) );
        assertThat( mike.subject().getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );

        // When
        mike = authManager.login( authToken( "mike", "123" ) );
        assertThat( mike.subject().getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );

        // Then
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );
    }

    @Test
    public void shouldNotCacheAuthorizationInfo() throws InvalidAuthTokenException
    {
        // Given
        EnterpriseLoginContext mike = authManager.login( authToken( "mike", "123" ) );
        assertThat( mike.subject().getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );

        mike.authorize( token ).mode().allowsReads();
        assertThat( "Test realm did not receive a call", testRealm.takeAuthorizationFlag(), is( true ) );

        // When
        mike.authorize( token ).mode().allowsWrites();

        // Then
        assertThat( "Test realm did not receive a call", testRealm.takeAuthorizationFlag(), is( true ) );
    }

    @Test
    public void shouldOnlyReloadUsersOrRolesIfNeeded() throws Throwable
    {
        assertSetUsersAndRolesNTimes( false, false, 0, 0 );
        assertSetUsersAndRolesNTimes( false, true, 0, 1 );
        assertSetUsersAndRolesNTimes( true, false, 1, 0 );
        assertSetUsersAndRolesNTimes( true, true, 1, 1 );
    }

    @Test
    public void shouldAssignAdminRoleToDefaultUser() throws Throwable
    {
        // Given
        InternalFlatFileRealm realm = internalTestRealmWithUsers( Collections.emptyList(), Collections.emptyList() );

        // When
        realm.initialize();
        realm.start();

        // Then
        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ),
                contains( InternalFlatFileRealm.INITIAL_USER_NAME ) );
    }

    @Test
    public void shouldAssignAdminRoleToSpecifiedUser() throws Throwable
    {
        // Given
        InternalFlatFileRealm realm = internalTestRealmWithUsers( Arrays.asList( "neo4j", "morpheus", "trinity" ),
                Collections.singletonList( "morpheus" ) );

        // When
        realm.initialize();
        realm.start();

        // Then
        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "morpheus" ) );
        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ).size(), equalTo( 1 ) );
    }

    @Test
    public void shouldAssignAdminRoleToOnlyUser() throws Throwable
    {
        // Given
        InternalFlatFileRealm realm =
                internalTestRealmWithUsers( Collections.singletonList( "morpheus" ), Collections.emptyList() );

        // When
        realm.initialize();
        realm.start();

        // Then
        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "morpheus" ) );
        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ).size(), equalTo( 1 ) );
    }

    @Test
    public void shouldNotAssignAdminToNonExistentUser() throws Throwable
    {
        // Given
        InternalFlatFileRealm realm = internalTestRealmWithUsers( Collections.singletonList( "neo4j" ),
                Collections.singletonList( "morpheus" ) );

        // Expect
        exception.expect( InvalidArgumentsException.class );
        exception.expectMessage(
                "No roles defined, and default admin user 'morpheus' does not exist. Please use `neo4j-admin " +
                        SetDefaultAdminCommand.COMMAND_NAME + "` to select a valid admin." );

        // When
        realm.initialize();
        realm.start();
    }

    @Test
    public void shouldGiveErrorOnMultipleUsersNoDefault() throws Throwable
    {
        // Given
        InternalFlatFileRealm realm =
                internalTestRealmWithUsers( Arrays.asList( "morpheus", "trinity" ), Collections.emptyList() );

        // Expect
        exception.expect( InvalidArgumentsException.class );
        exception.expectMessage(
                "No roles defined, and cannot determine which user should be admin. Please use `neo4j-admin " +
                        SetDefaultAdminCommand.COMMAND_NAME + "` to select an admin." );

        // When
        realm.initialize();
        realm.start();
    }

    @Test
    public void shouldFailToAssignMultipleDefaultAdmins() throws Throwable
    {
        // Given
        InternalFlatFileRealm realm = internalTestRealmWithUsers( Arrays.asList( "morpheus", "trinity", "tank" ),
                Arrays.asList( "morpheus", "trinity" ) );

        // Expect
        exception.expect( InvalidArgumentsException.class );
        exception.expectMessage(
                "No roles defined, and multiple users defined as default admin user. Please use `neo4j-admin " +
                        SetDefaultAdminCommand.COMMAND_NAME + "` to select a valid admin." );

        // When
        realm.initialize();
        realm.start();
    }

    @Test
    public void shouldAssignAdminRoleAfterBadSetting() throws Throwable
    {
        UserRepository userRepository = new InMemoryUserRepository();
        UserRepository initialUserRepository = new InMemoryUserRepository();
        UserRepository adminUserRepository = new InMemoryUserRepository();
        RoleRepository roleRepository = new InMemoryRoleRepository();
        userRepository.create( newUser( "morpheus", "123", false ) );
        userRepository.create( newUser( "trinity", "123", false ) );

        InternalFlatFileRealm realm = new InternalFlatFileRealm(
                userRepository,
                roleRepository,
                new BasicPasswordPolicy(),
                newRateLimitedAuthStrategy(),
                new InternalFlatFileRealmIT.TestJobScheduler(),
                initialUserRepository,
                adminUserRepository
        );

        try
        {
            realm.initialize();
            realm.start();
            fail( "Multiple users, no default admin provided" );
        }
        catch ( InvalidArgumentsException e )
        {
            realm.stop();
            realm.shutdown();
        }
        adminUserRepository.create( new User.Builder( "trinity", Credential.INACCESSIBLE ).build() );
        realm.initialize();
        realm.start();
        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ).size(), equalTo( 1 ) );
        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "trinity" ) );
    }

    private InternalFlatFileRealm internalTestRealmWithUsers( List<String> existing, List<String> defaultAdmin )
            throws Throwable
    {
        UserRepository userRepository = new InMemoryUserRepository();
        UserRepository initialUserRepository = new InMemoryUserRepository();
        UserRepository adminUserRepository = new InMemoryUserRepository();
        RoleRepository roleRepository = new InMemoryRoleRepository();
        for ( String user : existing )
        {
            userRepository.create( newUser( user, "123", false ) );
        }
        for ( String user : defaultAdmin )
        {
            adminUserRepository.create( new User.Builder( user, Credential.INACCESSIBLE ).build() );
        }
        return new InternalFlatFileRealm(
                userRepository,
                roleRepository,
                new BasicPasswordPolicy(),
                newRateLimitedAuthStrategy(),
                new InternalFlatFileRealmIT.TestJobScheduler(),
                initialUserRepository,
                adminUserRepository
        );
    }

    private User newUser( String userName, String password, boolean pwdChange )
    {
        return new User.Builder( userName, Credential.forPassword( password ) ).withRequiredPasswordChange( pwdChange )
            .build();
    }

    private void assertSetUsersAndRolesNTimes( boolean usersChanged, boolean rolesChanged,
            int nSetUsers, int nSetRoles ) throws Throwable
    {
        final UserRepository userRepository = mock( UserRepository.class );
        final RoleRepository roleRepository = mock( RoleRepository.class );
        final UserRepository initialUserRepository = mock( UserRepository.class );
        final UserRepository defaultAdminRepository = mock( UserRepository.class );
        final PasswordPolicy passwordPolicy = new BasicPasswordPolicy();
        AuthenticationStrategy authenticationStrategy = newRateLimitedAuthStrategy();
        InternalFlatFileRealmIT.TestJobScheduler jobScheduler = new InternalFlatFileRealmIT.TestJobScheduler();
        InternalFlatFileRealm realm =
                new InternalFlatFileRealm(
                        userRepository,
                        roleRepository,
                        passwordPolicy,
                        authenticationStrategy,
                        jobScheduler,
                        initialUserRepository,
                        defaultAdminRepository
                    );

        when( userRepository.getPersistedSnapshot() ).thenReturn(
                new ListSnapshot<>( 10L, Collections.emptyList(), usersChanged ) );
        when( userRepository.getUserByName( any() ) ).thenReturn( new User.Builder(  ).build() );
        when( roleRepository.getPersistedSnapshot() ).thenReturn(
                new ListSnapshot<>( 10L, Collections.emptyList(), rolesChanged ) );
        when( roleRepository.getRoleByName( anyString() ) ).thenReturn( new RoleRecord( "" ) );

        realm.init();
        realm.start();

        jobScheduler.scheduledRunnable.run();

        verify( userRepository, times( nSetUsers ) ).setUsers( any() );
        verify( roleRepository, times( nSetRoles ) ).setRoles( any() );
    }

    private static AuthenticationStrategy newRateLimitedAuthStrategy()
    {
        return new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() );
    }

    private class TestRealm extends InternalFlatFileRealm
    {
        private boolean authenticationFlag;
        private boolean authorizationFlag;

        TestRealm( UserRepository userRepository, RoleRepository roleRepository, PasswordPolicy passwordPolicy,
                AuthenticationStrategy authenticationStrategy, JobScheduler jobScheduler,
                UserRepository initialUserRepository, UserRepository defaultAdminRepository )
        {
            super( userRepository, roleRepository, passwordPolicy, authenticationStrategy, jobScheduler,
                    initialUserRepository, defaultAdminRepository );
        }

        boolean takeAuthenticationFlag()
        {
            boolean t = authenticationFlag;
            authenticationFlag = false;
            return t;
        }

        boolean takeAuthorizationFlag()
        {
            boolean t = authorizationFlag;
            authorizationFlag = false;
            return t;
        }

        @Override
        public String getName()
        {
            return "TestRealm wrapping " + super.getName();
        }

        @Override
        public boolean supports( AuthenticationToken token )
        {
            return super.supports( token );
        }

        @Override
        protected AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
        {
            authenticationFlag = true;
            return super.doGetAuthenticationInfo( token );
        }

        @Override
        protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
        {
            authorizationFlag = true;
            return super.doGetAuthorizationInfo( principals );
        }
    }
}
