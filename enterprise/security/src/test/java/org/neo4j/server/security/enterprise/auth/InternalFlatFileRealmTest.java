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

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.PrincipalCollection;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.Collections;
import java.util.List;

import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.server.security.enterprise.log.SecurityLog;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.PasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.time.Clocks;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.enterprise.auth.AuthTestUtil.listOf;

public class InternalFlatFileRealmTest
{
    private MultiRealmAuthManager authManager;
    private TestRealm testRealm;

    @Before
    public void setup() throws Throwable
    {
        testRealm = new TestRealm(
                        new InMemoryUserRepository(),
                        new InMemoryRoleRepository(),
                        new BasicPasswordPolicy(),
                        new RateLimitedAuthenticationStrategy( Clock.systemUTC(), 3 ),
                        mock( JobScheduler.class ),
                        new InMemoryUserRepository(),
                        new InMemoryUserRepository()
                    );

        List<Realm> realms = listOf( testRealm );

        authManager = new MultiRealmAuthManager( testRealm, realms, new MemoryConstrainedCacheManager(),
                mock( SecurityLog.class ), true );

        authManager.init();
        authManager.start();

        authManager.getUserManager().newUser( "mike", "123", false );
    }

    @Test
    public void shouldNotCacheAuthenticationInfo() throws InvalidAuthTokenException
    {
        // Given
        EnterpriseSecurityContext mike = authManager.login( authToken( "mike", "123" ) );
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
        EnterpriseSecurityContext mike = authManager.login( authToken( "mike", "123" ) );
        assertThat( mike.subject().getAuthenticationResult(), equalTo( AuthenticationResult.SUCCESS ) );

        mike.mode().allowsReads();
        assertThat( "Test realm did not receive a call", testRealm.takeAuthorizationFlag(), is( true ) );

        // When
        mike.mode().allowsWrites();

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

    private void assertSetUsersAndRolesNTimes( boolean usersChanged, boolean rolesChanged,
            int nSetUsers, int nSetRoles ) throws Throwable
    {
        final UserRepository userRepository = mock( UserRepository.class );
        final RoleRepository roleRepository = mock( RoleRepository.class );
        final UserRepository initialUserRepository = mock( UserRepository.class );
        final UserRepository defaultAdminRepository = mock( UserRepository.class );
        final PasswordPolicy passwordPolicy = new BasicPasswordPolicy();
        AuthenticationStrategy authenticationStrategy = new RateLimitedAuthenticationStrategy( Clocks.systemClock(), 3 );
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

    private class TestRealm extends InternalFlatFileRealm
    {
        private boolean authenticationFlag = false;
        private boolean authorizationFlag = false;

        public TestRealm( UserRepository userRepository, RoleRepository roleRepository, PasswordPolicy passwordPolicy,
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
            return "TestRealm wrapping "+ super.getName();
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
