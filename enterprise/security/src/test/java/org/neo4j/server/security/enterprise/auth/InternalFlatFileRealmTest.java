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
import java.util.List;

import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthSubject;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.PasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.enterprise.auth.AuthTestUtil.listOf;

public class InternalFlatFileRealmTest
{
    private MultiRealmAuthManager authManager;
    private TestRealm testRealm;
    private AssertableLogProvider log = null;

    @Before
    public void setup() throws Throwable
    {
        log = new AssertableLogProvider();
        Log actualLog = log.getLog( this.getClass() );

        testRealm = new TestRealm(
                        new InMemoryUserRepository(),
                        new InMemoryRoleRepository(),
                        new BasicPasswordPolicy(),
                        new RateLimitedAuthenticationStrategy( Clock.systemUTC(), 3 ),
                        mock( JobScheduler.class ),
                        actualLog );

        List<Realm> realms = listOf( testRealm );

        authManager = new MultiRealmAuthManager( testRealm, realms, new MemoryConstrainedCacheManager(), actualLog );
        authManager.init();
        authManager.start();

        authManager.getUserManager().newUser( "mike", "123", false );
    }

    @Test
    public void shouldNotCacheAuthenticationInfo() throws InvalidAuthTokenException
    {
        // Given
        authManager.login( authToken( "mike", "123" ) );
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );

        // When
        authManager.login( authToken( "mike", "123" ) );

        // Then
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );
    }

    @Test
    public void shouldNotCacheAuthorizationInfo() throws InvalidAuthTokenException
    {
        // Given
        EnterpriseAuthSubject mike = authManager.login( authToken( "mike", "123" ) );
        mike.allowsReads();
        assertThat( "Test realm did not receive a call", testRealm.takeAuthorizationFlag(), is( true ) );

        // When
        mike.allowsWrites();

        // Then
        assertThat( "Test realm did not receive a call", testRealm.takeAuthorizationFlag(), is( true ) );
    }

    private class TestRealm extends InternalFlatFileRealm
    {
        private boolean authenticationFlag = false;
        private boolean authorizationFlag = false;

        public TestRealm( UserRepository userRepository, RoleRepository roleRepository, PasswordPolicy passwordPolicy,
                AuthenticationStrategy authenticationStrategy, JobScheduler jobScheduler, Log securityLog )
        {
            super( userRepository, roleRepository, passwordPolicy, authenticationStrategy, true, true,
                    jobScheduler, securityLog );
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

    @Test
    public void shouldLogCreatingUser() throws Throwable
    {
        testRealm.newUser( "andres", "el password", true );
        testRealm.newUser( "mats", "el password", false );

        log.assertExactly(
                info( "User created: `%s` (password change required)", "andres" ),
                info( "User created: `%s`", "mats" ) );
    }

    @Test
    public void shouldLogCreatingUserWithBadPassword() throws Throwable
    {
        try { testRealm.newUser( "andres", "", true ); } catch (InvalidArgumentsException e) {/*ignore*/}
        try { testRealm.newUser( "mats", null, true ); } catch (InvalidArgumentsException e) {/*ignore*/}

        log.assertExactly(
                error( "User creation failed for user `%s`: %s", "andres", "A password cannot be empty." ),
                error( "User creation failed for user `%s`: %s", "mats", "A password cannot be empty." ) );
    }

    @Test
    public void shouldLogDeletingUser() throws Throwable
    {
        testRealm.newUser( "andres", "el password", false );
        testRealm.deleteUser( "andres" );

        log.assertExactly(
                info( "User created: `%s`", "andres" ),
                info( "User deleted: `%s`", "andres" ) );
    }

    @Test
    public void shouldLogDeletingNonExistentUser() throws Throwable
    {
        try { testRealm.deleteUser( "andres" ); } catch ( InvalidArgumentsException e ) { /*ignore*/}

        log.assertExactly( error( "User deletion failed for user `%s`: %s", "andres", "User 'andres' does not exist." ) );
    }

    private AssertableLogProvider.LogMatcher info( String message, String... arguments )
    {
        return inLog( this.getClass() ).info( message, arguments );
    }

    private AssertableLogProvider.LogMatcher error( String message, String... arguments )
    {
        return inLog( this.getClass() ).error( message, arguments );
    }
}
