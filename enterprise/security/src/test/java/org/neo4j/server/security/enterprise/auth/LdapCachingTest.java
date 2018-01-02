/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import com.google.common.testing.FakeTicker;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.enterprise.auth.AuthTestUtil.listOf;

public class LdapCachingTest
{
    private MultiRealmAuthManager authManager;
    private TestRealm testRealm;
    private FakeTicker fakeTicker;

    @Before
    public void setup() throws Throwable
    {
        SecurityLog securityLog = mock( SecurityLog.class );
        InternalFlatFileRealm internalFlatFileRealm =
            new InternalFlatFileRealm(
                new InMemoryUserRepository(),
                new InMemoryRoleRepository(),
                new BasicPasswordPolicy(),
                new RateLimitedAuthenticationStrategy( Clock.systemUTC(), 3 ),
                mock( JobScheduler.class ),
                new InMemoryUserRepository(),
                new InMemoryUserRepository()
            );

        testRealm = new TestRealm( getLdapConfig(), securityLog, new SecureHasher() );

        List<Realm> realms = listOf( internalFlatFileRealm, testRealm );

        fakeTicker = new FakeTicker();
        authManager = new MultiRealmAuthManager( internalFlatFileRealm, realms,
                new ShiroCaffeineCache.Manager( fakeTicker::read, 100, 10, true ), securityLog, false );
        authManager.init();
        authManager.start();

        authManager.getUserManager().newUser( "mike", "123", false );
        authManager.getUserManager().newUser( "mats", "456", false );
    }

    private Config getLdapConfig()
    {
        return Config.defaults( stringMap(
                SecuritySettings.native_authentication_enabled.name(), "false",
                SecuritySettings.native_authorization_enabled.name(), "false",
                SecuritySettings.ldap_authentication_enabled.name(), "true",
                SecuritySettings.ldap_authorization_enabled.name(), "true",
                SecuritySettings.ldap_authorization_user_search_base.name(), "dc=example,dc=com",
                SecuritySettings.ldap_authorization_group_membership_attribute_names.name(), "gidnumber"
            ) );
    }

    @Test
    public void shouldCacheAuthenticationInfo() throws InvalidAuthTokenException
    {
        // Given
        authManager.login( authToken( "mike", "123" ) );
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );

        // When
        authManager.login( authToken( "mike", "123" ) );

        // Then
        assertThat( "Test realm received a call", testRealm.takeAuthenticationFlag(), is( false ) );
    }

    @Test
    public void shouldCacheAuthorizationInfo() throws InvalidAuthTokenException
    {
        // Given
        EnterpriseSecurityContext mike = authManager.login( authToken( "mike", "123" ) );
        mike.mode().allowsReads();
        assertThat( "Test realm did not receive a call", testRealm.takeAuthorizationFlag(), is( true ) );

        // When
        mike.mode().allowsWrites();

        // Then
        assertThat( "Test realm received a call", testRealm.takeAuthorizationFlag(), is( false ) );
    }

    @Test
    public void shouldInvalidateAuthorizationCacheAfterTTL() throws InvalidAuthTokenException
    {
        // Given
        EnterpriseSecurityContext mike = authManager.login( authToken( "mike", "123" ) );
        mike.mode().allowsReads();
        assertThat( "Test realm did not receive a call", testRealm.takeAuthorizationFlag(), is( true ) );

        // When
        fakeTicker.advance( 99, TimeUnit.MILLISECONDS );
        mike.mode().allowsWrites();

        // Then
        assertThat( "Test realm received a call", testRealm.takeAuthorizationFlag(), is( false ) );

        // When
        fakeTicker.advance( 2, TimeUnit.MILLISECONDS );
        mike.mode().allowsWrites();

        // Then
        assertThat( "Test realm did not received a call", testRealm.takeAuthorizationFlag(), is( true ) );
    }

    @Test
    public void shouldInvalidateAuthenticationCacheAfterTTL() throws InvalidAuthTokenException
    {
        // Given
        Map<String,Object> mike = authToken( "mike", "123" );
        authManager.login( mike );
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );

        // When
        fakeTicker.advance( 99, TimeUnit.MILLISECONDS );
        authManager.login( mike );

        // Then
        assertThat( "Test realm received a call", testRealm.takeAuthenticationFlag(), is( false ) );

        // When
        fakeTicker.advance( 2, TimeUnit.MILLISECONDS );
        authManager.login( mike );

        // Then
        assertThat( "Test realm did not received a call", testRealm.takeAuthenticationFlag(), is( true ) );
    }

    @Test
    public void shouldInvalidateAuthenticationCacheOnDemand() throws InvalidAuthTokenException
    {
        // Given
        Map<String,Object> mike = authToken( "mike", "123" );
        authManager.login( mike );
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );

        // When
        fakeTicker.advance( 2, TimeUnit.MILLISECONDS );
        authManager.login( mike );

        // Then
        assertThat( "Test realm received a call", testRealm.takeAuthenticationFlag(), is( false ) );

        // When
        authManager.clearAuthCache();
        authManager.login( mike );

        // Then
        assertThat( "Test realm did not receive a call", testRealm.takeAuthenticationFlag(), is( true ) );
    }

    private class TestRealm extends LdapRealm
    {
        private boolean authenticationFlag;
        private boolean authorizationFlag;

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

        TestRealm( Config config, SecurityLog securityLog, SecureHasher secureHasher )
        {
            super( config, securityLog, secureHasher );
            setAuthenticationCachingEnabled( true );
            setAuthorizationCachingEnabled( true );
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
            return new AuthenticationInfo()
            {
                @Override
                public PrincipalCollection getPrincipals()
                {
                    return new SimplePrincipalCollection();
                }

                @Override
                public Object getCredentials()
                {
                    return "123";
                }
            };
        }

        @Override
        protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
        {
            authorizationFlag = true;
            return new AuthorizationInfo()
            {
                @Override
                public Collection<String> getRoles()
                {
                    return Collections.emptyList();
                }

                @Override
                public Collection<String> getStringPermissions()
                {
                    return Collections.emptyList();
                }

                @Override
                public Collection<Permission> getObjectPermissions()
                {
                    return Collections.emptyList();
                }
            };
        }
    }

}
