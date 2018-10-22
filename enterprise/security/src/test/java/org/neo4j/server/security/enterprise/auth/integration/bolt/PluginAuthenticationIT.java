/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.TestCustomCacheableAuthenticationPlugin;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.helpers.collection.MapUtil.map;

public class PluginAuthenticationIT extends EnterpriseAuthenticationTestBase
{
    private static final List<String> defaultTestPluginRealmList = Arrays.asList(
            "TestAuthenticationPlugin",
            "TestAuthPlugin",
            "TestCacheableAdminAuthPlugin",
            "TestCacheableAuthenticationPlugin",
            "TestCacheableAuthPlugin",
            "TestCustomCacheableAuthenticationPlugin",
            "TestCustomParametersAuthenticationPlugin"
    );

    private static final String DEFAULT_TEST_PLUGIN_REALMS = String.join( ", ",
            defaultTestPluginRealmList.stream()
                    .map( s -> StringUtils.prependIfMissing( s, SecuritySettings.PLUGIN_REALM_NAME_PREFIX ) )
                    .collect( Collectors.toList() )
    );

    @Override
    protected Map<Setting<?>,String> getSettings()
    {
        return Collections.singletonMap( SecuritySettings.auth_providers, DEFAULT_TEST_PLUGIN_REALMS );
    }

    @Test
    public void shouldAuthenticateWithTestAuthenticationPlugin()
    {
        assertAuth( "neo4j", "neo4j", "plugin-TestAuthenticationPlugin" );
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthenticationPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_cache_ttl.name(), "60m" );

        TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.set( 0 );

        // When we log in the first time our plugin should get a call
        assertAuth( "neo4j", "neo4j", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the second time our plugin should _not_ get a call since authentication info should be cached
        assertAuth( "neo4j", "neo4j", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since authentication info should be cached
        assertAuthFail( "neo4j", "wrong_password", "plugin-TestCacheableAuthenticationPlugin" );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateWithTestCustomCacheableAuthenticationPlugin() throws Throwable
    {
        TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.set( 0 );

        restartServerWithOverriddenSettings( SecuritySettings.auth_cache_ttl.name(), "60m" );

        // When we log in the first time our plugin should get a call
        assertAuth( "neo4j", "neo4j", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the second time our plugin should _not_ get a call since authentication info should be cached
        assertAuth( "neo4j", "neo4j", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since authentication info should be cached
        assertAuthFail( "neo4j", "wrong_password", "plugin-TestCustomCacheableAuthenticationPlugin" );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestAuthPlugin()
    {
        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithCacheableTestAuthPlugin()
    {
        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthPlugin() throws Throwable
    {
        TestCacheableAuthPlugin.getAuthInfoCallCount.set( 0 );

        restartServerWithOverriddenSettings( SecuritySettings.auth_cache_ttl.name(), "60m" );

        // When we log in the first time our plugin should get a call
        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }

        // When we log in the second time our plugin should _not_ get a call since auth info should be cached
        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) )
        {
            assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since auth info should be cached
        assertAuthFail( "neo4j", "wrong_password", "plugin-TestCacheableAuthPlugin" );
        assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestCombinedAuthPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_providers.name(), "plugin-TestCombinedAuthPlugin" );

        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCombinedAuthPlugin" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTwoSeparateTestPlugins() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_providers.name(), "plugin-TestAuthenticationPlugin,plugin-TestAuthorizationPlugin" );

        try ( Driver driver = connectDriver( "neo4j", "neo4j" ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldFailIfAuthorizationExpiredWithAuthPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_providers.name(), "plugin-TestCacheableAdminAuthPlugin" );

        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" ) )
        {
            assertReadSucceeds( driver );

            // When
            clearAuthCacheFromDifferentConnection( "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" );

            // Then
            assertAuthorizationExpired( driver );
        }
    }

    @Test
    public void shouldSucceedIfAuthorizationExpiredWithinTransactionWithAuthPlugin() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_providers.name(), "plugin-TestCacheableAdminAuthPlugin" );

        // Then
        try ( Driver driver = connectDriver( "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" );
                Session session = driver.session() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CALL dbms.security.clearAuthCache()" );
                assertThat( tx.run( "MATCH (n) RETURN count(n)" ).single().get( 0 ).asInt(), greaterThanOrEqualTo( 0 ) );
                tx.success();
            }
        }
    }

    @Test
    public void shouldAuthenticateWithTestCustomParametersAuthenticationPlugin() throws Throwable
    {
        AuthToken token = AuthTokens.custom( "neo4j", "", "plugin-TestCustomParametersAuthenticationPlugin", "custom",
                map( "my_credentials", Arrays.asList( 1L, 2L, 3L, 4L ) ) );
        assertAuth( token );
    }

    @Test
    public void shouldPassOnAuthorizationExpiredException() throws Throwable
    {
        restartServerWithOverriddenSettings( SecuritySettings.auth_providers.name(), "plugin-TestCombinedAuthPlugin" );

        try ( Driver driver = connectDriver( "authorization_expired_user", "neo4j" ) )
        {
            assertAuthorizationExpired( driver );
        }
    }
}
