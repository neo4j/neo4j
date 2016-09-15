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
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.junit.Test;

import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.auth.SecuritySettings;
import org.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.TestCustomCacheableAuthenticationPlugin;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class PluginAuthenticationIT extends EnterpriseAuthenticationTestBase
{
    @Override
    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return super.getSettingsFunction().andThen( pluginOnlyAuthSettings );
    }

    @Test
    public void shouldAuthenticateWithTestAuthenticationPlugin() throws Throwable
    {
        assertConnectionSucceeds( authToken( "neo4j", "neo4j", "plugin-TestAuthenticationPlugin" ) );
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthenticationPlugin() throws Throwable
    {
        Map<String,Object> authToken = authToken( "neo4j", "neo4j",
                "plugin-TestCacheableAuthenticationPlugin" );

        TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.set( 0 );

        restartNeo4jServerWithOverriddenSettings( settings -> {
            settings.put( SecuritySettings.auth_cache_ttl, "60m" );
        });

        // When we log in the first time our plugin should get a call
        assertConnectionSucceeds( authToken );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the second time our plugin should _not_ get a call since authentication info should be cached
        reconnect();
        assertConnectionSucceeds( authToken );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since authentication info should be cached
        reconnect();
        authToken.put( "credentials", "wrong_password" );
        assertConnectionFails( authToken );
        assertThat( TestCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateWithTestCustomCacheableAuthenticationPlugin() throws Throwable
    {
        Map<String,Object> authToken = authToken( "neo4j", "neo4j",
                "plugin-TestCustomCacheableAuthenticationPlugin" );

        TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.set( 0 );

        restartNeo4jServerWithOverriddenSettings( settings -> {
            settings.put( SecuritySettings.auth_cache_ttl, "60m" );
        });

        // When we log in the first time our plugin should get a call
        assertConnectionSucceeds( authToken );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the second time our plugin should _not_ get a call since authentication info should be cached
        reconnect();
        assertConnectionSucceeds( authToken );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since authentication info should be cached
        reconnect();
        authToken.put( "credentials", "wrong_password" );
        assertConnectionFails( authToken );
        assertThat( TestCustomCacheableAuthenticationPlugin.getAuthenticationInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestAuthPlugin() throws Throwable
    {
        assertConnectionSucceeds( authToken( "neo4j", "neo4j", "plugin-TestAuthPlugin" ) );
        assertReadSucceeds();
        assertWriteFails( "neo4j" );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithCacheableTestAuthPlugin() throws Throwable
    {
        assertConnectionSucceeds( authToken( "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) );
        assertReadSucceeds();
        assertWriteFails( "neo4j" );
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthPlugin() throws Throwable
    {
        Map<String,Object> authToken = authToken( "neo4j", "neo4j",
                "plugin-TestCacheableAuthPlugin" );

        TestCacheableAuthPlugin.getAuthInfoCallCount.set( 0 );

        restartNeo4jServerWithOverriddenSettings( settings -> {
            settings.put( SecuritySettings.auth_cache_ttl, "60m" );
        });

        // When we log in the first time our plugin should get a call
        assertConnectionSucceeds( authToken );
        assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
        assertReadSucceeds();
        assertWriteFails( "neo4j" );

        // When we log in the second time our plugin should _not_ get a call since auth info should be cached
        reconnect();
        assertConnectionSucceeds( authToken );
        assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
        assertReadSucceeds();
        assertWriteFails( "neo4j" );

        // When we log in the with the wrong credentials it should fail and
        // our plugin should _not_ get a call since auth info should be cached
        reconnect();
        authToken.put( "credentials", "wrong_password" );
        assertConnectionFails( authToken );
        assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTestCombinedAuthPlugin() throws Throwable
    {
        assertConnectionSucceeds( authToken( "neo4j", "neo4j", "plugin-TestCombinedAuthPlugin" ) );
        assertReadSucceeds();
        assertWriteFails( "neo4j" );
    }
}
