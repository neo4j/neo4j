/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.TestCacheableAuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.TestCustomCacheableAuthenticationPlugin;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
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
    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return super.getSettingsFunction()
                .andThen( settings -> settings.put( SecuritySettings.auth_providers, DEFAULT_TEST_PLUGIN_REALMS ) );
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

        restartNeo4jServerWithOverriddenSettings( settings -> settings.put( SecuritySettings.auth_cache_ttl, "60m" ) );

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

        restartNeo4jServerWithOverriddenSettings( settings -> settings.put( SecuritySettings.auth_cache_ttl, "60m" ) );

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
        assertWriteFails( "neo4j", "reader" );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithCacheableTestAuthPlugin() throws Throwable
    {
        assertConnectionSucceeds( authToken( "neo4j", "neo4j", "plugin-TestCacheableAuthPlugin" ) );
        assertReadSucceeds();
        assertWriteFails( "neo4j", "reader" );
    }

    @Test
    public void shouldAuthenticateWithTestCacheableAuthPlugin() throws Throwable
    {
        Map<String,Object> authToken = authToken( "neo4j", "neo4j",
                "plugin-TestCacheableAuthPlugin" );

        TestCacheableAuthPlugin.getAuthInfoCallCount.set( 0 );

        restartNeo4jServerWithOverriddenSettings( settings -> settings.put( SecuritySettings.auth_cache_ttl, "60m" ) );

        // When we log in the first time our plugin should get a call
        assertConnectionSucceeds( authToken );
        assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
        assertReadSucceeds();
        assertWriteFails( "neo4j", "reader" );

        // When we log in the second time our plugin should _not_ get a call since auth info should be cached
        reconnect();
        assertConnectionSucceeds( authToken );
        assertThat( TestCacheableAuthPlugin.getAuthInfoCallCount.get(), equalTo( 1 ) );
        assertReadSucceeds();
        assertWriteFails( "neo4j", "reader" );

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
        restartNeo4jServerWithOverriddenSettings(
                settings -> settings.put( SecuritySettings.auth_providers, "plugin-TestCombinedAuthPlugin" ) );

        assertConnectionSucceeds( authToken( "neo4j", "neo4j", "plugin-TestCombinedAuthPlugin" ) );
        assertReadSucceeds();
        assertWriteFails( "neo4j", "reader" );
    }

    @Test
    public void shouldAuthenticateAndAuthorizeWithTwoSeparateTestPlugins() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings -> settings.put( SecuritySettings.auth_providers,
                "plugin-TestAuthenticationPlugin,plugin-TestAuthorizationPlugin" ) );

        assertConnectionSucceeds( authToken( "neo4j", "neo4j", null ) );
        assertReadSucceeds();
        assertWriteFails( "neo4j", "reader" );
    }

    @Test
    public void shouldFailIfAuthorizationExpiredWithAuthPlugin() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings(
                settings -> settings.put( SecuritySettings.auth_providers, "plugin-TestCacheableAdminAuthPlugin" ) );

        assertConnectionSucceeds( authToken( "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" ) );
        assertReadSucceeds();

        // When
        client.send( util.chunk(
                run( "CALL dbms.security.clearAuthCache()" ), pullAll() ) );
        assertThat( client, util.eventuallyReceives( msgSuccess(), msgSuccess() ) );

        // Then
        client.send( util.chunk(
                run( "MATCH (n) RETURN n" ), pullAll() ) );
        assertThat( client, util.eventuallyReceives(
                msgFailure( Status.Security.AuthorizationExpired,
                        "Plugin 'plugin-TestCacheableAdminAuthPlugin' authorization info expired." ) ) );
    }

    @Test
    public void shouldSucceedIfAuthorizationExpiredWithinTransactionWithAuthPlugin() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings(
                settings -> settings.put( SecuritySettings.auth_providers, "plugin-TestCacheableAdminAuthPlugin" ) );

        // Then
        assertConnectionSucceeds( authToken( "neo4j", "neo4j", "plugin-TestCacheableAdminAuthPlugin" ) );

        client.send( util.chunk(
                run( "CALL dbms.security.clearAuthCache() MATCH (n) RETURN n" ), pullAll() ) );

        // Then
        assertThat( client, util.eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    @Test
    public void shouldAuthenticateWithTestCustomParametersAuthenticationPlugin() throws Throwable
    {
        assertConnectionSucceeds( map(
                "scheme", "custom",
                "principal", "neo4j",
                "realm", "plugin-TestCustomParametersAuthenticationPlugin",
                "parameters", map( "my_credentials", Arrays.asList( 1L, 2L, 3L, 4L ) ) ) );
    }

    @Test
    public void shouldPassOnAuthorizationExpiredException() throws Throwable
    {
        restartNeo4jServerWithOverriddenSettings( settings -> settings.put( SecuritySettings.auth_providers,
                "plugin-TestCombinedAuthPlugin" ) );

        assertConnectionSucceeds( authToken( "authorization_expired_user", "neo4j", null ) );

        // Then
        client.send( util.chunk(
                run( "MATCH (n) RETURN n" ), pullAll() ) );
        assertThat( client, util.eventuallyReceives(
                msgFailure( Status.Security.AuthorizationExpired,
                        "Plugin 'plugin-TestCombinedAuthPlugin' authorization info expired: " +
                        "authorization_expired_user needs to re-authenticate." ) ) );
    }
}
