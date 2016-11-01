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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EnterpriseSecurityModuleTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private Config config;
    private LogProvider mockLogProvider;

    @Test
    public void shouldFailOnIllegalRealmNameConfiguration()
    {
        // Given
        nativeAuth( true, true );
        ldapAuth( true, true );
        pluginAuth( false, false );
        authProviders( "this-realm-does-not-exist" );

        // Then
        thrown.expect( IllegalArgumentException.class );
        thrown.expectMessage( "Illegal configuration: No valid auth provider is active." );

        // When
        new EnterpriseSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class), null, null );
    }

    @Test
    public void shouldFailOnNoAuthenticationMechanism()
    {
        // Given
        nativeAuth( false, true );
        ldapAuth( false, false );
        pluginAuth( false, false );
        authProviders( SecuritySettings.NATIVE_REALM_NAME );

        // Then
        thrown.expect( IllegalArgumentException.class );
        thrown.expectMessage( "Illegal configuration: All authentication providers are disabled." );

        // When
        new EnterpriseSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class), null, null );
    }

    @Test
    public void shouldFailOnNoAuthorizationMechanism()
    {
        // Given
        nativeAuth( true, false );
        ldapAuth( false, false );
        pluginAuth( false, false );
        authProviders( SecuritySettings.NATIVE_REALM_NAME );

        // Then
        thrown.expect( IllegalArgumentException.class );
        thrown.expectMessage( "Illegal configuration: All authorization providers are disabled." );

        // When
        new EnterpriseSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class), null, null );
    }

    @Test
    public void shouldFailOnIllegalAdvancedRealmConfiguration()
    {
        // Given
        nativeAuth( false, false );
        ldapAuth( false, false );
        pluginAuth( true, true );
        authProviders( SecuritySettings.NATIVE_REALM_NAME, SecuritySettings.LDAP_REALM_NAME );

        // Then
        thrown.expect( IllegalArgumentException.class );
        thrown.expectMessage( "Illegal configuration: Native auth provider configured, " +
                                "but both authentication and authorization are disabled." );

        // When
        new EnterpriseSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class), null, null );
    }

    @Test
    public void shouldFailOnNotLoadedPluginAuthProvider()
    {
        // Given
        nativeAuth( false, false );
        ldapAuth( false, false );
        pluginAuth( true, true );
        authProviders(
                SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "TestAuthenticationPlugin",
                SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "IllConfiguredAuthorizationPlugin"
        );

        // Then
        thrown.expect( IllegalArgumentException.class );
        thrown.expectMessage(
                "Illegal configuration: Failed to load auth plugin 'plugin-IllConfiguredAuthorizationPlugin'." );

        // When
        new EnterpriseSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class), null, null );
    }

    @Test
    public void shouldNotFailNativeWithPluginAuthorizationProvider()
    {
        // Given
        nativeAuth( true, true );
        ldapAuth( false, false );
        pluginAuth( true, true );
        authProviders(
                SecuritySettings.NATIVE_REALM_NAME,
                SecuritySettings.PLUGIN_REALM_NAME_PREFIX + "TestAuthorizationPlugin"
        );

        // When
        new EnterpriseSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class), null, null );
    }

    // --------- HELPERS ----------

    @Before
    public void setup()
    {
        config = mock( Config.class );
        mockLogProvider = mock( LogProvider.class );
        Log mockLog = mock( Log.class );
        when( mockLogProvider.getLog( anyString() ) ).thenReturn( mockLog );
        when( mockLog.isDebugEnabled() ).thenReturn( true );
        when( config.get( SecuritySettings.auth_cache_ttl ) ).thenReturn( 0L );
        when( config.get( SecuritySettings.auth_cache_max_capacity ) ).thenReturn( 10 );
        when( config.get( SecuritySettings.security_log_successful_authentication ) ).thenReturn( false );
    }

    private void nativeAuth( boolean authn, boolean authr )
    {
        when( config.get( SecuritySettings.native_authentication_enabled ) ).thenReturn( authn );
        when( config.get( SecuritySettings.native_authorization_enabled ) ).thenReturn( authr );
    }

    private void ldapAuth( boolean authn, boolean authr )
    {
        when( config.get( SecuritySettings.ldap_authentication_enabled ) ).thenReturn( authn );
        when( config.get( SecuritySettings.ldap_authorization_enabled ) ).thenReturn( authr );
    }

    private void pluginAuth( boolean authn, boolean authr )
    {
        when( config.get( SecuritySettings.plugin_authentication_enabled ) ).thenReturn( authn );
        when( config.get( SecuritySettings.plugin_authorization_enabled ) ).thenReturn( authr );
    }

    private void authProviders( String... authProviders )
    {
        when( config.get( SecuritySettings.auth_providers ) ).thenReturn( Arrays.asList( authProviders ) );
    }
}
