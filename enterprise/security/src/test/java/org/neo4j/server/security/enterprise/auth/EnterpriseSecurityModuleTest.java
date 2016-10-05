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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.log.SecurityLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EnterpriseSecurityModuleTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldFailOnIllegalRealmNameConfiguration()
    {
        // Given
        Config config = mock( Config.class );
        LogProvider mockLogProvider = mock( LogProvider.class );
        Log mockLog = mock( Log.class );
        when( mockLogProvider.getLog( anyString() ) ).thenReturn( mockLog );
        when( mockLog.isDebugEnabled() ).thenReturn( true );
        when( config.get( SecuritySettings.native_authentication_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.native_authorization_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.ldap_authentication_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.ldap_authorization_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.plugin_authentication_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.plugin_authorization_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.active_realms ) ).thenReturn( Arrays.asList( "this-realm-does-not-exist" ) );
        thrown.expect( IllegalArgumentException.class );

        // When
        new EnterpriseSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class), null, null );

        // Then
        verify( mockLog, atLeastOnce() ).debug( anyString(),
                contains( "Illegal configuration: No valid security realm is active." ), anyString() );
    }

    @Test
    public void shouldFailOnIllegalAdvancedRealmConfiguration()
    {
        // Given
        Config config = mock( Config.class );
        LogProvider mockLogProvider = mock( LogProvider.class );
        Log mockLog = mock( Log.class );
        when( mockLogProvider.getLog( anyString() ) ).thenReturn( mockLog );
        when( mockLog.isDebugEnabled() ).thenReturn( true );
        when( config.get( SecuritySettings.native_authentication_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.native_authorization_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_authentication_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_authorization_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.plugin_authentication_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.plugin_authorization_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.active_realms ) ).thenReturn(
                Arrays.asList(
                        SecuritySettings.NATIVE_REALM_NAME,
                        SecuritySettings.LDAP_REALM_NAME )
        );
        thrown.expect( IllegalArgumentException.class );

        // When
        new EnterpriseSecurityModule().newAuthManager( config, mockLogProvider, mock( SecurityLog.class), null, null );

        // Then
        verify( mockLog, atLeastOnce() ).debug( anyString(),
                contains( "Illegal configuration: No valid security realm is active." ), anyString() );
    }
}
