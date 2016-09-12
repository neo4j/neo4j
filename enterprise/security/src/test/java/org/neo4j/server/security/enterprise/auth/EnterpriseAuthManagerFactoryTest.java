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

import org.junit.Ignore;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EnterpriseAuthManagerFactoryTest
{
    // Since this depends on the order of static class initialization it doensn't work on full test runs
    @Ignore
    public void shouldGetShiroDebugLogs()
    {
        // Given
        Config config = mock( Config.class );
        LogProvider mockLogProvider = mock( LogProvider.class );
        Log mockLog = mock( Log.class );
        when( mockLogProvider.getLog( anyString() ) ).thenReturn( mockLog );
        when( mockLog.isDebugEnabled() ).thenReturn( true );
        when( config.get( SecuritySettings.internal_authentication_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.internal_authorization_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.ldap_authentication_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.ldap_authorization_enabled ) ).thenReturn( true );
        when( config.get( SecuritySettings.plugin_authentication_enabled ) ).thenReturn( false );
        when( config.get( SecuritySettings.plugin_authorization_enabled ) ).thenReturn( false );

        // NOTE: This test assumes Shiro JndiLdapRealm will at least output a debug log with the user dn template and
        //       is brittle toward future Shiro version updates
        when( config.get( SecuritySettings.ldap_user_dn_template ) ).thenReturn( "prefix{0}" );

        // When
        new EnterpriseAuthManagerFactory().newInstance( config, mockLogProvider, null, null );

        // Then
        verify( mockLog, atLeastOnce() ).debug( anyString(), contains( "prefix" ), anyString() );
    }
}
