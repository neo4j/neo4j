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

import org.apache.shiro.realm.Realm;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.PasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.time.Clocks;

import static org.neo4j.server.security.auth.BasicAuthManagerFactory.getUserRepository;

/**
 * Wraps EnterpriseAuthManager and exposes it as a Service
 */
@Service.Implementation( AuthManager.Factory.class )
public class EnterpriseAuthManagerFactory extends AuthManager.Factory
{
    private static final String ROLE_STORE_FILENAME = "roles";

    public EnterpriseAuthManagerFactory()
    {
        super( "enterprise-auth-manager" );
    }

    @Override
    public AuthManager newInstance( Config config, LogProvider logProvider )
    {
        List<Realm> realms = new ArrayList<>( 2 );

        // We always create the internal realm as it is our only UserManager implementation
        InternalFlatFileRealm internalRealm = createInternalRealm( config, logProvider );

        if ( config.get( SecuritySettings.internal_authentication_enabled ) ||
             config.get( SecuritySettings.internal_authorization_enabled ) )
        {
            realms.add( internalRealm );
        }

        if ( config.get( SecuritySettings.ldap_authentication_enabled ) ||
             config.get( SecuritySettings.ldap_authorization_enabled ) )
        {
            realms.add( new LdapRealm( config, logProvider ) );
        }

        if ( config.get( SecuritySettings.plugin_authentication_enabled ) ||
             config.get( SecuritySettings.plugin_authorization_enabled ) )
        {
            // TODO: Load pluggable realms
        }

        return new MultiRealmAuthManager( internalRealm, realms );
    }

    private static InternalFlatFileRealm createInternalRealm( Config config, LogProvider logProvider )
    {
        // Resolve auth store and roles file names
        File authStoreDir = config.get( DatabaseManagementSystemSettings.auth_store_directory );
        File roleStoreFile = new File( authStoreDir, ROLE_STORE_FILENAME );
        final UserRepository userRepository = getUserRepository( config, logProvider );

        final RoleRepository roleRepository =
                new FileRoleRepository( roleStoreFile.toPath(), logProvider );

        final PasswordPolicy passwordPolicy = new BasicPasswordPolicy();

        AuthenticationStrategy authenticationStrategy = new RateLimitedAuthenticationStrategy( Clocks.systemClock(), 3 );

        return new InternalFlatFileRealm( userRepository, roleRepository, passwordPolicy, authenticationStrategy,
                config.get( SecuritySettings.internal_authentication_enabled ),
                config.get( SecuritySettings.internal_authorization_enabled ) );
    }
}
