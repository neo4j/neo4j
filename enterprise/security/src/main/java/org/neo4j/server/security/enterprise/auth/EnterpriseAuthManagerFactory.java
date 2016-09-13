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

import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.realm.Realm;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.SecurityLog;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.enterprise.auth.plugin.PluginRealm;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;
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
    public EnterpriseAuthManager newInstance( Config config, LogProvider logProvider, Log allegedSecurityLog,
            FileSystemAbstraction fileSystem, JobScheduler jobScheduler )
    {
//        StaticLoggerBinder.setNeo4jLogProvider( logProvider );

        List<Realm> realms = new ArrayList<>( 2 );
        SecurityLog securityLog = getSecurityLog( allegedSecurityLog );

        // We always create the internal realm as it is our only UserManager implementation
        InternalFlatFileRealm internalRealm = createInternalRealm( config, logProvider, fileSystem, jobScheduler );

        if ( config.get( SecuritySettings.internal_authentication_enabled ) ||
             config.get( SecuritySettings.internal_authorization_enabled ) )
        {
            realms.add( internalRealm );
        }

        if ( config.get( SecuritySettings.ldap_authentication_enabled ) ||
             config.get( SecuritySettings.ldap_authorization_enabled ) )
        {
            realms.add( new LdapRealm( config, securityLog ) );
        }

        // Load plugin realms if we have any
        realms.addAll( createPluginRealms( config, logProvider ) );

        return new MultiRealmAuthManager( internalRealm, realms, createCacheManager( config ),
                securityLog, config.get( EnterpriseEditionSettings.security_log_successful_authentication ) );
    }

    private static InternalFlatFileRealm createInternalRealm( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem, JobScheduler jobScheduler )
    {
        return new InternalFlatFileRealm(
                getUserRepository( config, logProvider, fileSystem ),
                getRoleRepository( config, logProvider, fileSystem ),
                new BasicPasswordPolicy(), new RateLimitedAuthenticationStrategy( Clocks.systemClock(), 3 ),
                config.get( SecuritySettings.internal_authentication_enabled ),
                config.get( SecuritySettings.internal_authorization_enabled ), jobScheduler );
    }

    private SecurityLog getSecurityLog( Log allegedSecurityLog )
    {
        return allegedSecurityLog instanceof SecurityLog ?
               (SecurityLog) allegedSecurityLog :
               new SecurityLog( allegedSecurityLog );
    }

    private static CacheManager createCacheManager( Config config )
    {
        long ttl = config.get( SecuritySettings.auth_cache_ttl );
        int maxCapacity = config.get( SecuritySettings.auth_cache_max_capacity );
        return new ShiroCaffeineCache.Manager( Ticker.systemTicker(), ttl, maxCapacity );
    }

    private static List<Realm> createPluginRealms( Config config, LogProvider logProvider )
    {
        List<Realm> realms = new ArrayList<>();
        Set<Class> excludedClasses = new HashSet<>();

        Boolean pluginAuthenticationEnabled = config.get( SecuritySettings.plugin_authentication_enabled );
        Boolean pluginAuthorizationEnabled = config.get( SecuritySettings.plugin_authorization_enabled );

        if ( pluginAuthenticationEnabled && pluginAuthorizationEnabled )
        {
            // Combined authentication and authorization plugins
            Iterable<AuthPlugin> authPlugins = Service.load( AuthPlugin.class );

            for ( AuthPlugin plugin : authPlugins )
            {
                PluginRealm pluginRealm = new PluginRealm( plugin, config, logProvider,  Clocks.systemClock() );
                realms.add( pluginRealm );
            }
        }

        if ( pluginAuthenticationEnabled )
        {
            // Authentication only plugins
            Iterable<AuthenticationPlugin> authenticationPlugins = Service.load( AuthenticationPlugin.class );

            for ( AuthenticationPlugin plugin : authenticationPlugins )
            {
                PluginRealm pluginRealm;

                if ( pluginAuthorizationEnabled && plugin instanceof AuthorizationPlugin )
                {
                    // This plugin implements both interfaces, create a combined plugin
                    pluginRealm = new PluginRealm( plugin, (AuthorizationPlugin) plugin, config, logProvider,
                            Clocks.systemClock() );

                    // We need to make sure we do not add a duplicate when the AuthorizationPlugin service gets loaded
                    // so we allow only one instance per combined plugin class
                    excludedClasses.add( plugin.getClass() );
                }
                else
                {
                    pluginRealm = new PluginRealm( plugin, null, config, logProvider, Clocks.systemClock() );
                }
                realms.add( pluginRealm );
            }
        }

        if ( pluginAuthorizationEnabled )
        {
            // Authorization only plugins
            Iterable<AuthorizationPlugin> authorizationPlugins = Service.load( AuthorizationPlugin.class );

            for ( AuthorizationPlugin plugin : authorizationPlugins )
            {
                if ( !excludedClasses.contains( plugin.getClass() ) )
                {
                    PluginRealm pluginRealm = new PluginRealm( null, plugin, config, logProvider, Clocks.systemClock() );
                    realms.add( pluginRealm );
                }
            }
        }

        return realms;
    }

    public static RoleRepository getRoleRepository( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem )
    {
        File authStoreDir = config.get( DatabaseManagementSystemSettings.auth_store_directory );
        File roleStoreFile = new File( authStoreDir, ROLE_STORE_FILENAME );
        return new FileRoleRepository( fileSystem, roleStoreFile, logProvider );
    }
}
