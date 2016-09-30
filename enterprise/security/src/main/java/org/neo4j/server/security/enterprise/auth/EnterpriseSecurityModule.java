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
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.impl.enterprise.SecurityLog;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserManager;
import org.neo4j.server.security.enterprise.auth.plugin.PluginRealm;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;
import org.neo4j.time.Clocks;

import static org.neo4j.kernel.api.proc.Context.AUTH_SUBJECT;

@Service.Implementation( SecurityModule.class )
public class EnterpriseSecurityModule extends SecurityModule
{
    private static final String ROLE_STORE_FILENAME = "roles";

    public EnterpriseSecurityModule()
    {
        super( EnterpriseEditionSettings.ENTERPRISE_SECURITY_MODULE_ID );
    }

    @Override
    public void setup( PlatformModule platformModule, Procedures procedures ) throws KernelException
    {
        Config config = platformModule.config;
        LogProvider logProvider = platformModule.logging.getUserLogProvider();
        JobScheduler jobScheduler = platformModule.jobScheduler;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;

        SecurityLog securityLog = SecurityLog.create(
                config,
                platformModule.logging.getInternalLog( GraphDatabaseFacade.class ),
                fileSystem,
                jobScheduler
            );
        platformModule.life.add( securityLog );

        EnterpriseAuthAndUserManager authManager = newAuthManager( config, logProvider, securityLog, fileSystem, jobScheduler );
        platformModule.life.add( platformModule.dependencies.satisfyDependency( authManager ) );

        procedures.registerComponent( UserManager.class, ctx -> authManager.getUserManager( ctx.get( AUTH_SUBJECT ) ) );
        procedures.registerComponent( SecurityLog.class, (ctx) -> securityLog );
        procedures.registerProcedure( org.neo4j.server.security.auth.AuthProcedures.class );
        procedures.registerProcedure( org.neo4j.server.security.enterprise.auth.AuthProcedures.class, true );
    }

    public EnterpriseAuthAndUserManager newAuthManager( Config config, LogProvider logProvider, SecurityLog securityLog,
            FileSystemAbstraction fileSystem, JobScheduler jobScheduler )
    {
        List<String> configuredRealms = config.get( SecuritySettings.active_realms );
        List<Realm> realms = new ArrayList<>( configuredRealms.size() + 1 );

        SecureHasher secureHasher = new SecureHasher();

        // We always create the internal realm as it is our only UserManager implementation
        InternalFlatFileRealm internalRealm = createInternalRealm( config, logProvider, fileSystem, jobScheduler );

        if ( config.get( SecuritySettings.native_authentication_enabled ) ||
                config.get( SecuritySettings.native_authorization_enabled ) )
        {
            realms.add( internalRealm );
        }

        if ( (config.get( SecuritySettings.ldap_authentication_enabled ) ||
                config.get( SecuritySettings.ldap_authorization_enabled ))
                && configuredRealms.contains( SecuritySettings.LDAP_REALM_NAME ) )
        {
            realms.add( new LdapRealm( config, securityLog ) );
        }

        // Load plugin realms if we have any
        realms.addAll( createPluginRealms( config, logProvider, secureHasher ) );

        // Select the active realms in the order they are configured
        List<Realm> orderedActiveRealms = selectOrderedActiveRealms( configuredRealms, realms );

        if ( orderedActiveRealms.isEmpty() )
        {
            String message = "Illegal configuration: No valid security realm is active.";
            securityLog.error( message );
            throw new IllegalArgumentException( message );
        }

        return new MultiRealmAuthManager( internalRealm, orderedActiveRealms, createCacheManager( config ),
                securityLog, config.get( EnterpriseEditionSettings.security_log_successful_authentication ) );
    }

    private static List<Realm> selectOrderedActiveRealms( List<String> configuredRealms, List<Realm> availableRealms )
    {
        List<Realm> orderedActiveRealms = new ArrayList<>( configuredRealms.size() );
        for ( String configuredRealmName : configuredRealms )
        {
            for ( Realm realm : availableRealms )
            {
                if ( configuredRealmName.equals( realm.getName() ) )
                {
                    orderedActiveRealms.add( realm );
                    break;
                }
            }
        }
        return orderedActiveRealms;
    }

    public static InternalFlatFileRealm createInternalRealm( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem, JobScheduler jobScheduler )
    {
        return new InternalFlatFileRealm(
                CommunitySecurityModule.getUserRepository( config, logProvider, fileSystem ),
                getRoleRepository( config, logProvider, fileSystem ),
                new BasicPasswordPolicy(),
                new RateLimitedAuthenticationStrategy( Clocks.systemClock(), 3 ),
                config.get( SecuritySettings.native_authentication_enabled ),
                config.get( SecuritySettings.native_authorization_enabled ),
                jobScheduler,
                CommunitySecurityModule.getInitialUserRepository( config, logProvider, fileSystem )
            );
    }

    private static CacheManager createCacheManager( Config config )
    {
        long ttl = config.get( SecuritySettings.auth_cache_ttl );
        int maxCapacity = config.get( SecuritySettings.auth_cache_max_capacity );
        return new ShiroCaffeineCache.Manager( Ticker.systemTicker(), ttl, maxCapacity );
    }

    private static List<Realm> createPluginRealms( Config config, LogProvider logProvider, SecureHasher secureHasher )
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
                PluginRealm pluginRealm =
                        new PluginRealm( plugin, config, logProvider, Clocks.systemClock(), secureHasher );
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
                            Clocks.systemClock(), secureHasher );

                    // We need to make sure we do not add a duplicate when the AuthorizationPlugin service gets loaded
                    // so we allow only one instance per combined plugin class
                    excludedClasses.add( plugin.getClass() );
                }
                else
                {
                    pluginRealm =
                            new PluginRealm( plugin, null, config, logProvider, Clocks.systemClock(), secureHasher );
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
                    PluginRealm pluginRealm =
                            new PluginRealm( null, plugin, config, logProvider, Clocks.systemClock(), secureHasher );
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
