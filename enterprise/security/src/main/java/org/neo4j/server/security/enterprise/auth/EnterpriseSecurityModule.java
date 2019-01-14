/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.security.enterprise.auth;

import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.realm.Realm;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.plugin.PluginRealm;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.log.SecurityLog;
import org.neo4j.time.Clocks;

import static java.lang.String.format;
import static org.neo4j.kernel.api.proc.Context.SECURITY_CONTEXT;

@Service.Implementation( SecurityModule.class )
public class EnterpriseSecurityModule extends SecurityModule
{
    private static final String ROLE_STORE_FILENAME = "roles";
    private static final String DEFAULT_ADMIN_STORE_FILENAME = SetDefaultAdminCommand.ADMIN_INI;

    public EnterpriseSecurityModule()
    {
        super( EnterpriseEditionSettings.ENTERPRISE_SECURITY_MODULE_ID );
    }

    @Override
    public void setup( Dependencies dependencies ) throws KernelException
    {
        Config config = dependencies.config();
        Procedures procedures = dependencies.procedures();
        LogProvider logProvider = dependencies.logService().getUserLogProvider();
        JobScheduler jobScheduler = dependencies.scheduler();
        FileSystemAbstraction fileSystem = dependencies.fileSystem();
        LifeSupport life = dependencies.lifeSupport();

        SecurityLog securityLog = SecurityLog.create(
                config,
                dependencies.logService().getInternalLog( GraphDatabaseFacade.class ),
                fileSystem,
                jobScheduler
            );
        life.add( securityLog );

        EnterpriseAuthAndUserManager authManager = newAuthManager( config, logProvider, securityLog, fileSystem, jobScheduler );
        life.add( dependencies.dependencySatisfier().satisfyDependency( authManager ) );

        // Register procedures
        procedures.registerComponent( SecurityLog.class, ctx -> securityLog, false );
        procedures.registerComponent( EnterpriseAuthManager.class, ctx -> authManager, false );
        procedures.registerComponent( EnterpriseSecurityContext.class,
                ctx -> asEnterprise( ctx.get( SECURITY_CONTEXT ) ), true );

        if ( config.get( SecuritySettings.native_authentication_enabled )
             || config.get( SecuritySettings.native_authorization_enabled ) )
        {
            procedures.registerComponent( EnterpriseUserManager.class,
                    ctx -> authManager.getUserManager( ctx.get( SECURITY_CONTEXT ).subject(), ctx.get( SECURITY_CONTEXT ).isAdmin() ), true );
            if ( config.get( SecuritySettings.auth_providers ).size() > 1 )
            {
                procedures.registerProcedure( UserManagementProcedures.class, true, "%s only applies to native users."  );
            }
            else
            {
                procedures.registerProcedure( UserManagementProcedures.class, true );
            }
        }
        else
        {
            procedures.registerComponent( EnterpriseUserManager.class, ctx -> EnterpriseUserManager.NOOP, true );
        }

        procedures.registerProcedure( SecurityProcedures.class, true );
    }

    private EnterpriseSecurityContext asEnterprise( SecurityContext securityContext )
    {
        if ( securityContext instanceof EnterpriseSecurityContext )
        {
            return (EnterpriseSecurityContext) securityContext;
        }
        // TODO: better handling of this possible cast failure
        throw new RuntimeException( "Expected EnterpriseSecurityContext, got " + securityContext.getClass().getName() );
    }

    public EnterpriseAuthAndUserManager newAuthManager( Config config, LogProvider logProvider, SecurityLog securityLog,
            FileSystemAbstraction fileSystem, JobScheduler jobScheduler )
    {
        SecurityConfig securityConfig = new SecurityConfig( config );
        securityConfig.validate();

        List<Realm> realms = new ArrayList<>( securityConfig.authProviders.size() + 1 );
        SecureHasher secureHasher = new SecureHasher();

        InternalFlatFileRealm internalRealm = null;
        if ( securityConfig.hasNativeProvider )
        {
            internalRealm = createInternalRealm( config, logProvider, fileSystem, jobScheduler );
            realms.add( internalRealm );
        }

        if ( securityConfig.hasLdapProvider )
        {
            realms.add( new LdapRealm( config, securityLog, secureHasher ) );
        }

        if ( !securityConfig.pluginAuthProviders.isEmpty() )
        {
            realms.addAll( createPluginRealms( config, securityLog, secureHasher, securityConfig ) );
        }

        // Select the active realms in the order they are configured
        List<Realm> orderedActiveRealms = selectOrderedActiveRealms( securityConfig.authProviders, realms );

        if ( orderedActiveRealms.isEmpty() )
        {
            throw illegalConfiguration( "No valid auth provider is active." );
        }

        return new MultiRealmAuthManager( internalRealm, orderedActiveRealms, createCacheManager( config ),
                securityLog, config.get( SecuritySettings.security_log_successful_authentication ),
                securityConfig.propertyAuthorization, securityConfig.propertyBlacklist );
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
                createAuthenticationStrategy( config ),
                config.get( SecuritySettings.native_authentication_enabled ),
                config.get( SecuritySettings.native_authorization_enabled ),
                jobScheduler,
                CommunitySecurityModule.getInitialUserRepository( config, logProvider, fileSystem ),
                getDefaultAdminRepository( config, logProvider, fileSystem )
            );
    }

    private static AuthenticationStrategy createAuthenticationStrategy( Config config )
    {
        return new RateLimitedAuthenticationStrategy( Clocks.systemClock(), config );
    }

    private static CacheManager createCacheManager( Config config )
    {
        long ttl = config.get( SecuritySettings.auth_cache_ttl ).toMillis();
        boolean useTTL = config.get( SecuritySettings.auth_cache_use_ttl );
        int maxCapacity = config.get( SecuritySettings.auth_cache_max_capacity );
        return new ShiroCaffeineCache.Manager( Ticker.systemTicker(), ttl, maxCapacity, useTTL );
    }

    private static List<PluginRealm> createPluginRealms(
            Config config, SecurityLog securityLog, SecureHasher secureHasher, SecurityConfig securityConfig )
    {
        List<PluginRealm> availablePluginRealms = new ArrayList<>();
        Set<Class> excludedClasses = new HashSet<>();

        if ( securityConfig.pluginAuthentication && securityConfig.pluginAuthorization )
        {
            for ( AuthPlugin plugin : Service.load( AuthPlugin.class ) )
            {
                PluginRealm pluginRealm =
                        new PluginRealm( plugin, config, securityLog, Clocks.systemClock(), secureHasher );
                availablePluginRealms.add( pluginRealm );
            }
        }

        if ( securityConfig.pluginAuthentication )
        {
            for ( AuthenticationPlugin plugin : Service.load( AuthenticationPlugin.class ) )
            {
                PluginRealm pluginRealm;

                if ( securityConfig.pluginAuthorization && plugin instanceof AuthorizationPlugin )
                {
                    // This plugin implements both interfaces, create a combined plugin
                    pluginRealm = new PluginRealm( plugin, (AuthorizationPlugin) plugin, config, securityLog,
                            Clocks.systemClock(), secureHasher );

                    // We need to make sure we do not add a duplicate when the AuthorizationPlugin service gets loaded
                    // so we allow only one instance per combined plugin class
                    excludedClasses.add( plugin.getClass() );
                }
                else
                {
                    pluginRealm =
                            new PluginRealm( plugin, null, config, securityLog, Clocks.systemClock(), secureHasher );
                }
                availablePluginRealms.add( pluginRealm );
            }
        }

        if ( securityConfig.pluginAuthorization )
        {
            for ( AuthorizationPlugin plugin : Service.load( AuthorizationPlugin.class ) )
            {
                if ( !excludedClasses.contains( plugin.getClass() ) )
                {
                    availablePluginRealms.add(
                            new PluginRealm( null, plugin, config, securityLog, Clocks.systemClock(), secureHasher )
                        );
                }
            }
        }

        for ( String pluginRealmName : securityConfig.pluginAuthProviders )
        {
            if ( availablePluginRealms.stream().noneMatch( r -> r.getName().equals( pluginRealmName ) ) )
            {
                throw illegalConfiguration( format( "Failed to load auth plugin '%s'.", pluginRealmName ) );
            }
        }

        List<PluginRealm> realms =
                availablePluginRealms.stream()
                        .filter( realm -> securityConfig.pluginAuthProviders.contains( realm.getName() ) )
                        .collect( Collectors.toList() );

        boolean missingAuthenticatingRealm =
                securityConfig.onlyPluginAuthentication() && realms.stream().noneMatch( PluginRealm::canAuthenticate );
        boolean missingAuthorizingRealm =
                securityConfig.onlyPluginAuthorization() && realms.stream().noneMatch( PluginRealm::canAuthorize );

        if ( missingAuthenticatingRealm || missingAuthorizingRealm )
        {
            String missingProvider =
                    ( missingAuthenticatingRealm && missingAuthorizingRealm ) ? "authentication or authorization" :
                    missingAuthenticatingRealm ? "authentication" : "authorization";

            throw illegalConfiguration( format(
                    "No plugin %s provider loaded even though required by configuration.", missingProvider ) );
        }

        return realms;
    }

    public static RoleRepository getRoleRepository( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem )
    {
        return new FileRoleRepository( fileSystem, getRoleRepositoryFile( config ), logProvider );
    }

    public static UserRepository getDefaultAdminRepository( Config config, LogProvider logProvider,
            FileSystemAbstraction fileSystem )
    {
        return new FileUserRepository( fileSystem, getDefaultAdminRepositoryFile( config ), logProvider );
    }

    private static File getRoleRepositoryFile( Config config )
    {
        return new File( config.get( DatabaseManagementSystemSettings.auth_store_directory ), ROLE_STORE_FILENAME );
    }

    private static File getDefaultAdminRepositoryFile( Config config )
    {
        return new File( config.get( DatabaseManagementSystemSettings.auth_store_directory ),
                DEFAULT_ADMIN_STORE_FILENAME );
    }

    private static IllegalArgumentException illegalConfiguration( String message )
    {
        return new IllegalArgumentException( "Illegal configuration: " + message );
    }

    static class SecurityConfig
    {
        final List<String> authProviders;
        final boolean hasNativeProvider;
        final boolean hasLdapProvider;
        final List<String> pluginAuthProviders;
        final boolean nativeAuthentication;
        final boolean nativeAuthorization;
        final boolean ldapAuthentication;
        final boolean ldapAuthorization;
        final boolean pluginAuthentication;
        final boolean pluginAuthorization;
        final boolean propertyAuthorization;
        private final String propertyAuthMapping;
        final Map<String,List<String>> propertyBlacklist = new HashMap<>();

        SecurityConfig( Config config )
        {
            authProviders = config.get( SecuritySettings.auth_providers );
            hasNativeProvider = authProviders.contains( SecuritySettings.NATIVE_REALM_NAME );
            hasLdapProvider = authProviders.contains( SecuritySettings.LDAP_REALM_NAME );
            pluginAuthProviders = authProviders.stream()
                    .filter( r -> r.startsWith( SecuritySettings.PLUGIN_REALM_NAME_PREFIX ) )
                    .collect( Collectors.toList() );

            nativeAuthentication = config.get( SecuritySettings.native_authentication_enabled );
            nativeAuthorization = config.get( SecuritySettings.native_authorization_enabled );
            ldapAuthentication = config.get( SecuritySettings.ldap_authentication_enabled );
            ldapAuthorization = config.get( SecuritySettings.ldap_authorization_enabled );
            pluginAuthentication = config.get( SecuritySettings.plugin_authentication_enabled );
            pluginAuthorization = config.get( SecuritySettings.plugin_authorization_enabled );
            propertyAuthorization = config.get( SecuritySettings.property_level_authorization_enabled );
            propertyAuthMapping = config.get( SecuritySettings.property_level_authorization_permissions );
        }

        void validate()
        {
            if ( !nativeAuthentication && !ldapAuthentication && !pluginAuthentication )
            {
                throw illegalConfiguration( "All authentication providers are disabled." );
            }

            if ( !nativeAuthorization && !ldapAuthorization && !pluginAuthorization )
            {
                throw illegalConfiguration( "All authorization providers are disabled." );
            }

            if ( hasNativeProvider && !nativeAuthentication && !nativeAuthorization )
            {
                throw illegalConfiguration(
                        "Native auth provider configured, but both authentication and authorization are disabled." );
            }

            if ( hasLdapProvider && !ldapAuthentication && !ldapAuthorization )
            {
                throw illegalConfiguration(
                        "LDAP auth provider configured, but both authentication and authorization are disabled." );
            }

            if ( !pluginAuthProviders.isEmpty() && !pluginAuthentication && !pluginAuthorization )
            {
                throw illegalConfiguration(
                        "Plugin auth provider configured, but both authentication and authorization are disabled." );
            }
            if ( propertyAuthorization && !parsePropertyPermissions() )
            {
                throw illegalConfiguration(
                        "Property level authorization is enabled but there is a error in the permissions mapping." );
            }
        }

        private boolean parsePropertyPermissions()
        {
            if ( propertyAuthMapping != null && !propertyAuthMapping.isEmpty() )
            {
                String rolePattern = "\\s*[a-zA-Z0-9_]+\\s*";
                String propertyPattern = "\\s*[a-zA-Z0-9_]+\\s*";
                String roleToPerm = rolePattern + "=" + propertyPattern + "(," + propertyPattern + ")*";
                String multiLine = roleToPerm + "(;" + roleToPerm + ")*";

                boolean valid = propertyAuthMapping.matches( multiLine );
                if ( !valid )
                {
                    return false;
                }

                for ( String rolesAndPermissions : propertyAuthMapping.split( ";" ) )
                {
                    if ( !rolesAndPermissions.isEmpty() )
                    {
                        String[] split = rolesAndPermissions.split( "=" );
                        String role = split[0].trim();
                        String permissions = split[1];
                        List<String> permissionsList = new ArrayList<>();
                        for ( String perm : permissions.split( "," ) )
                        {
                            if ( !perm.isEmpty() )
                            {
                                permissionsList.add( perm.trim() );
                            }
                        }
                        propertyBlacklist.put( role, permissionsList );
                    }
                }
            }
            return true;
        }

        public boolean onlyPluginAuthentication()
        {
            return !nativeAuthentication && !ldapAuthentication && pluginAuthentication;
        }

        public boolean onlyPluginAuthorization()
        {
            return !nativeAuthorization && !ldapAuthorization && pluginAuthorization;
        }
    }
}
