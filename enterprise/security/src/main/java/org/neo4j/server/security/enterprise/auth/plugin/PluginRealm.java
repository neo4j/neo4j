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
package org.neo4j.server.security.enterprise.auth.plugin;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.Version;
import org.neo4j.logging.Log;
import org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder;
import org.neo4j.server.security.enterprise.auth.RealmLifecycle;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.ShiroAuthToken;
import org.neo4j.server.security.enterprise.auth.ShiroAuthorizationInfoProvider;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthorizationExpiredException;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CustomCacheableAuthenticationInfo;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.PLUGIN_REALM_NAME_PREFIX;

public class PluginRealm extends AuthorizingRealm implements RealmLifecycle, ShiroAuthorizationInfoProvider
{
    private AuthenticationPlugin authenticationPlugin;
    private AuthorizationPlugin authorizationPlugin;
    private final Config config;
    private AuthPlugin authPlugin;
    private final Log log;
    private final Clock clock;
    private final SecureHasher secureHasher;

    private AuthProviderOperations authProviderOperations = new PluginRealmOperations();

    public PluginRealm( Config config, SecurityLog securityLog, Clock clock, SecureHasher secureHasher )
    {
        this.config = config;
        this.clock = clock;
        this.secureHasher = secureHasher;
        this.log = securityLog;

        setCredentialsMatcher( new CredentialsMatcher() );

        // Synchronize this default value with the javadoc for AuthProviderOperations.setAuthenticationCachingEnabled
        setAuthenticationCachingEnabled( false );

        // Synchronize this default value with the javadoc for AuthProviderOperations.setAuthorizationCachingEnabled
        setAuthorizationCachingEnabled( true );

        setRolePermissionResolver( PredefinedRolesBuilder.rolePermissionResolver );
    }

    public PluginRealm( AuthenticationPlugin authenticationPlugin, AuthorizationPlugin authorizationPlugin,
            Config config, SecurityLog securityLog, Clock clock, SecureHasher secureHasher )
    {
        this( config, securityLog, clock, secureHasher );
        this.authenticationPlugin = authenticationPlugin;
        this.authorizationPlugin = authorizationPlugin;
        resolvePluginName();
    }

    public PluginRealm( AuthPlugin authPlugin, Config config, SecurityLog securityLog, Clock clock,
            SecureHasher secureHasher )
    {
        this( config, securityLog, clock, secureHasher );
        this.authPlugin = authPlugin;
        resolvePluginName();
    }

    private void resolvePluginName()
    {
        String pluginName = null;
        if ( authPlugin != null )
        {
            pluginName = authPlugin.name();
        }
        else if ( authenticationPlugin != null )
        {
            pluginName = authenticationPlugin.name();
        }
        else if ( authorizationPlugin != null )
        {
            pluginName = authorizationPlugin.name();
        }

        if ( pluginName != null && !pluginName.isEmpty() )
        {
            setName( PLUGIN_REALM_NAME_PREFIX + pluginName );
        }
        // Otherwise we rely on the Shiro default generated name
    }

    private Collection<AuthorizationPlugin.PrincipalAndProvider> getPrincipalAndProviderCollection(
            PrincipalCollection principalCollection
    )
    {
        Collection<AuthorizationPlugin.PrincipalAndProvider> principalAndProviderCollection = new ArrayList<>();

        for ( String realm : principalCollection.getRealmNames() )
        {
            for ( Object principal : principalCollection.fromRealm( realm ) )
            {
                principalAndProviderCollection.add( new AuthorizationPlugin.PrincipalAndProvider( principal, realm ) );
            }
        }

        return principalAndProviderCollection;
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        if ( authorizationPlugin != null )
        {
            org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo authorizationInfo;
            try
            {
                 authorizationInfo = authorizationPlugin.authorize( getPrincipalAndProviderCollection( principals ) );
            }
            catch ( AuthorizationExpiredException e )
            {
                throw new org.neo4j.graphdb.security.AuthorizationExpiredException(
                        "Plugin '" + getName() + "' authorization info expired: " + e.getMessage(), e );
            }
            if ( authorizationInfo != null )
            {
                return PluginAuthorizationInfo.create( authorizationInfo );
            }
        }
        else if ( authPlugin != null && !principals.fromRealm( getName() ).isEmpty() )
        {
            // The cached authorization info has expired.
            // Since we do not have the subject's credentials we cannot perform a new
            // authenticateAndAuthorize() to renew authorization info.
            // Instead we need to fail with a special status, so that the client can react by re-authenticating.
            throw new org.neo4j.graphdb.security.AuthorizationExpiredException(
                    "Plugin '" + getName() + "' authorization info expired." );
        }
        return null;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
    {
        if ( token instanceof ShiroAuthToken )
        {
            try
            {
                AuthToken pluginAuthToken =
                        PluginApiAuthToken.createFromMap( ((ShiroAuthToken) token).getAuthTokenMap() );
                if ( authPlugin != null )
                {
                    AuthInfo authInfo = authPlugin.authenticateAndAuthorize( pluginAuthToken );
                    if ( authInfo != null )
                    {
                        PluginAuthInfo pluginAuthInfo =
                                PluginAuthInfo.createCacheable( authInfo, getName(), secureHasher );

                        cacheAuthorizationInfo( pluginAuthInfo );

                        return pluginAuthInfo;
                    }
                }
                else if ( authenticationPlugin != null )
                {
                    org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo authenticationInfo =
                            authenticationPlugin.authenticate( pluginAuthToken );
                    if ( authenticationInfo != null )
                    {
                        return PluginAuthenticationInfo.createCacheable( authenticationInfo, getName(), secureHasher );
                    }
                }
            }
            catch ( org.neo4j.server.security.enterprise.auth.plugin.api.AuthenticationException |
                    InvalidAuthTokenException e )
            {
                throw new AuthenticationException( e.getMessage(), e.getCause() );
            }
        }
        return null;
    }

    private void cacheAuthorizationInfo( PluginAuthInfo authInfo )
    {
        // Use the existing authorizationCache in our base class
        Cache<Object, AuthorizationInfo> authorizationCache = getAuthorizationCache();
        Object key = getAuthorizationCacheKey( authInfo.getPrincipals() );
        authorizationCache.put( key, authInfo );
    }

    public boolean canAuthenticate()
    {
        return authPlugin != null || authenticationPlugin != null;
    }

    public boolean canAuthorize()
    {
        return authPlugin != null || authorizationPlugin != null;
    }

    @Override
    public AuthorizationInfo getAuthorizationInfoSnapshot( PrincipalCollection principalCollection )
    {
        return getAuthorizationInfo( principalCollection );
    }

    @Override
    protected Object getAuthorizationCacheKey( PrincipalCollection principals )
    {
        return getAvailablePrincipal( principals );
    }

    @Override
    protected Object getAuthenticationCacheKey( AuthenticationToken token )
    {
        return token != null ? token.getPrincipal() : null;
    }

    @Override
    public boolean supports( AuthenticationToken token )
    {
        return supportsSchemeAndRealm( token );
    }

    private boolean supportsSchemeAndRealm( AuthenticationToken token )
    {
        if ( token instanceof ShiroAuthToken )
        {
            ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;
            return shiroAuthToken.supportsRealm( getName() );
        }
        return false;
    }

    @Override
    public void initialize() throws Throwable
    {
        if ( authenticationPlugin != null )
        {
            authenticationPlugin.initialize( authProviderOperations );
        }
        if ( authorizationPlugin != null && authorizationPlugin != authenticationPlugin )
        {
            authorizationPlugin.initialize( authProviderOperations );
        }
        if ( authPlugin != null )
        {
            authPlugin.initialize( authProviderOperations );
        }
    }

    @Override
    public void start()
    {
        if ( authenticationPlugin != null )
        {
            authenticationPlugin.start();
        }
        if ( authorizationPlugin != null && authorizationPlugin != authenticationPlugin )
        {
            authorizationPlugin.start();
        }
        if ( authPlugin != null )
        {
            authPlugin.start();
        }
    }

    @Override
    public void stop()
    {
        if ( authenticationPlugin != null )
        {
            authenticationPlugin.stop();
        }
        if ( authorizationPlugin != null && authorizationPlugin != authenticationPlugin )
        {
            authorizationPlugin.stop();
        }
        if ( authPlugin != null )
        {
            authPlugin.stop();
        }
    }

    @Override
    public void shutdown()
    {
        if ( authenticationPlugin != null )
        {
            authenticationPlugin.shutdown();
        }
        if ( authorizationPlugin != null && authorizationPlugin != authenticationPlugin )
        {
            authorizationPlugin.shutdown();
        }
        if ( authPlugin != null )
        {
            authPlugin.shutdown();
        }
    }

    private static CustomCacheableAuthenticationInfo.CredentialsMatcher getCustomCredentialsMatcherIfPresent(
            AuthenticationInfo info
    )
    {
        if ( info instanceof CustomCredentialsMatcherSupplier )
        {
            return ((CustomCredentialsMatcherSupplier) info).getCredentialsMatcher();
        }
        return null;
    }

    private class CredentialsMatcher implements org.apache.shiro.authc.credential.CredentialsMatcher
    {
        @Override
        public boolean doCredentialsMatch( AuthenticationToken token, AuthenticationInfo info )
        {
            CustomCacheableAuthenticationInfo.CredentialsMatcher
                    customCredentialsMatcher = getCustomCredentialsMatcherIfPresent( info );

            if ( customCredentialsMatcher != null )
            {
                // Authentication info is originating from a CustomCacheableAuthenticationInfo
                Map<String,Object> authToken = ((ShiroAuthToken) token).getAuthTokenMap();
                try
                {
                    AuthToken pluginApiAuthToken = PluginApiAuthToken.createFromMap( authToken );
                    return customCredentialsMatcher.doCredentialsMatch( pluginApiAuthToken );
                }
                catch ( InvalidAuthTokenException e )
                {
                    throw new AuthenticationException( e.getMessage() );
                }
            }
            else if ( info.getCredentials() != null )
            {
                // Authentication info is originating from a CacheableAuthenticationInfo or a CacheableAuthInfo
                return secureHasher.getHashedCredentialsMatcher()
                        .doCredentialsMatch( PluginShiroAuthToken.of( token ), info );
            }
            else
            {
                // Authentication info is originating from an AuthenticationInfo or an AuthInfo
                if ( PluginRealm.this.isAuthenticationCachingEnabled() )
                {
                    log.error( "Authentication caching is enabled in plugin %s but it does not return " +
                               "cacheable credentials. This configuration is not secure.", getName() );
                    return false;
                }
                return true; // Always match if we do not cache credentials
            }
        }
    }

    private class PluginRealmOperations implements AuthProviderOperations
    {
        private Log innerLog = new Log()
        {
            private String withPluginName( String msg )
            {
                return "{" + getName() + "} " + msg;
            }

            @Override
            public void debug( String message )
            {
                log.debug( withPluginName( message ) );
            }

            @Override
            public void info( String message )
            {
                log.info( withPluginName( message ) );
            }

            @Override
            public void warn( String message )
            {
                log.warn( withPluginName( message ) );
            }

            @Override
            public void error( String message )
            {
                log.error( withPluginName( message ) );
            }

            @Override
            public boolean isDebugEnabled()
            {
                return log.isDebugEnabled();
            }
        };

        @Override
        public Path neo4jHome()
        {
            return config.get( GraphDatabaseSettings.neo4j_home ).getAbsoluteFile().toPath();
        }

        @Override
        public Optional<Path> neo4jConfigFile()
        {
            return Optional.empty();
        }

        @Override
        public String neo4jVersion()
        {
            return Version.getNeo4jVersion();
        }

        @Override
        public Clock clock()
        {
            return clock;
        }

        @Override
        public Log log()
        {
            return innerLog;
        }

        @Override
        public void setAuthenticationCachingEnabled( boolean authenticationCachingEnabled )
        {
            PluginRealm.this.setAuthenticationCachingEnabled( authenticationCachingEnabled );
        }

        @Override
        public void setAuthorizationCachingEnabled( boolean authorizationCachingEnabled )
        {
            PluginRealm.this.setAuthorizationCachingEnabled( authorizationCachingEnabled );
        }
    }
}
