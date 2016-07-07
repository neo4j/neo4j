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
package org.neo4j.server.security.enterprise.auth.plugin;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.credential.AllowAllCredentialsMatcher;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder;
import org.neo4j.server.security.enterprise.auth.RealmLifecycle;
import org.neo4j.server.security.enterprise.auth.ShiroAuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;

public class PluginRealm extends AuthorizingRealm implements RealmLifecycle
{
    private AuthenticationPlugin authenticationPlugin;
    private AuthorizationPlugin authorizationPlugin;
    private AuthPlugin authPlugin;

    public PluginRealm()
    {
        super();
        setCredentialsMatcher( new AllowAllCredentialsMatcher() );
        setAuthenticationCachingEnabled( true );
        setAuthorizationCachingEnabled( true );
        setRolePermissionResolver( PredefinedRolesBuilder.rolePermissionResolver );
    }

    // TODO: Merge AuthenticationPlugin and AuthorizationPlugin into a single plugin interface?
    public PluginRealm( AuthenticationPlugin authenticationPlugin, AuthorizationPlugin authorizationPlugin )
    {
        this();
        this.authenticationPlugin = authenticationPlugin;
        this.authorizationPlugin = authorizationPlugin;
    }

    public PluginRealm( AuthPlugin authPlugin )
    {
        this();
        this.authPlugin = authPlugin;
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        if ( authorizationPlugin != null )
        {
            org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo authorizationInfo =
                    authorizationPlugin.getAuthorizationInfo( principals.asSet() );
            if ( authorizationInfo != null )
            {
                return PluginAuthorizationInfo.create( authorizationInfo );
            }
        }
        return null;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
    {
        if ( token instanceof ShiroAuthToken )
        {
            if ( authPlugin != null )
            {
                AuthInfo authInfo = authPlugin.getAuthInfo( ((ShiroAuthToken) token).getAuthTokenMap() );
                if ( authInfo != null )
                {
                    PluginAuthInfo pluginAuthInfo = PluginAuthInfo.create( authInfo, getName() );

                    cacheAuthorizationInfo( pluginAuthInfo );

                    return pluginAuthInfo;
                }
            }
            else if ( authenticationPlugin != null )
            {
                org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo authenticationInfo =
                        authenticationPlugin.getAuthenticationInfo( ((ShiroAuthToken) token).getAuthTokenMap() );
                if ( authenticationInfo != null )
                {
                    return PluginAuthenticationInfo.create( authenticationInfo, getName() );
                }
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

    @Override
    protected Object getAuthorizationCacheKey( PrincipalCollection principals )
    {
        return getAvailablePrincipal( principals );
    }

    @Override
    public boolean supports( AuthenticationToken token )
    {
        return token instanceof ShiroAuthToken;
    }

    @Override
    public void initialize() throws Throwable
    {
        if ( authenticationPlugin != null )
        {
            authenticationPlugin.initialize();
        }
        if ( authorizationPlugin != null && authorizationPlugin != authenticationPlugin )
        {
            authorizationPlugin.initialize();
        }
    }

    @Override
    public void start() throws Throwable
    {
        if ( authenticationPlugin != null )
        {
            authenticationPlugin.start();
        }
        if ( authorizationPlugin != null && authorizationPlugin != authenticationPlugin )
        {
            authorizationPlugin.start();
        }
    }

    @Override
    public void stop() throws Throwable
    {
        if ( authenticationPlugin != null )
        {
            authenticationPlugin.stop();
        }
        if ( authorizationPlugin != null && authorizationPlugin != authenticationPlugin )
        {
            authorizationPlugin.stop();
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        if ( authenticationPlugin != null )
        {
            authenticationPlugin.shutdown();
        }
        if ( authorizationPlugin != null && authorizationPlugin != authenticationPlugin )
        {
            authorizationPlugin.shutdown();
        }
    }
}
