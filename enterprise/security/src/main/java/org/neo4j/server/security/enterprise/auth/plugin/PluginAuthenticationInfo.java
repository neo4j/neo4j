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

import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.util.ByteSource;

import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CustomCacheableAuthenticationInfo;

public class PluginAuthenticationInfo extends SimpleAuthenticationInfo implements CustomCredentialsMatcherSupplier
{
    private CustomCacheableAuthenticationInfo.CredentialsMatcher credentialsMatcher;

    public PluginAuthenticationInfo( Object principal, String realmName,
            CustomCacheableAuthenticationInfo.CredentialsMatcher credentialsMatcher )
    {
        super( principal, null, realmName );
        this.credentialsMatcher = credentialsMatcher;
    }

    public PluginAuthenticationInfo( Object principal, Object hashedCredentials, ByteSource credentialsSalt,
            String realmName )
    {
        super( principal, hashedCredentials, credentialsSalt, realmName );
    }

    @Override
    public CustomCacheableAuthenticationInfo.CredentialsMatcher getCredentialsMatcher()
    {
        return credentialsMatcher;
    }

    private static PluginAuthenticationInfo create( AuthenticationInfo authenticationInfo, String realmName )
    {
        return new PluginAuthenticationInfo( authenticationInfo.getPrincipal(), realmName, null );
    }

    private static PluginAuthenticationInfo create( AuthenticationInfo authenticationInfo, SimpleHash hashedCredentials,
            String realmName )
    {
        return new PluginAuthenticationInfo( authenticationInfo.getPrincipal(),
                hashedCredentials.getBytes(), hashedCredentials.getSalt(), realmName );
    }

    public static PluginAuthenticationInfo createCacheable( AuthenticationInfo authenticationInfo, String realmName,
            SecureHasher secureHasher )
    {
        if ( authenticationInfo instanceof CustomCacheableAuthenticationInfo )
        {
            CustomCacheableAuthenticationInfo info = (CustomCacheableAuthenticationInfo) authenticationInfo;
            return new PluginAuthenticationInfo( authenticationInfo, realmName, info.getCredentialsMatcher() );
        }
        else if ( authenticationInfo instanceof CacheableAuthenticationInfo )
        {
            byte[] credentials = ((CacheableAuthenticationInfo) authenticationInfo).getCredentials();
            SimpleHash hashedCredentials = secureHasher.hash( credentials );
            return PluginAuthenticationInfo.create( authenticationInfo, hashedCredentials, realmName );
        }
        else
        {
            return PluginAuthenticationInfo.create( authenticationInfo, realmName );
        }
    }
}
