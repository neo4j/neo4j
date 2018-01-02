/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.util.ByteSource;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.ShiroAuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CustomCacheableAuthenticationInfo;

class PluginAuthenticationInfo extends ShiroAuthenticationInfo implements CustomCredentialsMatcherSupplier
{
    private CustomCacheableAuthenticationInfo.CredentialsMatcher credentialsMatcher;

    private PluginAuthenticationInfo( Object principal, String realmName,
            CustomCacheableAuthenticationInfo.CredentialsMatcher credentialsMatcher )
    {
        super( principal, realmName, AuthenticationResult.SUCCESS );
        this.credentialsMatcher = credentialsMatcher;
    }

    private PluginAuthenticationInfo( Object principal, Object hashedCredentials, ByteSource credentialsSalt,
            String realmName )
    {
        super( principal, hashedCredentials, credentialsSalt, realmName, AuthenticationResult.SUCCESS );
    }

    @Override
    public CustomCacheableAuthenticationInfo.CredentialsMatcher getCredentialsMatcher()
    {
        return credentialsMatcher;
    }

    private static PluginAuthenticationInfo create(
            AuthenticationInfo authenticationInfo,
            String realmName )
    {
        return new PluginAuthenticationInfo( authenticationInfo.principal(), realmName, null );
    }

    private static PluginAuthenticationInfo create(
            AuthenticationInfo authenticationInfo,
            SimpleHash hashedCredentials,
            String realmName )
    {
        return new PluginAuthenticationInfo(
                            authenticationInfo.principal(),
                            hashedCredentials.getBytes(),
                            hashedCredentials.getSalt(),
                            realmName );
    }

    public static PluginAuthenticationInfo createCacheable(
            AuthenticationInfo authenticationInfo,
            String realmName,
            SecureHasher secureHasher )
    {
        if ( authenticationInfo instanceof CustomCacheableAuthenticationInfo )
        {
            CustomCacheableAuthenticationInfo info = (CustomCacheableAuthenticationInfo) authenticationInfo;
            return new PluginAuthenticationInfo( authenticationInfo.principal(), realmName, info.credentialsMatcher() );
        }
        else if ( authenticationInfo instanceof CacheableAuthenticationInfo )
        {
            byte[] credentials = ((CacheableAuthenticationInfo) authenticationInfo).credentials();
            SimpleHash hashedCredentials = secureHasher.hash( credentials );
            return PluginAuthenticationInfo.create( authenticationInfo, hashedCredentials, realmName );
        }
        else
        {
            return PluginAuthenticationInfo.create( authenticationInfo, realmName );
        }
    }
}
