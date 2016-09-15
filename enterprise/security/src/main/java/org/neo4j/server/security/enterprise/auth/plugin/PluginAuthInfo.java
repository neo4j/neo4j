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

import org.apache.shiro.authc.SimpleAccount;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.util.ByteSource;

import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthInfo;

public class PluginAuthInfo extends SimpleAccount
{
    public PluginAuthInfo( Object principal, Object credentials, String realmName, Set<String> roles )
    {
        super( principal, credentials, realmName, roles, null );
    }

    public PluginAuthInfo( Object principal, Object hashedCredentials, ByteSource credentialsSalt,
            String realmName, Set<String> roles )
    {
        super( principal, hashedCredentials, credentialsSalt, realmName );
        setRoles( roles );
    }

    public static PluginAuthInfo create( AuthInfo authInfo, String realmName )
    {
        return new PluginAuthInfo( authInfo.getPrincipal(), null, realmName,
                authInfo.getRoles().stream().collect( Collectors.toSet() ) );
    }

    private static PluginAuthInfo create( AuthInfo authInfo, SimpleHash hashedCredentials,
            String realmName )
    {
        return new PluginAuthInfo( authInfo.getPrincipal(),
                hashedCredentials.getBytes(), hashedCredentials.getSalt(), realmName,
                authInfo.getRoles().stream().collect( Collectors.toSet()) );
    }

    public static PluginAuthInfo createCacheable( AuthInfo authInfo, String realmName, SecureHasher secureHasher )
    {
        if ( authInfo instanceof CacheableAuthInfo )
        {
            byte[] credentials = ((CacheableAuthInfo) authInfo).getCredentials();
            SimpleHash hashedCredentials = secureHasher.hash( credentials );
            return PluginAuthInfo.create( authInfo, hashedCredentials, realmName );
        }
        else
        {
            return new PluginAuthInfo( authInfo.getPrincipal(), null, realmName,
                    authInfo.getRoles().stream().collect( Collectors.toSet() ) );
        }
    }
}
