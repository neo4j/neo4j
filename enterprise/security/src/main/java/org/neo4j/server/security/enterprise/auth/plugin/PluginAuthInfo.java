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

import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.util.ByteSource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.ShiroAuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthInfo;

public class PluginAuthInfo extends ShiroAuthenticationInfo implements AuthorizationInfo
{
    Set<String> roles;

    private PluginAuthInfo( Object principal, String realmName, Set<String> roles )
    {
        super( principal, realmName, AuthenticationResult.SUCCESS );
        this.roles = roles;
    }

    private PluginAuthInfo( Object principal, Object hashedCredentials, ByteSource credentialsSalt,
            String realmName, Set<String> roles )
    {
        super( principal, hashedCredentials, credentialsSalt, realmName, AuthenticationResult.SUCCESS );
        this.roles = roles;
    }

    private PluginAuthInfo( AuthInfo authInfo, SimpleHash hashedCredentials, String realmName )
    {
        this( authInfo.principal(), hashedCredentials.getBytes(), hashedCredentials.getSalt(), realmName,
                new HashSet<>( authInfo.roles() ) );
    }

    public static PluginAuthInfo create( AuthInfo authInfo, String realmName )
    {
        return new PluginAuthInfo( authInfo.principal(), realmName, new HashSet<>( authInfo.roles() ) );
    }

    public static PluginAuthInfo createCacheable( AuthInfo authInfo, String realmName, SecureHasher secureHasher )
    {
        if ( authInfo instanceof CacheableAuthInfo )
        {
            byte[] credentials = ((CacheableAuthInfo) authInfo).credentials();
            SimpleHash hashedCredentials = secureHasher.hash( credentials );
            return new PluginAuthInfo( authInfo, hashedCredentials, realmName );
        }
        else
        {
            return new PluginAuthInfo( authInfo.principal(), realmName, new HashSet<>( authInfo.roles() ) );
        }
    }

    @Override
    public Collection<String> getRoles()
    {
        return roles;
    }

    @Override
    public Collection<String> getStringPermissions()
    {
        return Collections.emptyList();
    }

    @Override
    public Collection<Permission> getObjectPermissions()
    {
        return Collections.emptyList();
    }
}
