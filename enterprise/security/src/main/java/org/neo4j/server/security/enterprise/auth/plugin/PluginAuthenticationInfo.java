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

import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;

public class PluginAuthenticationInfo extends SimpleAuthenticationInfo
{
    public PluginAuthenticationInfo( Object principal, Object credentials, String realmName )
    {
        super( principal, credentials, realmName );
    }

    public PluginAuthenticationInfo( Object principal, Object hashedCredentials, ByteSource credentialsSalt,
            String realmName )
    {
        super( principal, hashedCredentials, credentialsSalt, realmName );
    }

    public static PluginAuthenticationInfo create( AuthenticationInfo authenticationInfo, String realmName )
    {
        return new PluginAuthenticationInfo( authenticationInfo.getPrincipal(), null, realmName );
    }

    public static PluginAuthenticationInfo create( AuthenticationInfo authenticationInfo, SimpleHash hashedCredentials,
            String realmName )
    {
        return new PluginAuthenticationInfo( authenticationInfo.getPrincipal(),
                hashedCredentials.getBytes(), hashedCredentials.getSalt(), realmName );
    }
}
