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

import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthInfo;

public class PluginAuthInfo extends SimpleAccount
{
    public PluginAuthInfo( Object principal, Object credentials, String realmName, Set<String> roles )
    {
        super( principal, credentials, realmName, roles, null );
    }

    public static PluginAuthInfo create( AuthInfo authInfo, String realmName )
    {
        return new PluginAuthInfo( authInfo.getPrincipal(), null, realmName,
                new LinkedHashSet<>( authInfo.getRoles() ) );
    }

    public static PluginAuthInfo create( CacheableAuthInfo authInfo, String realmName )
    {
        return new PluginAuthInfo( authInfo.getPrincipal(), authInfo.getCredentials(), realmName,
                new LinkedHashSet<>( authInfo.getRoles() ) );
    }
}
