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

import org.apache.shiro.authz.Permission;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;

import java.util.Collection;

import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;

public class PluginAuthInfo implements org.apache.shiro.authc.AuthenticationInfo, org.apache.shiro.authz.AuthorizationInfo
{
    private final AuthInfo authInfo;
    private final String realmName;

    public static PluginAuthInfo create( AuthInfo authInfo, String realmName )
    {
        return new PluginAuthInfo( authInfo, realmName );
    }

    private PluginAuthInfo( AuthInfo authInfo, String realmName )
    {
        this.authInfo = authInfo;
        this.realmName = realmName;
    }

    @Override
    public PrincipalCollection getPrincipals()
    {
        return new SimplePrincipalCollection( this.authInfo.getPrincipal(), realmName );
    }

    @Override
    public Object getCredentials()
    {
        return this.authInfo.getCredentials();
    }

    @Override
    public Collection<String> getRoles()
    {
        return this.authInfo.getRoles();
    }

    @Override
    public Collection<String> getStringPermissions()
    {
        return null;
    }

    @Override
    public Collection<Permission> getObjectPermissions()
    {
        return null;
    }
}