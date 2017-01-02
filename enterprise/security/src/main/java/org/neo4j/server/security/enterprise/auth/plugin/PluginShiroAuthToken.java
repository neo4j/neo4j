/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.apache.shiro.authc.AuthenticationToken;

import java.util.Map;

import org.neo4j.server.security.enterprise.auth.ShiroAuthToken;

/**
 * Version of ShiroAuthToken that returns credentials as a char array
 * so that it is compatible for credentials matching with the
 * cacheable auth info results returned by the plugin API
 */
public class PluginShiroAuthToken extends ShiroAuthToken
{
    private PluginShiroAuthToken( Map<String,Object> authTokenMap )
    {
        super( authTokenMap );
    }

    @Override
    public Object getCredentials()
    {
        return ((String) super.getCredentials()).toCharArray();
    }

    public static PluginShiroAuthToken of( ShiroAuthToken shiroAuthToken )
    {
        return new PluginShiroAuthToken( shiroAuthToken.getAuthTokenMap() );
    }

    public static PluginShiroAuthToken of( AuthenticationToken authenticationToken )
    {
        ShiroAuthToken shiroAuthToken = (ShiroAuthToken) authenticationToken;
        return PluginShiroAuthToken.of( shiroAuthToken );
    }
}
