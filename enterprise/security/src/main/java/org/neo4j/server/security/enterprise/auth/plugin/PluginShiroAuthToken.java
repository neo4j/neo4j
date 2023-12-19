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
