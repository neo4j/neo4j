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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthorizationExpiredException;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;

public class TestCombinedAuthPlugin extends AuthenticationPlugin.Adapter implements AuthorizationPlugin
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthenticationInfo authenticate( AuthToken authToken )
    {
        String principal = authToken.principal();
        char[] credentials = authToken.credentials();

        if ( principal.equals( "neo4j" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return AuthenticationInfo.of( "neo4j" );
        }
        else if ( principal.equals( "authorization_expired_user" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return (AuthenticationInfo) () -> "authorization_expired_user";
        }
        return null;
    }

    @Override
    public AuthorizationInfo authorize( Collection<PrincipalAndProvider> principals )
    {
        if ( principals.stream().anyMatch( p -> "neo4j".equals( p.principal() ) ) )
        {
            return (AuthorizationInfo) () -> Collections.singleton( PredefinedRoles.READER );
        }
        else if ( principals.stream().anyMatch( p -> "authorization_expired_user".equals( p.principal() ) ) )
        {
            throw new AuthorizationExpiredException( "authorization_expired_user needs to re-authenticate." );
        }
        return null;
    }
}
