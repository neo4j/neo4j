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
import java.util.List;
import java.util.Map;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;

public class TestCustomParametersAuthenticationPlugin extends AuthenticationPlugin.Adapter
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthenticationInfo authenticate( AuthToken authToken )
    {
        Map<String,Object> parameters = authToken.parameters();

        List<Long> myCredentials = (List<Long>) parameters.get( "my_credentials" );

        if ( myCredentials.containsAll( Arrays.asList( 1L, 2L, 3L, 4L ) ) )
        {
            return (AuthenticationInfo) () -> "neo4j";
        }
        return null;
    }
}
