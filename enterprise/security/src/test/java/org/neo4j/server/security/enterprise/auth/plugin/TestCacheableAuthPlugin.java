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
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthInfo;

public class TestCacheableAuthPlugin extends AuthPlugin.CachingEnabledAdapter
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthInfo authenticateAndAuthorize( AuthToken authToken )
    {
        getAuthInfoCallCount.incrementAndGet();

        String principal = authToken.principal();
        char[] credentials = authToken.credentials();

        if ( principal.equals( "neo4j" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return CacheableAuthInfo.of( "neo4j", "neo4j".getBytes(),
                    Collections.singleton( PredefinedRoles.READER ) );
        }
        return null;
    }

    // For testing purposes
    public static AtomicInteger getAuthInfoCallCount = new AtomicInteger( 0 );
}
