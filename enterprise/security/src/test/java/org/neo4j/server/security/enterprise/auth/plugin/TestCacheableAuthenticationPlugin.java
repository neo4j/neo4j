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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.server.security.enterprise.auth.plugin.api.RealmOperations;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthenticationInfo;

public class TestCacheableAuthenticationPlugin implements AuthenticationPlugin
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthenticationInfo getAuthenticationInfo( Map<String,Object> authToken )
    {
        String principal;
        String credentials;

        getAuthenticationInfoCallCount.incrementAndGet();

        try
        {
            principal = AuthToken.safeCast( AuthToken.PRINCIPAL, authToken );
            credentials = AuthToken.safeCast( AuthToken.CREDENTIALS, authToken );
        }
        catch ( InvalidAuthTokenException e )
        {
            return null;
        }

        if ( principal.equals( "neo4j" ) && credentials.equals( "neo4j" ) )
        {
            return new CacheableAuthenticationInfo()
            {
                @Override
                public Object getPrincipal()
                {
                    return "neo4j";
                }

                @Override
                public byte[] getCredentials()
                {
                    return credentials.getBytes();
                }
            };
        }
        return null;
    }

    @Override
    public void initialize( RealmOperations realmOperations ) throws Throwable
    {
        realmOperations.setAuthenticationCachingEnabled( true );
    }

    @Override
    public void start() throws Throwable
    {

    }

    @Override
    public void stop() throws Throwable
    {

    }

    @Override
    public void shutdown() throws Throwable
    {

    }

    // For testing purposes
    public static AtomicInteger getAuthenticationInfoCallCount = new AtomicInteger( 0 );
}
