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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;

public class TestCombinedAuthPlugin implements AuthenticationPlugin, AuthorizationPlugin
{
    @Override
    public AuthenticationInfo getAuthenticationInfo( Map<String,Object> authToken )
    {
        String principal;
        String credentials;
        String realm;

        try
        {
            principal = AuthToken.safeCast( AuthToken.PRINCIPAL, authToken );
            credentials = AuthToken.safeCast( AuthToken.CREDENTIALS, authToken );
            realm = AuthToken.safeCast( "realm", authToken );
        }
        catch ( InvalidAuthTokenException e )
        {
            return null;
        }

        if ( realm.equals( getClass().getSimpleName() ) && principal.equals( "neo4j" ) && credentials.equals( "neo4j" ) )
        {
            return new AuthenticationInfo()
            {
                @Override
                public Object getPrincipal()
                {
                    return "neo4j";
                }

                @Override
                public Object getCredentials()
                {
                    return null;
                }
            };
        }
        return null;
    }

    @Override
    public void initialize() throws Throwable
    {

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

    @Override
    public AuthorizationInfo getAuthorizationInfo( Collection<Object> principals )
    {
        if ( principals.contains( "neo4j" ) )
        {
            return (AuthorizationInfo) () -> Collections.singleton( PredefinedRoles.READER );
        }
        return null;
    }
}
