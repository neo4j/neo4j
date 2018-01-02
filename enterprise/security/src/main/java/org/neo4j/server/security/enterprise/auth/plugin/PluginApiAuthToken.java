/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;

import static org.neo4j.kernel.api.security.AuthToken.PRINCIPAL;
import static org.neo4j.kernel.api.security.AuthToken.CREDENTIALS;
import static org.neo4j.kernel.api.security.AuthToken.PARAMETERS;

public class PluginApiAuthToken implements AuthToken
{
    private final String principal;
    private final char[] credentials;
    private final Map<String,Object> parameters;

    private PluginApiAuthToken( String principal, char[] credentials, Map<String,Object> parameters )
    {
        this.principal = principal;
        this.credentials = credentials;
        this.parameters = parameters;
    }

    @Override
    public String principal()
    {
        return principal;
    }

    @Override
    public char[] credentials()
    {
        return credentials;
    }

    @Override
    public Map<String,Object> parameters()
    {
        return parameters;
    }

    public static AuthToken of( String principal, char[] credentials, Map<String,Object> parameters )
    {
        return new PluginApiAuthToken( principal, credentials, parameters );
    }

    public static AuthToken createFromMap( Map<String,Object> authTokenMap ) throws InvalidAuthTokenException
    {
        String scheme = org.neo4j.kernel.api.security.AuthToken
                .safeCast( org.neo4j.kernel.api.security.AuthToken.SCHEME_KEY, authTokenMap );

        // Always require principal
        String principal = org.neo4j.kernel.api.security.AuthToken.safeCast( PRINCIPAL, authTokenMap );

        String credentials = null;
        if ( scheme.equals( org.neo4j.kernel.api.security.AuthToken.BASIC_SCHEME ) )
        {
            // Basic scheme requires credentials
            credentials = org.neo4j.kernel.api.security.AuthToken.safeCast( CREDENTIALS, authTokenMap );
        }
        else
        {
            // Otherwise credentials are optional
            Object credentialsObject = authTokenMap.get( CREDENTIALS );
            if ( credentialsObject instanceof String )
            {
                credentials = (String) credentialsObject;
            }
        }
        Map<String,Object> parameters = org.neo4j.kernel.api.security.AuthToken.safeCastMap( PARAMETERS, authTokenMap );

        return PluginApiAuthToken.of(
                principal,
                credentials != null ? credentials.toCharArray() : null,
                parameters );
    }
}
