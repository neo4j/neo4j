/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.security.auth;

import java.util.Map;

import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

/**
 * Performs basic authentication with user name and password.
 */
public class BasicAuthentication implements Authentication
{
    private final AuthManager authManager;

    public BasicAuthentication( AuthManager authManager )
    {
        this.authManager = authManager;
    }

    @Override
    public AuthenticationResult authenticate( Map<String,Object> authToken ) throws AuthenticationException
    {
        try
        {
            LoginContext loginContext = authManager.login( authToken );

            switch ( loginContext.subject().getAuthenticationResult() )
            {
            case SUCCESS:
            case PASSWORD_CHANGE_REQUIRED:
                break;
            case TOO_MANY_ATTEMPTS:
                throw new AuthenticationException( Status.Security.AuthenticationRateLimit );
            default:
                throw new AuthenticationException( Status.Security.Unauthorized );
            }

            return new BasicAuthenticationResult( loginContext );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new AuthenticationException( e.status(), e.getMessage() );
        }
    }
}
