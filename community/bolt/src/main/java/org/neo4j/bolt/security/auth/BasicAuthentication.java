/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.api.security.AuthToken.NEW_CREDENTIALS;
import static org.neo4j.kernel.api.security.AuthToken.PRINCIPAL;
import static org.neo4j.kernel.api.security.AuthToken.SCHEME_KEY;

/**
 * Performs basic authentication with user name and password.
 */
public class BasicAuthentication implements Authentication
{
    private final AuthManager authManager;
    private static final String SCHEME = "basic";
    private final Log log;

    public BasicAuthentication( AuthManager authManager, LogProvider logProvider )
    {
        this.authManager = authManager;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public AuthenticationResult authenticate( Map<String,Object> authToken ) throws AuthenticationException
    {
        if ( !SCHEME.equals( authToken.get( SCHEME_KEY ) ) )
        {
            throw new AuthenticationException( Status.Security.Unauthorized,
                    "Missing username and password" );
        }

        if ( authToken.containsKey( NEW_CREDENTIALS ) )
        {
            return update( authToken );
        }
        else
        {
            return doAuthenticate( authToken );
        }
    }

    private AuthenticationResult doAuthenticate( Map<String,Object> authToken ) throws AuthenticationException
    {
        try
        {
            AuthSubject authSubject = authManager.login( authToken );

            switch ( authSubject.getAuthenticationResult() )
            {
            case SUCCESS:
            case PASSWORD_CHANGE_REQUIRED:
                break;
            case TOO_MANY_ATTEMPTS:
                throw new AuthenticationException( Status.Security.AuthenticationRateLimit );
            default:
                log.warn( "Failed authentication attempt for '%s'", AuthToken.safeCast( PRINCIPAL, authToken ) );
                throw new AuthenticationException( Status.Security.Unauthorized );
            }

            return new BasicAuthenticationResult( authSubject );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new AuthenticationException( e.status(), e.getMessage() );
        }
    }

    private AuthenticationResult update( Map<String,Object> authToken ) throws AuthenticationException
    {
        try
        {
            AuthSubject authSubject = authManager.login( authToken );

            switch ( authSubject.getAuthenticationResult() )
            {
            case SUCCESS:
            case PASSWORD_CHANGE_REQUIRED:
                String newPassword = AuthToken.safeCast( NEW_CREDENTIALS, authToken );
                authSubject.setPassword( newPassword );
                break;
            default:
                throw new AuthenticationException( Status.Security.Unauthorized );
            }

            return new BasicAuthenticationResult( authSubject );
        }
        catch ( AuthorizationViolationException | InvalidArgumentsException | InvalidAuthTokenException e )
        {
            throw new AuthenticationException( e.status(), e.getMessage(), e );
        }
        catch ( IOException e )
        {
            throw new AuthenticationException( Status.Security.Unauthorized, e.getMessage(), e );
        }
    }
}
