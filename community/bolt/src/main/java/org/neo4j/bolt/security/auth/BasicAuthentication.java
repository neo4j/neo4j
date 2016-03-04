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
import java.util.function.Supplier;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.auth.BasicAuthManager;

/**
 * Performs basic authentication with user name and password.
 */
public class BasicAuthentication implements Authentication
{
    private final BasicAuthManager authManager;
    private final static String SCHEME = "basic";
    private final Log log;
    private final Supplier<String> identifier;


    public BasicAuthentication( BasicAuthManager authManager, LogProvider logProvider, Supplier<String> identifier )
    {
        this.authManager = authManager;
        this.log = logProvider.getLog( getClass() );
        this.identifier = identifier;
    }

    @Override
    public void authenticate( Map<String,Object> authToken ) throws AuthenticationException
    {
        if ( !SCHEME.equals( authToken.get( SCHEME_KEY ) ) )
        {
            throw new AuthenticationException( Status.Security.Unauthorized, identifier.get(),
                    "Authentication token must contain: '" + SCHEME_KEY + " : " + SCHEME + "'" );
        }

        String user = safeCast( PRINCIPAL, authToken );
        String password = safeCast( CREDENTIALS, authToken );
        if ( authToken.containsKey( NEW_CREDENTIALS ) )
        {
            update( user, password, safeCast( NEW_CREDENTIALS, authToken ) );
        }
        else
        {
            authenticate( user, password );
        }
    }

    private void authenticate( String user, String password ) throws AuthenticationException
    {
        switch ( authManager.authenticate( user, password ) )
        {
        case SUCCESS:
            break;
        case PASSWORD_CHANGE_REQUIRED:
            throw new AuthenticationException( Status.Security.CredentialsExpired, identifier.get() );
        case TOO_MANY_ATTEMPTS:
            throw new AuthenticationException( Status.Security.AuthenticationRateLimit, identifier.get() );
        default:
            log.warn( "Failed authentication attempt for '%s'", user);
            throw new AuthenticationException( Status.Security.Unauthorized, identifier.get() );
        }
    }

    private void update( String user, String password, String newPassword ) throws AuthenticationException
    {
        switch ( authManager.authenticate( user, password ) )
        {
        case SUCCESS:
        case PASSWORD_CHANGE_REQUIRED:
            try
            {
                authManager.setPassword( user, newPassword );
            }
            catch ( IOException e )
            {
                throw new AuthenticationException( Status.Security.Unauthorized, identifier.get(), e.getMessage(), e );
            }
            break;
        default:
            throw new AuthenticationException( Status.Security.Unauthorized, identifier.get() );
        }
    }

    private String safeCast( String key, Map<String,Object> authToken ) throws AuthenticationException
    {
        Object value = authToken.get( key );
        if ( value == null || !(value instanceof String) )
        {
            throw new AuthenticationException( Status.Security.Unauthorized, identifier.get(),
                    "The value associated with the key `" + key + "` must be a String but was: " +
                    (value == null ? "null" : value.getClass().getSimpleName()));
        }

        return (String) value;
    }
}
