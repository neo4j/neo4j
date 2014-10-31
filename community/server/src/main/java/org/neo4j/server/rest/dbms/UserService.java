/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.rest.dbms;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.server.rest.repr.AuthorizationRepresentation;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.ExceptionRepresentation;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.security.auth.SecurityCentral;
import org.neo4j.server.security.auth.exception.IllegalTokenException;
import org.neo4j.server.security.auth.exception.TooManyAuthenticationAttemptsException;

import static org.neo4j.server.rest.web.CustomStatusType.TOO_MANY;
import static org.neo4j.server.rest.web.CustomStatusType.UNPROCESSABLE;

@Path("/user")
public class UserService
{
    private final SecurityCentral security;
    private final InputFormat input;
    private final OutputFormat output;
    private final ConsoleLogger log;

    public UserService(@Context SecurityCentral security, @Context InputFormat input, @Context OutputFormat output,
                       @Context ConsoleLogger log)
    {
        this.security = security;
        this.input = input;
        this.output = output;
        this.log = log;
    }

    @POST
    @Path("/{user}/authorization_token")
    public Response regenerateToken( @PathParam( "user" ) String user, @Context HttpServletRequest req, String payload)
    {
        try
        {
            Map<String,Object> deserialized = input.readMap( payload );

            String password = getString( deserialized, "password" );

            if( security.authenticate( user, password ))
            {
                if( deserialized.containsKey( "new_authorization_token" ) )
                {
                    security.setToken( user, getString( deserialized, "new_authorization_token" ) );
                }
                else
                {
                    security.regenerateToken( user );
                }
                return output.ok( new AuthorizationRepresentation( security.userForName( user ) ) );
            }
            else
            {
                log.warn( "Failed authentication attempt for '%s' from %s", user, req.getRemoteAddr() );
            }

            return output.response( UNPROCESSABLE, new ExceptionRepresentation(
                    new Neo4jError( Status.Security.AuthenticationFailed, "Invalid username and/or password." ) ) );
        }
        catch ( BadInputException | IllegalTokenException e )
        {
            return output.badRequestWithoutLegacyStacktrace( e );
        }
        catch ( TooManyAuthenticationAttemptsException e )
        {
            return output.response( TOO_MANY, new ExceptionRepresentation( new Neo4jError( e.status(), e ) ) );
        }
        catch ( IOException e )
        {
            return output.serverErrorWithoutLegacyStacktrace( e );
        }
    }

    @POST
    @Path("/{user}/password")
    public Response setPassword( @PathParam( "user" ) String user, @Context HttpServletRequest req, String payload )
    {
        try
        {
            Map<String,Object> deserialized = input.readMap( payload );

            String password = getString( deserialized, "password" );
            String newPassword = getString( deserialized, "new_password" );

            if(password.equals( newPassword ))
            {
                return output.response( UNPROCESSABLE, new ExceptionRepresentation(
                        new Neo4jError( Status.Request.Invalid, "Old password and new password cannot be the same." ) ) );
            }

            if( security.authenticate( user, password ))
            {
                security.setPassword( user, newPassword );
                return output.ok( new AuthorizationRepresentation( security.userForName( user ) ) );
            }
            else
            {
                log.warn( "Failed authentication attempt for '%s' from %s", user, req.getRemoteAddr() );
            }

            return output.response( UNPROCESSABLE, new ExceptionRepresentation(
                    new Neo4jError( Status.Security.AuthenticationFailed, "Invalid username and/or password." ) ) );
        }
        catch ( BadInputException e )
        {
            return output.badRequestWithoutLegacyStacktrace( e );
        }
        catch ( TooManyAuthenticationAttemptsException e )
        {
            return output.response( TOO_MANY, new ExceptionRepresentation( new Neo4jError( e.status(), e ) ) );
        }
        catch ( IOException e )
        {
            return output.serverErrorWithoutLegacyStacktrace( e );
        }
    }

    private String getString( Map<String, Object> data, String key ) throws BadInputException
    {
        Object o = data.get( key );
        if( o == null )
        {
            throw new BadInputException( String.format("Required parameter '%s' is missing.", key) );
        }
        if(!(o instanceof String))
        {
            throw new BadInputException( String.format("Expected '%s' to be a string.", key) );
        }

        return (String)o;
    }
}
