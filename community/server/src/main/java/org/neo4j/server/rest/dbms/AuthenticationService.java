/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.repr.AuthorizationRepresentation;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.ExceptionRepresentation;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.security.auth.SecurityCentral;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.exception.TooManyAuthenticationAttemptsException;

import static org.neo4j.kernel.api.exceptions.Status.Security.AuthorizationFailed;
import static org.neo4j.server.rest.dbms.UserService.PASSWORD;
import static org.neo4j.server.rest.dbms.UserService.USERNAME;
import static org.neo4j.server.rest.web.CustomStatusType.TOO_MANY;
import static org.neo4j.server.rest.web.CustomStatusType.UNPROCESSABLE;

/**
 * This serves as the HTTP API entry point into {@link org.neo4j.server.security.auth.SecurityCentral}, and is used
 * by remote clients to authenticate, check auth status and acquire authorization tokens.
 */
@Path(AuthenticationService.AUTHENTICATION_PATH)
public class AuthenticationService
{
    public static final String AUTHENTICATION_PATH = "/authentication";

    private final SecurityCentral security;
    private final InputFormat input;
    private final OutputFormat output;
    private final ConsoleLogger log;
    private final boolean authEnabled;

    public AuthenticationService(@Context SecurityCentral security, @Context InputFormat input,
                                 @Context Config config,
                                 @Context OutputFormat output, @Context ConsoleLogger log )
    {
        this.security = security;
        this.input = input;
        this.output = output;
        this.log = log;
        this.authEnabled = config.get( ServerSettings.authorization_enabled );
    }

    @POST
    public Response authenticate( @Context HttpServletRequest req, String payload )
    {
        try
        {
            Map<String,Object> deserialized = input.readMap( payload );

            String user = getString(deserialized, USERNAME );
            String password = getString( deserialized, PASSWORD );

            if( security.authenticate( user, password ))
            {
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
        catch ( IllegalArgumentException e )
        {
            return output.response( UNPROCESSABLE, new ExceptionRepresentation( new Neo4jError( Status.Request.Invalid, e.getMessage() ) ) );
        }
        catch ( TooManyAuthenticationAttemptsException e )
        {
            return output.response( TOO_MANY, new ExceptionRepresentation( new Neo4jError( e.status(), e ) ) );
        }
    }

    @GET
    public Response metadata( @HeaderParam( HttpHeaders.AUTHORIZATION ) String authHeader )
    {
        if(!authEnabled)
        {
            return output.ok();
        }

        if(authHeader == null)
        {
            return output.unauthorized( new ExceptionRepresentation ( new Neo4jError( AuthorizationFailed, "No authorization token supplied." ) ), "None" );
        }

        String token = AuthenticateHeaders.extractToken( authHeader );

        if(token.length() == 0)
        {
            return output.response( Response.Status.BAD_REQUEST, new ExceptionRepresentation ( new Neo4jError( Status.Request.InvalidFormat, "Invalid Authorization header." ) ));
        }

        User user = security.userForToken( token );
        if( user.privileges().APIAccess() )
        {
            return output.ok( new AuthorizationRepresentation( user ) );
        }

        return output.unauthorized( new ExceptionRepresentation (new Neo4jError( AuthorizationFailed, "Invalid authorization token supplied." ) ), "None" );
    }

    private String getString( Map<String, Object> data, String key ) throws IllegalArgumentException
    {
        Object o = data.get( key );
        if( o == null )
        {
            throw new IllegalArgumentException( String.format("Required parameter '%s' is missing.", key) );
        }
        if(!(o instanceof String))
        {
            throw new IllegalArgumentException( String.format("Expected '%s' to be a string.", key) );
        }

        return (String)o;
    }

}
