/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.rest.dbms;

import java.io.IOException;
import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.http.error.Neo4jHttpException;
import org.neo4j.server.rest.repr.AuthorizationRepresentation;
import org.neo4j.server.rest.repr.PasswordChangeRepresentation;
import org.neo4j.string.UTF8;

import static org.neo4j.server.rest.dbms.AuthorizedRequestWrapper.getLoginContextFromUserPrincipal;

@Path( "/user" )
@Produces( MediaType.APPLICATION_JSON )
@Consumes( MediaType.APPLICATION_JSON )
public class UserService
{
    private final UserManagerSupplier userManagerSupplier;
    private final UriInfo uriInfo;
    private final Principal principal;

    public UserService( @Context UserManagerSupplier userManagerSupplier, @Context HttpServletRequest request, @Context UriInfo uriInfo )
    {
        this.userManagerSupplier = userManagerSupplier;
        this.uriInfo = uriInfo;

        principal = request.getUserPrincipal();
    }

    @GET
    @Path( "/{username}" )
    public AuthorizationRepresentation getUser( @PathParam( "username" ) String username )
    {
        if ( principal == null || !principal.getName().equals( username ) )
        {
            throw new NotFoundException();
        }

        LoginContext loginContext = getLoginContextFromUserPrincipal( principal );
        UserManager userManager = userManagerSupplier.getUserManager( loginContext.subject(), false );

        try
        {
            User user = userManager.getUser( username );
            String passwordChange = uriInfo.getAbsolutePathBuilder().path( "password" ).build().toString();
            return new AuthorizationRepresentation( user, passwordChange );
        }
        catch ( InvalidArgumentsException e )
        {
            throw new NotFoundException();
        }
    }

    @POST
    @Path( "/{username}/password" )
    public void setPassword( @PathParam( "username" ) String username, PasswordChangeRepresentation payload ) throws IOException
    {
        if ( principal == null || !principal.getName().equals( username ) )
        {
            throw new NotFoundException();
        }

        if ( payload.getPassword() == null )
        {
            throw new Neo4jHttpException( 422, Status.Request.InvalidFormat, "Required parameter 'password' is missing." );
        }

        try
        {
            LoginContext loginContext = getLoginContextFromUserPrincipal( principal );
            if ( loginContext == null )
            {
                throw new NotFoundException();
            }
            else
            {
                UserManager userManager = userManagerSupplier.getUserManager( loginContext.subject(), false );
                userManager.setUserPassword( username, UTF8.encode( payload.getPassword() ), false );
            }
        }
        catch ( InvalidArgumentsException e )
        {
            throw new Neo4jHttpException( 422, e.status(), e.getMessage() );
        }
    }
}
