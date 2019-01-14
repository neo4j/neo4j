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
package org.neo4j.server.security.auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static java.util.Collections.emptyList;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class AuthProcedures
{
    @Context
    public SecurityContext securityContext;

    @Context
    public UserManager userManager;

    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = DBMS )
    public void createUser(
            @Name( "username" ) String username,
            @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        userManager.newUser( username, password, requirePasswordChange );
    }

    @Description( "Delete the specified user." )
    @Procedure( name = "dbms.security.deleteUser", mode = DBMS )
    public void deleteUser( @Name( "username" ) String username ) throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        if ( securityContext.subject().hasUsername( username ) )
        {
            throw new InvalidArgumentsException( "Deleting yourself (user '" + username + "') is not allowed." );
        }
        userManager.deleteUser( username );
    }

    @Deprecated
    @Description( "Change the current user's password. Deprecated by dbms.security.changePassword." )
    @Procedure( name = "dbms.changePassword", mode = DBMS, deprecatedBy = "dbms.security.changePassword" )
    public void changePasswordDeprecated( @Name( "password" ) String password ) throws InvalidArgumentsException, IOException
    {
        changePassword( password );
    }

    @Description( "Change the current user's password." )
    @Procedure( name = "dbms.security.changePassword", mode = DBMS )
    public void changePassword( @Name( "password" ) String password ) throws InvalidArgumentsException, IOException
    {
        if ( securityContext.subject() == AuthSubject.ANONYMOUS )
        {
            throw new AuthorizationViolationException( "Anonymous cannot change password" );
        }
        userManager.setUserPassword( securityContext.subject().username(), password, false );
        securityContext.subject().setPasswordChangeNoLongerRequired();
    }

    @Description( "Show the current user." )
    @Procedure( name = "dbms.showCurrentUser", mode = DBMS )
    public Stream<UserResult> showCurrentUser()
    {
        return Stream.of( userResultForName( securityContext.subject().username() ) );
    }

    @Deprecated
    @Description( "Show the current user. Deprecated by dbms.showCurrentUser." )
    @Procedure( name = "dbms.security.showCurrentUser", mode = DBMS, deprecatedBy = "dbms.showCurrentUser" )
    public Stream<UserResult> showCurrentUserDeprecated()
    {
        return showCurrentUser();
    }

    @Description( "List all native users." )
    @Procedure( name = "dbms.security.listUsers", mode = DBMS )
    public Stream<UserResult> listUsers()
    {
        securityContext.assertCredentialsNotExpired();
        Set<String> usernames = userManager.getAllUsernames();

        if ( usernames.isEmpty() )
        {
            return showCurrentUser();
        }
        else
        {
            return usernames.stream().map( this::userResultForName );
        }
    }

    private UserResult userResultForName( String username )
    {
        User user = userManager.silentlyGetUser( username );
        Iterable<String> flags = user == null ? emptyList() : user.getFlags();
        return new UserResult( username, flags );
    }

    public static class UserResult
    {
        public final String username;
        public final List<String> flags;

        UserResult( String username, Iterable<String> flags )
        {
            this.username = username;
            this.flags = new ArrayList<>();
            for ( String f : flags )
            {
                this.flags.add( f );
            }
        }
    }
}
