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
package org.neo4j.server.security.auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.DBMS;

public class AuthProcedures
{
    @Context
    public AuthSubject authSubject;

    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = DBMS )
    public void createUser(
            @Name( "username" ) String username,
            @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "true") boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        BasicAuthSubject subject = BasicAuthSubject.castOrFail( authSubject );
        subject.getAuthManager().newUser( username, password, requirePasswordChange );
    }

    @Description( "Delete the specified user." )
    @Procedure( name = "dbms.security.deleteUser", mode = DBMS )
    public void deleteUser( @Name( "username" ) String username ) throws InvalidArgumentsException, IOException
    {
        BasicAuthSubject subject = BasicAuthSubject.castOrFail( authSubject );
        if ( subject.hasUsername( username ) )
        {
            throw new InvalidArgumentsException( "Deleting yourself (user '" + username + "') is not allowed." );
        }
        subject.getAuthManager().deleteUser( username );
    }

    @Deprecated
    @Description( "Change the current user's password. Deprecated by dbms.security.changePassword." )
    @Procedure( name = "dbms.changePassword", mode = DBMS, deprecatedBy = "dbms.security.changePassword" )
    public void changePasswordDeprecated( @Name( "password" ) String password ) throws InvalidArgumentsException, IOException
    {
        authSubject.setPassword( password, false );
    }

    @Description( "Change the current user's password." )
    @Procedure( name = "dbms.security.changePassword", mode = DBMS )
    public void changePassword( @Name( "password" ) String password ) throws InvalidArgumentsException, IOException
    {
        authSubject.setPassword( password, false );
    }

    @Description( "Show the current user." )
    @Procedure( name = "dbms.security.showCurrentUser", mode = DBMS )
    public Stream<UserResult> showCurrentUser() throws InvalidArgumentsException, IOException
    {
        BasicAuthSubject subject = BasicAuthSubject.castOrFail( authSubject );
        return Stream.of( new UserResult(
                subject.name(),
                subject.getAuthManager().getUser( subject.name() ).getFlags()
            ) );
    }

    @Description( "List all local users." )
    @Procedure( name = "dbms.security.listUsers", mode = DBMS )
    public Stream<UserResult> listUsers() throws InvalidArgumentsException, IOException
    {
        BasicAuthSubject subject = BasicAuthSubject.castOrFail( authSubject );
        Set<String> usernames = subject.getAuthManager().getAllUsernames();
        List<UserResult> results = new ArrayList<>();
        for ( String username : usernames )
        {
            results.add( new UserResult(
                    username,
                    subject.getAuthManager().getUser( username ).getFlags()
                ) );
        }
        return results.stream();
    }

    public static class UserResult
    {
        public final String username;
        public final List<String> flags;

        UserResult( String username, Iterable<String> flags )
        {
            this.username = username;
            this.flags = new ArrayList<>();
            for ( String f : flags ) {this.flags.add( f );}
        }
    }
}
