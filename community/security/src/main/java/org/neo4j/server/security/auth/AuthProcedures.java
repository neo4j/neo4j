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
package org.neo4j.server.security.auth;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.kernel.api.procedure.Sensitive;

import static java.util.Collections.emptyList;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;
import static org.neo4j.kernel.api.exceptions.Status.Statement.FeatureDeprecationWarning;
import static org.neo4j.kernel.impl.security.User.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.procedure.Mode.DBMS;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class AuthProcedures
{
    @Context
    public SecurityContext securityContext;

    @Context
    public Transaction transaction;

    @Context
    public GraphDatabaseAPI graph;

    @SystemProcedure
    @Deprecated
    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = WRITE, deprecatedBy = "Administration command: CREATE USER" )
    public void createUser(
            @Name( "username" ) String username,
            @Name( "password" ) @Sensitive String password,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws ProcedureException
    {
        var query = String.format( "CREATE USER %s SET PASSWORD '%s' %s", escapeParameter( username ), password == null ? "" : password,
                requirePasswordChange ? "CHANGE REQUIRED" : "CHANGE NOT REQUIRED" );
        runSystemCommand( query, "dbms.security.createUser" );
    }

    @SystemProcedure
    @Deprecated
    @Description( "Delete the specified user." )
    @Procedure( name = "dbms.security.deleteUser", mode = WRITE, deprecatedBy = "Administration command: DROP USER" )
    public void deleteUser( @Name( "username" ) String username ) throws ProcedureException
    {
        var query = String.format( "DROP USER %s", escapeParameter( username ) );
        runSystemCommand( query, "dbms.security.deleteUser" );
    }

    @SystemProcedure
    @Deprecated
    @Description( "Change the current user's password." )
    @Procedure( name = "dbms.security.changePassword", mode = WRITE, deprecatedBy = "Administration command: ALTER CURRENT USER SET PASSWORD" )
    public void changePassword( @Name( "password" ) @Sensitive String password ) throws ProcedureException
    {
        throw new ProcedureException( FeatureDeprecationWarning, "This procedure is no longer available, use: 'ALTER CURRENT USER SET PASSWORD'" );
    }

    @SystemProcedure
    @Description( "Show the current user." )
    @Procedure( name = "dbms.showCurrentUser", mode = DBMS )
    public Stream<UserResult> showCurrentUser()
    {
        securityContext.assertCredentialsNotExpired();
        String username = securityContext.subject().username();
        return Stream.of( new UserResult( username, false ) );
    }

    @SystemProcedure
    @Deprecated
    @Description( "List all native users." )
    @Procedure( name = "dbms.security.listUsers", mode = READ, deprecatedBy = "Administration command: SHOW USERS" )
    public Stream<UserResult> listUsers() throws ProcedureException
    {
        var query = "SHOW USERS";
        List<UserResult> result = new ArrayList<>();

        try
        {
            Result execute = transaction.execute( query );
            execute.accept( row ->
            {
                var username = row.getString( "user" );
                var changeRequired = row.getBoolean( "passwordChangeRequired" );
                result.add( new UserResult( username, changeRequired ) );
                return true;
            } );
        }
        catch ( Exception e )
        {
            translateException( e, "dbms.security.listUsers" );
        }

        if ( result.isEmpty() )
        {
            return showCurrentUser();
        }

        return result.stream();
    }

    private static List<String> changeRequiredList = List.of( PASSWORD_CHANGE_REQUIRED );

    public static class UserResult
    {
        public final String username;
        public final List<String> roles = null; // this is just so that the community version has the same signature as in enterprise
        public final List<String> flags;

        UserResult( String username, boolean changeRequired )
        {
            this.username = username;
            this.flags = changeRequired ? changeRequiredList : emptyList();
        }
    }

    private void runSystemCommand( String query, String procedureName ) throws ProcedureException
    {
        try
        {
            Result execute = transaction.execute( query );
            execute.accept( row -> true );
        }
        catch ( Exception e )
        {
            translateException( e, procedureName );
        }
    }

    private void translateException( Exception e, String procedureName ) throws ProcedureException
    {
        Status status = Status.statusCodeOf( e );
        if ( status != null && status.equals( Status.Statement.NotSystemDatabaseError ) )
        {
            throw new ProcedureException( ProcedureCallFailed, e,
                    String.format( "This is an administration command and it should be executed against the system database: %s", procedureName ) );
        }
        throw new ProcedureException( ProcedureCallFailed, e, e.getMessage() );
    }

    private String escapeParameter( String input )
    {
        return String.format( "`%s`", input == null ? "" : input );
    }
}
