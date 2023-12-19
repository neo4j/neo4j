/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class UserManagementProcedures extends AuthProceduresBase
{

    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = DBMS )
    public void createUser( @Name( "username" ) String username, @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        userManager.newUser( username, password, requirePasswordChange );
    }

    @Deprecated
    @Description( "Change the current user's password. Deprecated by dbms.security.changePassword." )
    @Procedure( name = "dbms.changePassword", mode = DBMS, deprecatedBy = "dbms.security.changePassword" )
    public void changePasswordDeprecated( @Name( "password" ) String password )
            throws InvalidArgumentsException, IOException
    {
        changePassword( password, false );
    }

    @Description( "Change the current user's password." )
    @Procedure( name = "dbms.security.changePassword", mode = DBMS )
    public void changePassword( @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "false" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        setUserPassword( securityContext.subject().username(), password, requirePasswordChange );
    }

    @Description( "Change the given user's password." )
    @Procedure( name = "dbms.security.changeUserPassword", mode = DBMS )
    public void changeUserPassword( @Name( "username" ) String username, @Name( "newPassword" ) String newPassword,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        setUserPassword( username, newPassword, requirePasswordChange );
    }

    @Description( "Assign a role to the user." )
    @Procedure( name = "dbms.security.addRoleToUser", mode = DBMS )
    public void addRoleToUser( @Name( "roleName" ) String roleName, @Name( "username" ) String username )
            throws IOException, InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        userManager.addRoleToUser( roleName, username );
    }

    @Description( "Unassign a role from the user." )
    @Procedure( name = "dbms.security.removeRoleFromUser", mode = DBMS )
    public void removeRoleFromUser( @Name( "roleName" ) String roleName, @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        userManager.removeRoleFromUser( roleName, username );
    }

    @Description( "Delete the specified user." )
    @Procedure( name = "dbms.security.deleteUser", mode = DBMS )
    public void deleteUser( @Name( "username" ) String username ) throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        if ( userManager.deleteUser( username ) )
        {
            kickoutUser( username, "deletion" );
        }
    }

    @Description( "Suspend the specified user." )
    @Procedure( name = "dbms.security.suspendUser", mode = DBMS )
    public void suspendUser( @Name( "username" ) String username ) throws IOException, InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        userManager.suspendUser( username );
        kickoutUser( username, "suspension" );
    }

    @Description( "Activate a suspended user." )
    @Procedure( name = "dbms.security.activateUser", mode = DBMS )
    public void activateUser( @Name( "username" ) String username,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        userManager.activateUser( username, requirePasswordChange );
    }

    @Description( "List all native users." )
    @Procedure( name = "dbms.security.listUsers", mode = DBMS )
    public Stream<UserResult> listUsers()
    {
        securityContext.assertCredentialsNotExpired();
        Set<String> users = userManager.getAllUsernames();
        if ( users.isEmpty() )
        {
            return Stream.of( userResultForSubject() );
        }
        else
        {
            return users.stream().map( this::userResultForName );
        }
    }

    @Description( "List all available roles." )
    @Procedure( name = "dbms.security.listRoles", mode = DBMS )
    public Stream<RoleResult> listRoles()
    {
        securityContext.assertCredentialsNotExpired();
        Set<String> roles = userManager.getAllRoleNames();
        return roles.stream().map( this::roleResultForName );
    }

    @Description( "List all roles assigned to the specified user." )
    @Procedure( name = "dbms.security.listRolesForUser", mode = DBMS )
    public Stream<StringResult> listRolesForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        return userManager.getRoleNamesForUser( username ).stream().map( StringResult::new );
    }

    @Description( "List all users currently assigned the specified role." )
    @Procedure( name = "dbms.security.listUsersForRole", mode = DBMS )
    public Stream<StringResult> listUsersForRole( @Name( "roleName" ) String roleName )
            throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        return userManager.getUsernamesForRole( roleName ).stream().map( StringResult::new );
    }

    @Description( "Create a new role." )
    @Procedure( name = "dbms.security.createRole", mode = DBMS )
    public void createRole( @Name( "roleName" ) String roleName ) throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        userManager.newRole( roleName );
    }

    @Description( "Delete the specified role. Any role assignments will be removed." )
    @Procedure( name = "dbms.security.deleteRole", mode = DBMS )
    public void deleteRole( @Name( "roleName" ) String roleName ) throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        userManager.deleteRole( roleName );
    }

    private void setUserPassword( String username, String newPassword, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        userManager.setUserPassword( username, newPassword, requirePasswordChange );
        if ( securityContext.subject().hasUsername( username ) )
        {
            securityContext.subject().setPasswordChangeNoLongerRequired();
        }
    }
}
