/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class UserManagementProcedures extends AuthProceduresBase
{
    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = DBMS )
    public void createUser( @Name( "username" ) String username, @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            adminSubject.getUserManager().newUser( username, password, requirePasswordChange );
            securityLog.info( authSubject, "created user `%s`%s", username,
                    requirePasswordChange ? ", with password change required" : "" );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to create user `%s`: %s", username, e.getMessage() );
            throw e;
        }
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
    public void changePassword(
            @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "false"  ) boolean requirePasswordChange
    )
            throws InvalidArgumentsException, IOException
    {
        // logging is handled by subject
        StandardEnterpriseAuthSubject enterpriseSubject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
        enterpriseSubject.setPassword( password, requirePasswordChange );
    }

    @Description( "Change the given user's password." )
    @Procedure( name = "dbms.security.changeUserPassword", mode = DBMS )
    public void changeUserPassword( @Name( "username" ) String username, @Name( "newPassword" ) String newPassword,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        if ( authSubject.hasUsername( username ) )
        {
            changePassword( newPassword, requirePasswordChange );
        }
        else
        {
            StandardEnterpriseAuthSubject enterpriseSubject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
            try
            {
                if ( !enterpriseSubject.isAdmin() )
                {
                    throw new AuthorizationViolationException( PERMISSION_DENIED );
                }
                else
                {
                    enterpriseSubject.getUserManager().setUserPassword( username, newPassword, requirePasswordChange );
                    securityLog.info( authSubject, "changed password for user `%s`%s", username,
                            requirePasswordChange ? ", with password change required" : "" );
                }
            }
            catch ( Exception e )
            {
                securityLog.error( authSubject, "tried to change password for user `%s`: %s",
                        username, e.getMessage() );
                throw e;
            }
        }
    }

    @Description( "Assign a role to the user." )
    @Procedure( name = "dbms.security.addRoleToUser", mode = DBMS )
    public void addRoleToUser( @Name( "roleName" ) String roleName, @Name( "username" ) String username )
            throws IOException, InvalidArgumentsException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            adminSubject.getUserManager().addRoleToUser( roleName, username );
            securityLog.info( authSubject, "added role `%s` to user `%s`", roleName, username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to add role `%s` to user `%s`: %s",
                    roleName, username, e.getMessage() );
            throw e;
        }
    }

    @Description( "Unassign a role from the user." )
    @Procedure( name = "dbms.security.removeRoleFromUser", mode = DBMS )
    public void removeRoleFromUser( @Name( "roleName" ) String roleName, @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            if ( adminSubject.hasUsername( username ) && roleName.equals( PredefinedRoles.ADMIN ) )
            {
                throw new InvalidArgumentsException(
                        "Removing yourself (user '" + username + "') from the admin role is not allowed." );
            }
            adminSubject.getUserManager().removeRoleFromUser( roleName, username );
            securityLog.info( authSubject, "removed role `%s` from user `%s`", roleName, username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to remove role `%s` from user `%s`: %s", roleName, username, e
                    .getMessage() );
            throw e;
        }
    }

    @Description( "Delete the specified user." )
    @Procedure( name = "dbms.security.deleteUser", mode = DBMS )
    public void deleteUser( @Name( "username" ) String username ) throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            if ( adminSubject.hasUsername( username ) )
            {
                throw new InvalidArgumentsException( "Deleting yourself (user '" + username + "') is not allowed." );
            }
            adminSubject.getUserManager().deleteUser( username );
            securityLog.info( authSubject, "deleted user `%s`", username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to delete user `%s`: %s", username, e.getMessage() );
            throw e;
        }

        kickoutUser( username, "deletion" );
    }

    private void kickoutUser( String username, String reason )
    {
        try
        {
            terminateTransactionsForValidUser( username );
            terminateConnectionsForValidUser( username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "failed to terminate running transaction and bolt connections for " +
                    "user `%s` following %s: %s", username, reason, e.getMessage() );
            throw e;
        }
    }

    @Description( "Suspend the specified user." )
    @Procedure( name = "dbms.security.suspendUser", mode = DBMS )
    public void suspendUser( @Name( "username" ) String username ) throws IOException, InvalidArgumentsException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            if ( adminSubject.hasUsername( username ) )
            {
                throw new InvalidArgumentsException( "Suspending yourself (user '" + username +
                        "') is not allowed." );
            }
            adminSubject.getUserManager().suspendUser( username );
            securityLog.info( authSubject, "suspended user `%s`", username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to suspend user `%s`: %s", username, e.getMessage() );
            throw e;
        }

        kickoutUser( username, "suspension" );
    }

    @Description( "Activate a suspended user." )
    @Procedure( name = "dbms.security.activateUser", mode = DBMS )
    public void activateUser( @Name( "username" ) String username,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            if ( adminSubject.hasUsername( username ) )
            {
                throw new InvalidArgumentsException( "Activating yourself (user '" + username +
                        "') is not allowed." );
            }
            adminSubject.getUserManager().activateUser( username, requirePasswordChange );
            securityLog.info( authSubject, "activated user `%s`", username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to activate user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Description( "List all local users." )
    @Procedure( name = "dbms.security.listUsers", mode = DBMS )
    public Stream<UserResult> listUsers() throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            EnterpriseUserManager userManager = adminSubject.getUserManager();
            Set<String> users = userManager.getAllUsernames();
            List<UserResult> results = new ArrayList<>();
            for ( String u : users )
            {
                results.add(
                        new UserResult( u, userManager.getRoleNamesForUser( u ), userManager.getUser( u ).getFlags() ) );
            }
            return results.stream();
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to list users: %s", e.getMessage() );
            throw e;
        }
    }

    @Description( "List all available roles." )
    @Procedure( name = "dbms.security.listRoles", mode = DBMS )
    public Stream<RoleResult> listRoles() throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            EnterpriseUserManager userManager = adminSubject.getUserManager();
            Set<String> roles = userManager.getAllRoleNames();
            List<RoleResult> results = new ArrayList<>();
            for ( String r : roles )
            {
                results.add( new RoleResult( r, userManager.getUsernamesForRole( r ) ) );
            }
            return results.stream();
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to list roles: %s", e.getMessage() );
            throw e;
        }
    }

    @Description( "List all roles assigned to the specified user." )
    @Procedure( name = "dbms.security.listRolesForUser", mode = DBMS )
    public Stream<StringResult> listRolesForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject subject = ensureSelfOrAdminAuthSubject( username );
            return subject.getUserManager().getRoleNamesForUser( username ).stream().map( StringResult::new );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to list roles for user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Description( "List all users currently assigned the specified role." )
    @Procedure( name = "dbms.security.listUsersForRole", mode = DBMS )
    public Stream<StringResult> listUsersForRole( @Name( "roleName" ) String roleName )
            throws InvalidArgumentsException, IOException
    {
        try
        {
            StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
            return adminSubject.getUserManager().getUsernamesForRole( roleName ).stream().map( StringResult::new );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to list users for role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Description( "Create a new role." )
    @Procedure( name = "dbms.security.createRole", mode = DBMS )
    public void createRole( @Name( "roleName" ) String roleName ) throws InvalidArgumentsException, IOException
    {
        try
        {
            ensureAdminAuthSubject().getUserManager().newRole( roleName );
            securityLog.info( authSubject, "created role `%s`", roleName );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to create role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Description( "Delete the specified role. Any role assignments will be removed." )
    @Procedure( name = "dbms.security.deleteRole", mode = DBMS )
    public void deleteRole( @Name( "roleName" ) String roleName ) throws InvalidArgumentsException, IOException
    {
        try
        {
            ensureAdminAuthSubject().getUserManager().deleteRole( roleName );
            securityLog.info( authSubject, "deleted role `%s`", roleName );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to delete role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }
}
