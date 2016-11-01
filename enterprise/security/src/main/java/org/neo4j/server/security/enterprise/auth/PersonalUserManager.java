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
import java.util.Set;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;

class PersonalUserManager implements EnterpriseUserManager
{
    private final EnterpriseUserManager userManager;
    private final SecurityContext securityContext;
    private final AuthSubject authSubject;
    private final SecurityLog securityLog;

    PersonalUserManager( EnterpriseUserManager userManager, SecurityContext securityContext, SecurityLog securityLog )
    {
        this.userManager = userManager;
        this.securityContext = securityContext;
        this.authSubject = securityContext.subject();
        this.securityLog = securityLog;
    }

    @Override
    public User newUser( String username, String initialPassword, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        try
        {
            assertAdmin();
            User user = userManager.newUser( username, initialPassword, requirePasswordChange );
            securityLog.info( authSubject, "created user `%s`%s", username,
                    requirePasswordChange ? ", with password change required" : "" );
            return user;
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to create user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public void suspendUser( String username ) throws IOException, InvalidArgumentsException
    {
        try
        {
            assertAdmin();
            if ( authSubject.hasUsername( username ) )
            {
                throw new InvalidArgumentsException( "Suspending yourself (user '" + username +
                        "') is not allowed." );
            }
            userManager.suspendUser( username );
            securityLog.info( authSubject, "suspended user `%s`", username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to suspend user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public boolean deleteUser( String username ) throws IOException, InvalidArgumentsException
    {
        try
        {
            assertAdmin();
            if ( authSubject.hasUsername( username ) )
            {
                throw new InvalidArgumentsException( "Deleting yourself (user '" + username + "') is not allowed." );
            }
            boolean wasDeleted = userManager.deleteUser( username );
            securityLog.info( authSubject, "deleted user `%s`", username );
            return wasDeleted;
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to delete user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public void activateUser( String username, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        try
        {
            assertAdmin();
            if ( authSubject.hasUsername( username ) )
            {
                throw new InvalidArgumentsException( "Activating yourself (user '" + username + "') is not allowed." );
            }
            userManager.activateUser( username, requirePasswordChange );
            securityLog.info( authSubject, "activated user `%s`", username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to activate user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public User getUser( String username ) throws InvalidArgumentsException
    {
        return userManager.getUser( username );
    }

    @Override
    public User silentlyGetUser( String username )
    {
        return userManager.silentlyGetUser( username );
    }

    @Override
    public RoleRecord newRole( String roleName, String... usernames ) throws IOException, InvalidArgumentsException
    {
        try
        {
            assertAdmin();
            RoleRecord newRole = userManager.newRole( roleName, usernames );
            securityLog.info( authSubject, "created role `%s`", roleName );
            return newRole;
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to create role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Override
    public boolean deleteRole( String roleName ) throws IOException, InvalidArgumentsException
    {
        try
        {
            assertAdmin();
            boolean wasDeleted = userManager.deleteRole( roleName );
            securityLog.info( authSubject, "deleted role `%s`", roleName );
            return wasDeleted;
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to delete role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Override
    public void setUserPassword( String username, String password, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        if ( authSubject.hasUsername( username ) )
        {
            try
            {
                userManager.setUserPassword( username, password, requirePasswordChange );
                securityLog.info( authSubject, "changed password%s",
                        requirePasswordChange ? ", with password change required" : "" );
            }
            catch ( Exception e )
            {
                securityLog.error( authSubject, "tried to change password: %s", e.getMessage() );
                throw e;
            }
        }
        else
        {
            try
            {
                assertAdmin();
                userManager.setUserPassword( username, password, requirePasswordChange );
                securityLog.info( authSubject, "changed password for user `%s`%s", username,
                        requirePasswordChange ? ", with password change required" : "" );
            }
            catch ( Exception e )
            {
                securityLog.error( authSubject, "tried to change password for user `%s`: %s", username,
                        e.getMessage() );
                throw e;
            }
        }
    }

    @Override
    public Set<String> getAllUsernames()
    {
        try
        {
            assertAdmin();
            return userManager.getAllUsernames();
        }
        catch ( AuthorizationViolationException e )
        {
            securityLog.error( authSubject, "tried to list users: %s", e.getMessage() );
            throw e;
        }
    }

    @Override
    public RoleRecord getRole( String roleName ) throws InvalidArgumentsException
    {
        return userManager.getRole( roleName );
    }

    @Override
    public RoleRecord silentlyGetRole( String roleName )
    {
        return userManager.silentlyGetRole( roleName );
    }

    @Override
    public void addRoleToUser( String roleName, String username ) throws IOException, InvalidArgumentsException
    {
        try
        {
            assertAdmin();
            userManager.addRoleToUser( roleName, username );
            securityLog.info( authSubject, "added role `%s` to user `%s`", roleName, username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to add role `%s` to user `%s`: %s", roleName, username,
                    e.getMessage() );
            throw e;
        }
    }

    @Override
    public void removeRoleFromUser( String roleName, String username ) throws IOException, InvalidArgumentsException
    {
        try
        {
            assertAdmin();
            if ( authSubject.hasUsername( username ) && roleName.equals( PredefinedRoles.ADMIN ) )
            {
                throw new InvalidArgumentsException(
                        "Removing yourself (user '" + username + "') from the admin role is not allowed." );
            }
            userManager.removeRoleFromUser( roleName, username );
            securityLog.info( authSubject, "removed role `%s` from user `%s`", roleName, username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to remove role `%s` from user `%s`: %s", roleName, username, e
                    .getMessage() );
            throw e;
        }
    }

    @Override
    public Set<String> getAllRoleNames()
    {
        try
        {
            assertAdmin();
            return userManager.getAllRoleNames();
        }
        catch ( AuthorizationViolationException e )
        {
            securityLog.error( authSubject, "tried to list roles: %s", e.getMessage() );
            throw e;
        }
    }

    @Override
    public Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
    {
        try
        {
            assertSelfOrAdmin( username );
            return userManager.getRoleNamesForUser( username );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to list roles for user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public Set<String> silentlyGetRoleNamesForUser( String username )
    {
        return userManager.silentlyGetRoleNamesForUser( username );
    }

    @Override
    public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        try
        {
            assertAdmin();
            return userManager.getUsernamesForRole( roleName );
        }
        catch ( Exception e )
        {
            securityLog.error( authSubject, "tried to list users for role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Override
    public Set<String> silentlyGetUsernamesForRole( String roleName )
    {
        return userManager.silentlyGetUsernamesForRole( roleName );
    }

    private void assertSelfOrAdmin( String username )
    {
        if ( !authSubject.hasUsername( username ) )
        {
            assertAdmin();
        }
    }

    private void assertAdmin() throws AuthorizationViolationException
    {
        if ( !securityContext.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }
}
