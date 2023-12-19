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

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;

class PersonalUserManager implements EnterpriseUserManager
{
    private final EnterpriseUserManager userManager;
    private final SecurityLog securityLog;
    private final AuthSubject subject;
    private final boolean isUserManager;

    PersonalUserManager( EnterpriseUserManager userManager, AuthSubject subject, SecurityLog securityLog, boolean isUserManager )
    {
        this.userManager = userManager;
        this.securityLog = securityLog;
        this.subject = subject;
        this.isUserManager = isUserManager;
    }

    @Override
    public User newUser( String username, String initialPassword, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            User user = userManager.newUser( username, initialPassword, requirePasswordChange );
            securityLog.info( subject, "created user `%s`%s", username,
                    requirePasswordChange ? ", with password change required" : "" );
            return user;
        }
        catch ( AuthorizationViolationException | IOException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to create user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public void suspendUser( String username )
            throws IOException, InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            if ( subject.hasUsername( username ) )
            {
                throw new InvalidArgumentsException( "Suspending yourself (user '" + username +
                        "') is not allowed." );
            }
            userManager.suspendUser( username );
            securityLog.info( subject, "suspended user `%s`", username );
        }
        catch ( AuthorizationViolationException | IOException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to suspend user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public boolean deleteUser( String username )
            throws IOException, InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            if ( subject.hasUsername( username ) )
            {
                throw new InvalidArgumentsException( "Deleting yourself (user '" + username + "') is not allowed." );
            }
            boolean wasDeleted = userManager.deleteUser( username );
            securityLog.info( subject, "deleted user `%s`", username );
            return wasDeleted;
        }
        catch ( AuthorizationViolationException | IOException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to delete user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public void activateUser( String username, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            if ( subject.hasUsername( username ) )
            {
                throw new InvalidArgumentsException( "Activating yourself (user '" + username + "') is not allowed." );
            }
            userManager.activateUser( username, requirePasswordChange );
            securityLog.info( subject, "activated user `%s`", username );
        }
        catch ( AuthorizationViolationException | IOException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to activate user `%s`: %s", username, e.getMessage() );
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
    public RoleRecord newRole( String roleName, String... usernames )
            throws IOException, InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            RoleRecord newRole = userManager.newRole( roleName, usernames );
            securityLog.info( subject, "created role `%s`", roleName );
            return newRole;
        }
        catch ( AuthorizationViolationException | IOException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to create role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Override
    public boolean deleteRole( String roleName )
            throws IOException, InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            boolean wasDeleted = userManager.deleteRole( roleName );
            securityLog.info( subject, "deleted role `%s`", roleName );
            return wasDeleted;
        }
        catch ( AuthorizationViolationException | IOException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to delete role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Override
    public void setUserPassword( String username, String password, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException, AuthorizationViolationException
    {
        if ( subject.hasUsername( username ) )
        {
            try
            {
                userManager.setUserPassword( username, password, requirePasswordChange );
                securityLog.info( subject, "changed password%s",
                        requirePasswordChange ? ", with password change required" : "" );
            }
            catch ( AuthorizationViolationException | IOException | InvalidArgumentsException e )
            {
                securityLog.error( subject, "tried to change password: %s", e.getMessage() );
                throw e;
            }
        }
        else
        {
            try
            {
                assertUserManager();
                userManager.setUserPassword( username, password, requirePasswordChange );
                securityLog.info( subject, "changed password for user `%s`%s", username,
                        requirePasswordChange ? ", with password change required" : "" );
            }
            catch ( AuthorizationViolationException | IOException | InvalidArgumentsException e )
            {
                securityLog.error( subject, "tried to change password for user `%s`: %s", username,
                        e.getMessage() );
                throw e;
            }
        }
    }

    @Override
    public Set<String> getAllUsernames() throws AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            return userManager.getAllUsernames();
        }
        catch ( AuthorizationViolationException e )
        {
            securityLog.error( subject, "tried to list users: %s", e.getMessage() );
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
    public void addRoleToUser( String roleName, String username )
            throws IOException, InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            userManager.addRoleToUser( roleName, username );
            securityLog.info( subject, "added role `%s` to user `%s`", roleName, username );
        }
        catch ( AuthorizationViolationException | IOException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to add role `%s` to user `%s`: %s", roleName, username,
                    e.getMessage() );
            throw e;
        }
    }

    @Override
    public void removeRoleFromUser( String roleName, String username )
            throws IOException, InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            if ( subject.hasUsername( username ) && roleName.equals( PredefinedRoles.ADMIN ) )
            {
                throw new InvalidArgumentsException(
                        "Removing yourself (user '" + username + "') from the admin role is not allowed." );
            }
            userManager.removeRoleFromUser( roleName, username );
            securityLog.info( subject, "removed role `%s` from user `%s`", roleName, username );
        }
        catch ( AuthorizationViolationException | IOException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to remove role `%s` from user `%s`: %s", roleName, username, e
                    .getMessage() );
            throw e;
        }
    }

    @Override
    public Set<String> getAllRoleNames() throws AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            return userManager.getAllRoleNames();
        }
        catch ( AuthorizationViolationException e )
        {
            securityLog.error( subject, "tried to list roles: %s", e.getMessage() );
            throw e;
        }
    }

    @Override
    public Set<String> getRoleNamesForUser( String username )
            throws InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertSelfOrUserManager( username );
            return userManager.getRoleNamesForUser( username );
        }
        catch ( AuthorizationViolationException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to list roles for user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public Set<String> silentlyGetRoleNamesForUser( String username )
    {
        return userManager.silentlyGetRoleNamesForUser( username );
    }

    @Override
    public Set<String> getUsernamesForRole( String roleName )
            throws InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            return userManager.getUsernamesForRole( roleName );
        }
        catch ( AuthorizationViolationException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to list users for role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Override
    public Set<String> silentlyGetUsernamesForRole( String roleName )
    {
        return userManager.silentlyGetUsernamesForRole( roleName );
    }

    private void assertSelfOrUserManager( String username )
    {
        if ( !subject.hasUsername( username ) )
        {
            assertUserManager();
        }
    }

    private void assertUserManager() throws AuthorizationViolationException
    {
        if ( !isUserManager )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }
}
