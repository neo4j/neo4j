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
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthSubject;
import org.neo4j.kernel.impl.enterprise.SecurityLog;
import org.neo4j.server.security.auth.User;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;

public class PersonalUserManager implements EnterpriseUserManager
{
    private EnterpriseUserManager userManager;
    private AuthSubject authSubject;
    private SecurityLog securityLog;

    public PersonalUserManager( EnterpriseUserManager userManager, AuthSubject authSubject, SecurityLog securityLog )
    {
        this.userManager = userManager;
        this.authSubject = authSubject;
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

    private void assertAdmin() throws InvalidArgumentsException
    {
        if ( authSubject instanceof EnterpriseAuthSubject )
        {
            if ( ((EnterpriseAuthSubject) authSubject).isAdmin() )
            {
                return;
            }
        }
        throw new AuthorizationViolationException( PERMISSION_DENIED );
    }

    @Override
    public void suspendUser( String username ) throws IOException, InvalidArgumentsException
    {
        userManager.suspendUser( username );
    }

    @Override
    public boolean deleteUser( String username ) throws IOException, InvalidArgumentsException
    {
        return userManager.deleteUser( username );
    }

    @Override
    public void activateUser( String username, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        userManager.activateUser( username, requirePasswordChange );
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
        return userManager.newRole( roleName, usernames );
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
    public boolean deleteRole( String roleName ) throws IOException, InvalidArgumentsException
    {
        return userManager.deleteRole( roleName );
    }

    @Override
    public Set<String> getAllUsernames()
    {
        return userManager.getAllUsernames();
    }

    @Override
    public RoleRecord getRole( String roleName ) throws InvalidArgumentsException
    {
        return userManager.getRole( roleName );
    }

    @Override
    public void addRoleToUser( String roleName, String username ) throws IOException, InvalidArgumentsException
    {
        userManager.addRoleToUser( roleName, username );
    }

    @Override
    public void removeRoleFromUser( String roleName, String username ) throws IOException, InvalidArgumentsException
    {
        userManager.removeRoleFromUser( roleName, username );
    }

    @Override
    public Set<String> getAllRoleNames()
    {
        return userManager.getAllRoleNames();
    }

    @Override
    public Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
    {
        return userManager.getRoleNamesForUser( username );
    }

    @Override
    public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        return userManager.getUsernamesForRole( roleName );
    }
}
