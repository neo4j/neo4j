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

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserManager;

import static java.util.Collections.emptySet;

public interface EnterpriseUserManager extends UserManager
{
    void suspendUser( String username ) throws IOException, InvalidArgumentsException;

    void activateUser( String username, boolean requirePasswordChange ) throws IOException, InvalidArgumentsException;

    RoleRecord newRole( String roleName, String... usernames ) throws IOException, InvalidArgumentsException;

    boolean deleteRole( String roleName ) throws IOException, InvalidArgumentsException;

    RoleRecord getRole( String roleName ) throws InvalidArgumentsException;

    RoleRecord silentlyGetRole( String roleName );

    /**
     * Assign a role to a user. The role and the user have to exist.
     *
     * @param roleName name of role
     * @param username name of user
     * @throws InvalidArgumentsException if the role does not exist
     * @throws IOException
     */
    void addRoleToUser( String roleName, String username ) throws IOException, InvalidArgumentsException;

    /**
     * Unassign a role from a user. The role and the user have to exist.
     *
     * @param roleName name of role
     * @param username name of user
     * @throws InvalidArgumentsException if the username or the role does not exist
     * @throws IOException
     */
    void removeRoleFromUser( String roleName, String username ) throws IOException, InvalidArgumentsException;

    Set<String> getAllRoleNames();

    Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException;

    Set<String> silentlyGetRoleNamesForUser( String username );

    Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException;

    Set<String> silentlyGetUsernamesForRole( String roleName );

    EnterpriseUserManager NOOP = new EnterpriseUserManager()
    {
        @Override
        public void suspendUser( String username ) throws IOException, InvalidArgumentsException
        {
        }

        @Override
        public void activateUser( String username, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
        }

        @Override
        public RoleRecord newRole( String roleName, String... usernames ) throws IOException, InvalidArgumentsException
        {
            return null;
        }

        @Override
        public boolean deleteRole( String roleName ) throws IOException, InvalidArgumentsException
        {
            return false;
        }

        @Override
        public RoleRecord getRole( String roleName ) throws InvalidArgumentsException
        {
            return null;
        }

        @Override
        public RoleRecord silentlyGetRole( String roleName )
        {
            return null;
        }

        @Override
        public void addRoleToUser( String roleName, String username ) throws IOException, InvalidArgumentsException
        {
        }

        @Override
        public void removeRoleFromUser( String roleName, String username ) throws IOException, InvalidArgumentsException
        {
        }

        @Override
        public Set<String> getAllRoleNames()
        {
            return emptySet();
        }

        @Override
        public Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
        {
            return emptySet();
        }

        @Override
        public Set<String> silentlyGetRoleNamesForUser( String username )
        {
            return emptySet();
        }

        @Override
        public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
        {
            return emptySet();
        }

        @Override
        public Set<String> silentlyGetUsernamesForRole( String roleName )
        {
            return emptySet();
        }

        @Override
        public User newUser( String username, String initialPassword, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
            return null;
        }

        @Override
        public boolean deleteUser( String username ) throws IOException, InvalidArgumentsException
        {
            return false;
        }

        @Override
        public User getUser( String username ) throws InvalidArgumentsException
        {
            return null;
        }

        @Override
        public User silentlyGetUser( String username )
        {
            return null;
        }

        @Override
        public void setUserPassword( String username, String password, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
        }

        @Override
        public Set<String> getAllUsernames()
        {
            return emptySet();
        }
    };
}
