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

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.kernel.api.security.UserManager;

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
        public void suspendUser( String username )
        {
        }

        @Override
        public void activateUser( String username, boolean requirePasswordChange )
        {
        }

        @Override
        public RoleRecord newRole( String roleName, String... usernames )
        {
            return null;
        }

        @Override
        public boolean deleteRole( String roleName )
        {
            return false;
        }

        @Override
        public RoleRecord getRole( String roleName )
        {
            return null;
        }

        @Override
        public RoleRecord silentlyGetRole( String roleName )
        {
            return null;
        }

        @Override
        public void addRoleToUser( String roleName, String username )
        {
        }

        @Override
        public void removeRoleFromUser( String roleName, String username )
        {
        }

        @Override
        public Set<String> getAllRoleNames()
        {
            return emptySet();
        }

        @Override
        public Set<String> getRoleNamesForUser( String username )
        {
            return emptySet();
        }

        @Override
        public Set<String> silentlyGetRoleNamesForUser( String username )
        {
            return emptySet();
        }

        @Override
        public Set<String> getUsernamesForRole( String roleName )
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
        {
            return null;
        }

        @Override
        public boolean deleteUser( String username )
        {
            return false;
        }

        @Override
        public User getUser( String username )
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
        {
        }

        @Override
        public Set<String> getAllUsernames()
        {
            return emptySet();
        }
    };
}
