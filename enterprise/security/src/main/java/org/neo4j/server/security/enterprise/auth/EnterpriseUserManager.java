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

import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.server.security.auth.UserManager;

public interface EnterpriseUserManager extends UserManager
{
    void setPassword( AuthSubject authSubject, String username, String password ) throws IOException,
            InvalidArgumentsException;

    void suspendUser( String username ) throws IOException, InvalidArgumentsException;

    void activateUser( String username ) throws IOException, InvalidArgumentsException;

    Set<String> getAllUsernames();

    RoleRecord newRole( String roleName, String... usernames ) throws IOException, InvalidArgumentsException;

    RoleRecord getRole( String roleName ) throws InvalidArgumentsException;

    /**
     * Add a user to a role. The role has to exist.
     *
     * @param username name of user
     * @param roleName name of role
     * @throws InvalidArgumentsException if the role does not exist
     * @throws IOException
     */
    void addRoleToUser( String username, String roleName ) throws IOException, InvalidArgumentsException;

    /**
     * Remove a user from a role.
     *
     * @param username name of user
     * @param roleName name of role
     * @throws InvalidArgumentsException if the username or the role does not exist
     * @throws IOException
     */
    void removeRoleFromUser( String username, String roleName ) throws IOException, InvalidArgumentsException;

    Set<String> getAllRoleNames();

    Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException;

    Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException;
}
