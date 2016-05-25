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

public interface RoleManager
{
    /**
     * Add a user to a role. The role has to exist.
     *
     * @param username
     * @param roleName
     * @throws IllegalArgumentException if the role does not exist
     * @throws IOException
     */
    void addUserToRole( String username, String roleName ) throws IOException;

    /**
     * Remove a user from a role.
     *
     * @param username
     * @param roleName
     * @throws IllegalArgumentException if the username or the role does not exist
     * @throws IOException
     */
    void removeUserFromRole( String username, String roleName ) throws IOException;
}
