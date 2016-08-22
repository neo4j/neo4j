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

import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

/**
 * A component that can store and retrieve roles. Implementations must be thread safe.
 */
public interface RoleRepository extends Lifecycle
{
    RoleRecord getRoleByName( String roleName );

    Set<String> getRoleNamesByUsername( String username );

    /**
     * Create a role, given that the roles token is unique.
     *
     * @param role the new role object
     * @throws IllegalArgumentException if the role name is not valid or the role name already exists
     */
    void create( RoleRecord role ) throws IllegalArgumentException, IOException;

    /**
     * Update a role, given that the role token is unique.
     *
     * @param existingRole the existing role object, which must match the current state in this repository
     * @param updatedRole the updated role object
     * @throws ConcurrentModificationException if the existingRole does not match the current state in the repository
     */
    void update( RoleRecord existingRole, RoleRecord updatedRole )
            throws ConcurrentModificationException, IOException;

    /**
     * Deletes a role.
     *
     * @param role the role to delete
     * @return true if the role was found and deleted
     */
    boolean delete( RoleRecord role ) throws IOException;

    int numberOfRoles();

    /** Utility for API consumers to tell if #create() will accept a given role name */
    boolean isValidRoleName( String roleName );

    void removeUserFromAllRoles( String username )
            throws ConcurrentModificationException, IOException;

    Set<String> getAllRoleNames();
}
