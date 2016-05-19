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

import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

/**
 * A component that can store and retrieve groups. Implementations must be thread safe.
 */
public interface GroupRepository extends Lifecycle
{
    GroupRecord findByName( String name );

    Set<String> findByUsername( String username );

    /**
     * Create a group, given that the groups token is unique.
     * @param group the new group object
     * @throws IllegalCredentialsException if the group name is not valid
     */
    void create( GroupRecord group ) throws IllegalCredentialsException, IOException;

    /**
     * Update a group, given that the group token is unique.
     * @param existingGroup the existing group object, which must match the current state in this repository
     * @param updatedGroup the updated group object
     * @throws ConcurrentModificationException if the existingGroup does not match the current state in the repository
     */
    void update( GroupRecord existingGroup, GroupRecord updatedGroup ) throws ConcurrentModificationException, IOException;

    /**
     * Deletes a group.
     * @param group the group to delete
     * @return true if the group was found and deleted
     */
    boolean delete( GroupRecord group ) throws IOException;

    int numberOfGroups();

    /** Utility for API consumers to tell if #create() will accept a given group name */
    boolean isValidName( String name );
}
