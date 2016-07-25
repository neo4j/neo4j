/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.auth;

import java.io.IOException;
import java.util.Set;

import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

/**
 * A component that can store and retrieve users. Implementations must be thread safe.
 */
public interface UserRepository extends Lifecycle
{
    User getUserByName( String username );

    /**
     * Create a user, given that the users token is unique.
     * @param user the new user object
     * @throws InvalidArgumentsException if the username is not valid
     * @throws IOException if the underlying storage for users fails
     */
    void create( User user ) throws InvalidArgumentsException, IOException;

    /**
     * Update a user, given that the users token is unique.
     * @param existingUser the existing user object, which must match the current state in this repository
     * @param updatedUser the updated user object
     * @throws ConcurrentModificationException if the existingUser does not match the current state in the repository
     * @throws IOException if the underlying storage for users fails
     * @throws InvalidArgumentsException if the existing and updated users have different names
     */
    void update( User existingUser, User updatedUser )
            throws ConcurrentModificationException, IOException, InvalidArgumentsException;

    /**
     * Deletes a user.
     * @param user the user to delete
     * @throws IOException if the underlying storage for users fails
     * @return true if the user was found and deleted
     */
    boolean delete( User user ) throws IOException;

    int numberOfUsers();

    /** Utility for API consumers to tell if #create() will accept a given username */
    boolean isValidUsername( String username );

    Set<String> getAllUsernames();
}
