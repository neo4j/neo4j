/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

/**
 * A component that can store and retrieve users. Implementations must be thread safe.
 */
public interface UserRepository extends Lifecycle
{
    /**
     * Clears all cached user data.
     */
    void clear();

    /**
     * Return the user associated with the given username.
     * @param username the username
     * @return the associated user, or null if no user exists
     */
    User getUserByName( String username );

    /**
     * Create a user, given that the users token is unique.
     * @param user the new user object
     * @throws InvalidArgumentsException if the username is not valid
     * @throws IOException if the underlying storage for users fails
     */
    void create( User user ) throws InvalidArgumentsException, IOException;

    /**
     * Replaces the users in the repository with the given users.
     * @param users the new users
     * @throws InvalidArgumentsException if any username is not valid
     * @throws IOException if the underlying storage for users fails
     */
    void setUsers( ListSnapshot<User> users ) throws InvalidArgumentsException;

    /**
     * Update a user, given that the users token is unique.
     * @param existingUser the existing user object, which must match the current state in this repository
     * @param updatedUser the updated user object
     * @throws ConcurrentModificationException if the existingUser does not match the current state in the repository
     * @throws IOException if the underlying storage for users fails
     * @throws InvalidArgumentsException if the existing and updated users have different names
     */
    void update( User existingUser, User updatedUser )
            throws ConcurrentModificationException, IOException;

    /**
     * Deletes a user.
     * @param user the user to delete
     * @throws IOException if the underlying storage for users fails
     * @return true if the user was found and deleted
     */
    boolean delete( User user ) throws IOException;

    int numberOfUsers();

    /**
     * Asserts whether the given username is valid or not. A valid username is non-null, non-empty, and contains
     * only simple ascii characters.
     * @param username the username to be tested.
     * @throws InvalidArgumentsException if the username was invalid.
     */
    void assertValidUsername( String username ) throws InvalidArgumentsException;

    Set<String> getAllUsernames();

    /**
     * Returns a snapshot of the current persisted user repository
     * @return a snapshot of the current persisted user repository
     * @throws IOException
     */
    ListSnapshot<User> getPersistedSnapshot() throws IOException;
}
