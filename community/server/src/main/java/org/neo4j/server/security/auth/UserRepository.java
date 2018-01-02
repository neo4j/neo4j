/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.server.security.auth.exception.IllegalUsernameException;

/**
 * A component that can store and retrieve users. Implementations must be thread safe.
 */
public interface UserRepository
{
    public User findByName( String name );

    /** Create a user, given that the users token is unique.
     * @param user the new user object
     * @throws IllegalUsernameException if the username is not valid
     */
    public void create( User user ) throws IllegalUsernameException, IOException;

    /** Update a user, given that the users token is unique.
     * @param existingUser the existing user object, which must match the current state in this repository
     * @param updatedUser the updated user object
     * @throws ConcurrentModificationException if the existingUser does not match the current state in the repository
     */
    public void update( User existingUser, User updatedUser ) throws ConcurrentModificationException, IOException;

    /** Deletes a user.
     * @param user the user to delete
     * @return true if the user was found and deleted
     */
    public boolean delete( User user ) throws IOException;

    int numberOfUsers();

    /** Utility for API consumers to tell if #save() will accept a given username */
    boolean isValidName( String name );
}
