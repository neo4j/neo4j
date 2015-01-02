/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.server.security.auth.exception.IllegalTokenException;
import org.neo4j.server.security.auth.exception.IllegalUsernameException;

/**
 * A component that can store and retrieve users. Implementations must be thread safe.
 */
public interface UserRepository extends Iterable<User>
{
    public User get( String name );

    /** Saves a user, given that the users token is unique. */
    public void save( User user ) throws IllegalTokenException, IOException, IllegalUsernameException;

    int numberOfUsers();

    /** Utility for API consumers to tell if #save() will accept a given username */
    boolean isValidName( String name );

    /** Utility for API consumers to tell if #save() will accept a given token */
    boolean isValidToken( String token );
}
