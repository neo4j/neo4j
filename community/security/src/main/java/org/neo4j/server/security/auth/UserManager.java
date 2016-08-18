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

public interface UserManager
{
    User newUser( String username, String initialPassword, boolean requirePasswordChange ) throws IOException,
            InvalidArgumentsException;

    boolean deleteUser( String username ) throws IOException, InvalidArgumentsException;

    User getUser( String username ) throws InvalidArgumentsException;

    void setUserPassword( String username, String password ) throws IOException, InvalidArgumentsException;

    Set<String> getAllUsernames();
}
