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
package org.neo4j.kernel.api.security;

import java.io.IOException;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;

public interface UserManager
{
    String INITIAL_USER_NAME = "neo4j";

    User newUser( String username, String initialPassword, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException;

    boolean deleteUser( String username ) throws IOException, InvalidArgumentsException;

    User getUser( String username ) throws InvalidArgumentsException;

    User silentlyGetUser( String username );

    void setUserPassword( String username, String password, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException;

    Set<String> getAllUsernames();

    UserManager NO_AUTH = new UserManager()
    {
        @Override
        public User newUser( String username, String initialPassword, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
            return null;
        }

        @Override
        public boolean deleteUser( String username ) throws IOException, InvalidArgumentsException
        {
            return false;
        }

        @Override
        public User getUser( String username ) throws InvalidArgumentsException
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
                throws IOException, InvalidArgumentsException
        {
        }

        @Override
        public Set<String> getAllUsernames()
        {
            return null;
        }
    };
}
