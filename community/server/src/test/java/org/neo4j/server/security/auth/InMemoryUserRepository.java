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

import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.server.security.auth.exception.IllegalTokenException;

/** A user repository implementation that just stores users in memory */
public class InMemoryUserRepository implements UserRepository
{
    private final ConcurrentHashMap<String, User> users = new ConcurrentHashMap<>();

    @Override
    public User findByName( String name )
    {
        return users.get( name );
    }

    @Override
    public User findByToken( String token )
    {
        for ( User user : users.values() )
        {
            if ( user.token().equals( token ) )
            {
                return user;
            }
        }
        return null;
    }

    /** This is synchronized to ensure we can't have users with duplicate tokens. */
    public synchronized void save( User user ) throws IllegalTokenException
    {
        for ( User other : users.values() )
        {
            if ( other.token().equals( user.token() ) && !other.name().equals( user.name() ) )
            {
                throw new IllegalTokenException( "Unable to set token, because the chosen token is already in use." );
            }
        }

        users.put( user.name(), user );
    }

    @Override
    public int numberOfUsers()
    {
        return users.size();
    }

    @Override
    public boolean isValidName( String name )
    {
        // This repo can store any name
        return true;
    }

    @Override
    public boolean isValidToken( String token )
    {
        // This repo can store any token
        return true;
    }
}
