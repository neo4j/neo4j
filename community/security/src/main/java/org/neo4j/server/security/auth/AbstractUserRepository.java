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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

public abstract class AbstractUserRepository extends LifecycleAdapter implements UserRepository
{
    /** Quick lookup of users by name */
    protected final Map<String, User> usersByName = new ConcurrentHashMap<>();

    /** Master list of users */
    protected volatile List<User> users = new ArrayList<>();

    private final Pattern usernamePattern = Pattern.compile( "^[a-zA-Z0-9_]+$" );

    @Override
    public User getUserByName( String username )
    {
        return usersByName.get( username );
    }

    @Override
    public void create( User user ) throws InvalidArgumentsException, IOException
    {
        if ( !isValidUsername( user.name() ) )
        {
            throw new InvalidArgumentsException( "'" + user.name() + "' is not a valid user name." );
        }

        synchronized (this)
        {
            // Check for existing user
            for ( User other : users )
            {
                if ( other.name().equals( user.name() ) )
                {
                    throw new InvalidArgumentsException( "The specified user '" + user.name() + "' already exists." );
                }
            }

            users.add( user );

            saveUsers();

            usersByName.put( user.name(), user );
        }
    }

    /**
     * Override this in the implementing class to persist users
     *
     * @throws IOException
     */
    protected abstract void saveUsers() throws IOException;

    @Override
    public void update( User existingUser, User updatedUser )
            throws ConcurrentModificationException, IOException, InvalidArgumentsException
    {
        // Assert input is ok
        if ( !existingUser.name().equals( updatedUser.name() ) )
        {
            throw new IllegalArgumentException( "The attempt to update the role from '" + existingUser.name() +
                    "' to '" + updatedUser.name() + "' failed. Changing a roles name is not allowed." );
        }

        synchronized (this)
        {
            // Copy-on-write for the users list
            List<User> newUsers = new ArrayList<>();
            boolean foundUser = false;
            for ( User other : users )
            {
                if ( other.equals( existingUser ) )
                {
                    foundUser = true;
                    newUsers.add( updatedUser );
                } else
                {
                    newUsers.add( other );
                }
            }

            if ( !foundUser )
            {
                throw new ConcurrentModificationException();
            }

            users = newUsers;

            saveUsers();

            usersByName.put( updatedUser.name(), updatedUser );
        }
    }

    @Override
    public synchronized boolean delete( User user ) throws IOException
    {
        boolean foundUser = false;
        // Copy-on-write for the users list
        List<User> newUsers = new ArrayList<>();
        for ( User other : users )
        {
            if ( other.name().equals( user.name() ) )
            {
                foundUser = true;
            }
            else
            {
                newUsers.add( other );
            }
        }

        if ( foundUser )
        {
            users = newUsers;

            saveUsers();

            usersByName.remove( user.name() );
        }
        return foundUser;
    }

    @Override
    public synchronized int numberOfUsers()
    {
        return users.size();
    }

    @Override
    public boolean isValidUsername( String username )
    {
        return usernamePattern.matcher( username ).matches();
    }

    @Override
    public synchronized Set<String> getAllUsernames()
    {
        return users.stream().map( User::name ).collect( Collectors.toSet() );
    }
}
