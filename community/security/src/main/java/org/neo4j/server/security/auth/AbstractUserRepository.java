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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

import static org.neo4j.helpers.collection.MapUtil.trimToList;

public abstract class AbstractUserRepository extends LifecycleAdapter implements UserRepository
{
    /** Quick lookup of users by name */
    private final Map<String,User> usersByName = new ConcurrentHashMap<>();

    /** Master list of users */
    protected volatile List<User> users = new ArrayList<>();
    protected AtomicLong lastLoaded = new AtomicLong( 0L );

    // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
    private final Pattern usernamePattern = Pattern.compile( "^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$" );

    @Override
    public void clear()
    {
        users.clear();
        usersByName.clear();
    }

    @Override
    public User getUserByName( String username )
    {
        return username == null ? null : usersByName.get( username );
    }

    @Override
    public void create( User user ) throws InvalidArgumentsException, IOException
    {
        assertValidUsername( user.name() );

        synchronized ( this )
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
            usersByName.put( user.name(), user );
            persistUsers();
        }
    }

    @Override
    public void setUsers( ListSnapshot<User> usersSnapshot ) throws InvalidArgumentsException
    {
        for ( User user : usersSnapshot.values() )
        {
            assertValidUsername( user.name() );
        }

        synchronized ( this )
        {
            users.clear();

            this.users.addAll( usersSnapshot.values() );
            this.lastLoaded.set( usersSnapshot.timestamp() );

            trimToList( usersByName, users, User::name );

            for ( User user : users )
            {
                usersByName.put( user.name(), user );
            }
        }
    }

    @Override
    public void update( User existingUser, User updatedUser )
            throws ConcurrentModificationException, IOException
    {
        // Assert input is ok
        if ( !existingUser.name().equals( updatedUser.name() ) )
        {
            throw new IllegalArgumentException( "The attempt to update the role from '" + existingUser.name() +
                    "' to '" + updatedUser.name() + "' failed. Changing a roles name is not allowed." );
        }

        synchronized ( this )
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
                }
                else
                {
                    newUsers.add( other );
                }
            }

            if ( !foundUser )
            {
                throw new ConcurrentModificationException();
            }

            users = newUsers;
            usersByName.put( updatedUser.name(), updatedUser );
            persistUsers();
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
            usersByName.remove( user.name() );
            persistUsers();
        }
        return foundUser;
    }

    @Override
    public synchronized int numberOfUsers()
    {
        return users.size();
    }

    @Override
    public void assertValidUsername( String username ) throws InvalidArgumentsException
    {
        if ( username == null || username.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided username is empty." );
        }
        if ( !usernamePattern.matcher( username ).matches() )
        {
            throw new InvalidArgumentsException(
                    "Username '" + username +
                    "' contains illegal characters. Use ascii characters that are not ',', ':' or whitespaces." );
        }
    }

    @Override
    public synchronized Set<String> getAllUsernames()
    {
        return users.stream().map( User::name ).collect( Collectors.toSet() );
    }

    /**
     * Override this in the implementing class to persist users
     *
     * @throws IOException
     */
    protected abstract void persistUsers() throws IOException;

    /**
     * Override this in the implementing class to load users
     *
     * @return a timestamped snapshot of users, or null if the backing file did not exist
     * @throws IOException
     */
    protected abstract ListSnapshot<User> readPersistedUsers() throws IOException;
}
