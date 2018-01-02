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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.server.security.auth.exception.IllegalUsernameException;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Stores user auth data. In memory, but backed by persistent storage so changes to this repository will survive
 * JVM restarts and crashes.
 */
public class FileUserRepository extends LifecycleAdapter implements UserRepository
{
    private final Path authFile;

    /** Quick lookup of users by name */
    private final Map<String, User> usersByName = new ConcurrentHashMap<>();
    private final Log log;

    /** Master list of users */
    private volatile List<User> users = new ArrayList<>();

    private final UserSerialization serialization = new UserSerialization();

    public FileUserRepository( Path file, LogProvider logProvider )
    {
        this.authFile = file.toAbsolutePath();
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public User findByName( String name )
    {
        return usersByName.get( name );
    }

    @Override
    public void start() throws Throwable
    {
        if ( Files.exists( authFile ) )
        {
            loadUsersFromFile();
        }
    }

    @Override
    public void create( User user ) throws IllegalUsernameException, IOException
    {
        if ( !isValidName( user.name() ) )
        {
            throw new IllegalUsernameException( "'" + user.name() + "' is not a valid user name." );
        }

        synchronized (this)
        {
            // Check for existing user
            for ( User other : users )
            {
                if ( other.name().equals( user.name() ) )
                {
                    throw new IllegalUsernameException( "The specified user already exists" );
                }
            }

            users.add( user );

            saveUsersToFile();

            usersByName.put( user.name(), user );
        }
    }

    @Override
    public void update( User existingUser, User updatedUser ) throws ConcurrentModificationException, IOException
    {
        // Assert input is ok
        if ( !existingUser.name().equals( updatedUser.name() ) )
        {
            throw new IllegalArgumentException( "updatedUser has a different name" );
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

            saveUsersToFile();

            usersByName.put( updatedUser.name(), updatedUser );
        }
    }

    @Override
    public boolean delete( User user ) throws IOException
    {
        boolean foundUser = false;
        synchronized (this)
        {
            // Copy-on-write for the users list
            List<User> newUsers = new ArrayList<>();
            for ( User other : users )
            {
                if ( other.name().equals( user.name() ) )
                {
                    foundUser = true;
                } else
                {
                    newUsers.add( other );
                }
            }

            if ( foundUser )
            {
                users = newUsers;

                saveUsersToFile();

                usersByName.remove( user.name() );
            }
        }
        return foundUser;
    }

    @Override
    public int numberOfUsers()
    {
        return users.size();
    }

    @Override
    public boolean isValidName( String name )
    {
        return name.matches( "^[a-zA-Z0-9_]+$" );
    }

    private void saveUsersToFile() throws IOException
    {
        Path directory = authFile.getParent();
        if ( !Files.exists( directory ) )
        {
            Files.createDirectories( directory );
        }

        Path tempFile = Files.createTempFile( directory, authFile.getFileName().toString() + "-", ".tmp" );
        try
        {
            Files.write( tempFile, serialization.serialize( users ) );
            Files.move( tempFile, authFile, ATOMIC_MOVE, REPLACE_EXISTING );
        } catch ( Throwable e )
        {
            Files.delete( tempFile );
            throw e;
        }
    }

    private void loadUsersFromFile() throws IOException
    {
        byte[] fileBytes = Files.readAllBytes( authFile );
        List<User> loadedUsers;
        try
        {
            loadedUsers = serialization.deserializeUsers( fileBytes );
        } catch ( UserSerialization.FormatException e )
        {
            log.error( "Ignoring authorization file \"%s\" (%s)", authFile.toAbsolutePath(), e.getMessage() );
            loadedUsers = new ArrayList<>();
        }

        if ( loadedUsers == null )
        {
            throw new IllegalStateException( "Failed to read authentication file: " + authFile );
        }

        users = loadedUsers;
        for ( User user : users )
        {
            usersByName.put( user.name(), user );
        }
    }
}
