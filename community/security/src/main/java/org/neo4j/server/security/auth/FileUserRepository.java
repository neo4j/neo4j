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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Stores user auth data. In memory, but backed by persistent storage so changes to this repository will survive
 * JVM restarts and crashes.
 */
public class FileUserRepository extends AbstractUserRepository
{
    private final Path authFile;

    // TODO: We could improve concurrency by using a ReadWriteLock

    private final Log log;

    private final UserSerialization serialization = new UserSerialization();

    public FileUserRepository( Path file, LogProvider logProvider )
    {
        this.authFile = file.toAbsolutePath();
        this.log = logProvider.getLog( getClass() );
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
    protected void saveUsers() throws IOException
    {
        saveUsersToFile();
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
            log.error( "Failed to read authentication file \"%s\" (%s)", authFile.toAbsolutePath(), e.getMessage() );
            throw new IllegalStateException( "Failed to read authentication file: " + authFile );
        }

        users = loadedUsers;
        for ( User user : users )
        {
            usersByName.put( user.name(), user );
        }
    }
}
