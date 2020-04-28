/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cypher.internal.security.FormatException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Stores user auth data. In memory, but backed by persistent storage so changes to this repository will survive
 * JVM restarts and crashes.
 */
public class FileUserRepository extends AbstractUserRepository implements FileRepository
{
    private final File authFile;
    private final FileSystemAbstraction fileSystem;

    // TODO: We could improve concurrency by using a ReadWriteLock

    private final Log log;

    private final UserSerialization serialization = new UserSerialization();

    public FileUserRepository( FileSystemAbstraction fileSystem, File file, LogProvider logProvider )
    {
        this.fileSystem = fileSystem;
        this.authFile = file;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Exception
    {
        clear();

        FileRepository.assertNotMigrated( authFile, fileSystem, log );

        ListSnapshot<User> onDiskUsers = readPersistedUsers();
        if ( onDiskUsers != null )
        {
            setUsers( onDiskUsers );
        }
    }

    @Override
    protected ListSnapshot<User> readPersistedUsers() throws IOException
    {
        if ( fileSystem.fileExists( authFile ) )
        {
            long readTime;
            List<User> readUsers;
            try
            {
                readTime = fileSystem.lastModifiedTime( authFile );
                readUsers = serialization.loadRecordsFromFile( fileSystem, authFile );
            }
            catch ( FormatException e )
            {
                log.error( "Failed to read authentication file \"%s\" (%s)",
                        authFile.getAbsolutePath(), e.getMessage() );
                throw new IllegalStateException( "Failed to read authentication file: " + authFile );
            }

            return new ListSnapshot<>( readTime, readUsers );
        }
        return null;
    }

    @Override
    protected void persistUsers() throws IOException
    {
        serialization.saveRecordsToFile( fileSystem, authFile, users );
    }

    @Override
    public ListSnapshot<User> getSnapshot() throws IOException
    {
        if ( lastLoaded.get() < fileSystem.lastModifiedTime( authFile ) )
        {
            return readPersistedUsers();
        }
        synchronized ( this )
        {
            return new ListSnapshot<>( lastLoaded.get(), new ArrayList<>( users ) );
        }
    }

    @Override
    public void purge() throws IOException
    {
        super.purge(); // Clears all cached data

        // Delete the file
        if ( !fileSystem.deleteFile( authFile ) )
        {
            throw new IOException( "Failed to delete file '" + authFile.getAbsolutePath() + '\'' );
        }
    }

    @Override
    public void markAsMigrated() throws IOException
    {
        super.markAsMigrated(); // Clears all cached data

        // Rename the file
        File destinationFile = FileRepository.getMigratedFile( authFile );
        fileSystem.renameFile( authFile, destinationFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES );
    }
}
