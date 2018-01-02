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
package org.neo4j.kernel;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class StoreLocker
{
    public static final String STORE_LOCK_FILENAME = "store_lock";

    private final FileSystemAbstraction fileSystemAbstraction;

    private FileLock storeLockFileLock;
    private StoreChannel storeLockFileChannel;

    public StoreLocker( FileSystemAbstraction fileSystemAbstraction )
    {
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    /**
     * Obtains lock on store file so that we can ensure the store is not shared between database instances
     * <p>
     * Creates store dir if necessary, creates store lock file if necessary
     *
     * @throws StoreLockException if lock could not be acquired
     */
    public void checkLock( File storeDir ) throws StoreLockException
    {
        File storeLockFile = new File( storeDir, STORE_LOCK_FILENAME );

        try
        {
            if ( !fileSystemAbstraction.fileExists( storeLockFile ) )
            {
                fileSystemAbstraction.mkdirs( storeLockFile.getParentFile() );
            }
        }
        catch ( IOException e )
        {
            String message = "Unable to create path for store dir: " + storeDir;
            throw storeLockException( message, e );
        }

        try
        {
            storeLockFileChannel = fileSystemAbstraction.open( storeLockFile, "rw" );
            storeLockFileLock = storeLockFileChannel.tryLock();
            if ( storeLockFileLock == null )
            {
                String message = "Store and its lock file has been locked by another process: " + storeLockFile;
                throw storeLockException( message, null );
            }
        }
        catch ( OverlappingFileLockException | IOException e )
        {
            String message = "Unable to obtain lock on store lock file: " + storeLockFile;
            throw storeLockException( message, e );
        }
    }

    private StoreLockException storeLockException( String message, Exception e )
    {
        String help = "Please ensure no other process is using this database, and that the directory is writable " +
                      "(required even for read-only access)";
        return new StoreLockException( message + ". " + help, e );
    }

    public void release() throws IOException
    {
        if ( storeLockFileLock != null )
        {
            storeLockFileLock.release();
            storeLockFileLock = null;
        }
        if ( storeLockFileChannel != null )
        {
            storeLockFileChannel.close();
            storeLockFileChannel = null;
        }
    }
}
