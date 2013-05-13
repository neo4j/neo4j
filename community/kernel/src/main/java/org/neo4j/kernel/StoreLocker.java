/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.read_only;

public class StoreLocker
{
    public static final String STORE_LOCK_FILENAME = "store_lock";

    private final Config configuration;
    private final FileSystemAbstraction fileSystemAbstraction;

    private FileLock storeLockFileLock;
    private FileChannel storeLockFileChannel;

    public StoreLocker( Config configuration, FileSystemAbstraction fileSystemAbstraction )
    {
        this.configuration = configuration;
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    /**
     * Obtains lock on store file so that we can ensure the store is not shared between database instances
     * <p/>
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
                if ( configuration.get( read_only ) )
                {
                    String msg = "Unable to lock store as store dir does not exist and instance is in read-only mode";
                    throw new StoreLockException( msg );
                }

                fileSystemAbstraction.mkdirs( storeLockFile.getParentFile() );
            }
        }
        catch ( IOException e )
        {
            throw new StoreLockException( "Unable to create path for store dir: " + storeDir, e );
        }

        try
        {
            storeLockFileChannel = fileSystemAbstraction.open( storeLockFile, "rw" );
            storeLockFileLock = fileSystemAbstraction.tryLock( storeLockFile, storeLockFileChannel );

            if ( storeLockFileLock == null )
            {
                throw new StoreLockException( "Could not create lock file" );
            }
        }
        catch ( OverlappingFileLockException e )
        {
            throw new StoreLockException( "Unable to obtain lock on store lock file: " + storeLockFile, e );
        }
        catch ( IOException e )
        {
            throw new StoreLockException( "Unable to obtain lock on store lock file: " + storeLockFile, e );
        }
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