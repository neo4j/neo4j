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

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.read_only;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.StringLogger;

public class StoreLocker
{
    public static final String STORE_LOCK_FILENAME = "store_lock";

    private final Config configuration;
    private final StringLogger logger;
    private final FileSystemAbstraction fileSystemAbstraction;

    private FileLock storeLockFileLock;
    private FileChannel storeLockFileChannel;

    public StoreLocker( Config configuration, FileSystemAbstraction fileSystemAbstraction, StringLogger logger )
    {
        this.configuration = configuration;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.logger = logger;
    }

    /**
     * Obtains lock on store file so that we can ensure the store is not shared between database instances
     * <p/>
     * Creates store dir if necessary, creates store lock file if necessary
     *
     * @return true if lock was successfully obtained, false otherwise
     */
    public boolean lock( File storeDir )
    {
        File storeLockFile = new File( storeDir, STORE_LOCK_FILENAME );

        try
        {
            if ( !fileSystemAbstraction.fileExists( storeLockFile.toString() ) )
            {
                if ( configuration.get( read_only ) )
                {
                    logger.warn( "Unable to lock store as store dir does not exist and instance is in read-only mode" );
                    return false;
                }

                fileSystemAbstraction.autoCreatePath( storeLockFile.getParentFile() );
            }
        }
        catch ( IOException e )
        {
            logger.warn( "Unable to create path for store dir: " + storeDir, e );
            return false;
        }

        try
        {
            storeLockFileChannel = fileSystemAbstraction.open( storeLockFile.toString(), "rw" );
            storeLockFileLock = fileSystemAbstraction.tryLock( storeLockFile.toString(), storeLockFileChannel );
            return storeLockFileLock != null;
        }
        catch ( OverlappingFileLockException e )
        {
            logger.warn( "Unable to obtain lock on store lock file: " + storeLockFile, e );
            return false;
        }
        catch ( IOException e )
        {
            logger.warn( "Unable to obtain lock on store lock file: " + storeLockFile, e );
            return false;
        }
    }

    public void release() throws IOException
    {
        if (storeLockFileLock != null) storeLockFileLock.release();
        if (storeLockFileChannel != null) storeLockFileChannel.close();
    }
}