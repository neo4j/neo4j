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
package org.neo4j.kernel.internal.locker;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.StoreLockException;

/**
 * The class takes a lock on the {@link #STORE_LOCK_FILENAME} file. The lock is valid after a successful call to
 * {@link #checkLock()} until a call to {@link #close()}.
 */
public class StoreLocker implements Closeable
{
    public static final String STORE_LOCK_FILENAME = "store_lock";

    final FileSystemAbstraction fileSystemAbstraction;
    final File storeLockFile;

    FileLock storeLockFileLock;
    private StoreChannel storeLockFileChannel;

    public StoreLocker( FileSystemAbstraction fileSystemAbstraction, File storeDirectory )
    {
        this.fileSystemAbstraction = fileSystemAbstraction;
        storeLockFile = new File( storeDirectory, STORE_LOCK_FILENAME );
    }

    /**
     * Obtains lock on store file so that we can ensure the store is not shared between database instances
     * <p>
     * Creates store dir if necessary, creates store lock file if necessary
     * <p>
     * Please note that this lock is only valid for as long the {@link #storeLockFileChannel} lives, so make sure the
     * lock cannot be garbage collected as long as the lock should be valid.
     *
     * @throws StoreLockException if lock could not be acquired
     */
    public void checkLock() throws StoreLockException
    {
        if ( haveLockAlready() )
        {
            return;
        }

        try
        {
            if ( !fileSystemAbstraction.fileExists( storeLockFile ) )
            {
                fileSystemAbstraction.mkdirs( storeLockFile.getParentFile() );
            }
        }
        catch ( IOException e )
        {
            String message = "Unable to create path for store dir: " + storeLockFile.getParent();
            throw storeLockException( message, e );
        }

        try
        {
            if ( storeLockFileChannel == null )
            {
                storeLockFileChannel = fileSystemAbstraction.open( storeLockFile, OpenMode.READ_WRITE );
            }
            storeLockFileLock = storeLockFileChannel.tryLock();
            if ( storeLockFileLock == null )
            {
                String message = "Store and its lock file has been locked by another process: " + storeLockFile;
                throw storeLockException( message, null );
            }
        }
        catch ( OverlappingFileLockException | IOException e )
        {
            throw unableToObtainLockException();
        }
    }

    protected boolean haveLockAlready()
    {
        return storeLockFileLock != null && storeLockFileChannel != null;
    }

    StoreLockException unableToObtainLockException()
    {
        String message = "Unable to obtain lock on store lock file: " + storeLockFile;
        return storeLockException( message, null );
    }

    private StoreLockException storeLockException( String message, Exception e )
    {
        String help = "Please ensure no other process is using this database, and that the directory is writable " +
                "(required even for read-only access)";
        return new StoreLockException( message + ". " + help, e );
    }

    @Override
    public void close() throws IOException
    {
        if ( storeLockFileLock != null )
        {
            releaseLock();
        }
        if ( storeLockFileChannel != null )
        {
            releaseChannel();
        }
    }

    private void releaseChannel() throws IOException
    {
        storeLockFileChannel.close();
        storeLockFileChannel = null;
    }

    protected void releaseLock() throws IOException
    {
        storeLockFileLock.release();
        storeLockFileLock = null;
    }
}
