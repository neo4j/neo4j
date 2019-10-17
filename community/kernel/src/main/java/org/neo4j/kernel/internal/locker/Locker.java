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
import org.neo4j.io.fs.StoreChannel;

/**
 * The class takes a lock on provided file. The lock is valid after a successful call to
 * {@link #checkLock()} until a call to {@link #close()}.
 */
public class Locker implements Closeable
{
    private final FileSystemAbstraction fileSystemAbstraction;
    private final File lockFile;

    FileLock lockFileLock;
    private StoreChannel lockFileChannel;

    public Locker( FileSystemAbstraction fileSystemAbstraction, File lockFile )
    {
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.lockFile = lockFile;
    }

    public final File lockFile()
    {
        return lockFile;
    }

    /**
     * Obtains lock on file so that we can ensure the store is not shared between different database instances
     * <p>
     * Creates dir if necessary, creates lock file if necessary
     * <p>
     * Please note that this lock is only valid for as long the {@link #lockFileChannel} lives, so make sure the
     * lock cannot be garbage collected as long as the lock should be valid.
     *
     * @throws FileLockException if lock could not be acquired
     */
    public void checkLock() throws FileLockException
    {
        if ( haveLockAlready() )
        {
            return;
        }

        try
        {
            if ( !fileSystemAbstraction.fileExists( lockFile ) )
            {
                fileSystemAbstraction.mkdirs( lockFile.getParentFile() );
            }
        }
        catch ( IOException e )
        {
            String message = "Unable to create path for dir: " + lockFile.getParent();
            throw storeLockException( message, e );
        }

        try
        {
            if ( lockFileChannel == null )
            {
                lockFileChannel = fileSystemAbstraction.write( lockFile );
            }
            lockFileLock = lockFileChannel.tryLock();
            if ( lockFileLock == null )
            {
                String message = "Lock file has been locked by another process: " + lockFile;
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
        return lockFileLock != null && lockFileChannel != null;
    }

    FileLockException unableToObtainLockException()
    {
        String message = "Unable to obtain lock on file: " + lockFile;
        return storeLockException( message, null );
    }

    private static FileLockException storeLockException( String message, Exception e )
    {
        String help = "Please ensure no other process is using this database, and that the directory is writable " +
                "(required even for read-only access)";
        return new FileLockException( message + ". " + help, e );
    }

    @Override
    public void close() throws IOException
    {
        if ( lockFileLock != null )
        {
            releaseLock();
        }
        if ( lockFileChannel != null )
        {
            releaseChannel();
        }
    }

    private void releaseChannel() throws IOException
    {
        lockFileChannel.close();
        lockFileChannel = null;
    }

    protected void releaseLock() throws IOException
    {
        lockFileLock.release();
        lockFileLock = null;
    }
}
