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
package org.neo4j.kernel.internal.locker;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

/**
 * The class takes a lock on provided file. The lock is valid after a successful call to
 * {@link #checkLock()} until a call to {@link #close()}.
 */
public class Locker implements Closeable
{
    private final FileSystemAbstraction fileSystemAbstraction;
    private final Path lockFile;

    FileLock lockFileLock;
    private StoreChannel lockFileChannel;

    public Locker( FileSystemAbstraction fileSystemAbstraction, Path lockFile )
    {
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.lockFile = lockFile;
    }

    public final Path lockFile()
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
    public void checkLock()
    {
        if ( haveLockAlready() )
        {
            return;
        }

        try
        {
            if ( !fileSystemAbstraction.fileExists( lockFile ) )
            {
                fileSystemAbstraction.mkdirs( lockFile.getParent() );
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
        catch ( OverlappingFileLockException e )
        {
            throw unableToObtainLockException();
        }
        catch ( IOException e )
        {
            // This isn't your normal "locked by another process" error, it may be related to permissions or something else,
            // so in this case try to figure out as much as possible about the state of the file and directory and include that
            // in the error message given to the user.
            throw unableToObtainLockException( tryCollectPermissionInformation(), e );
        }
    }

    private String tryCollectPermissionInformation()
    {
        String processUserName = System.getProperty( "user.name" );
        String additionalInformation = null;
        if ( processUserName != null )
        {
            Path lockPath = lockFile;
            try
            {
                String lockFileOwner = Files.getOwner( lockPath ).getName();
                if ( !processUserName.equals( lockFileOwner ) )
                {
                    additionalInformation = String.format(
                            "Owner of the lock file '%s' and user running this process '%s' differs, which means this could be a file permission problem. " +
                            "Ensure that the lock file has the same owner, or at least has write access for the user running the Neo4j process " +
                            "trying to lock it", lockFileOwner, processUserName );
                }
                // else no useful additional information can be provided
            }
            catch ( IOException fe )
            {
                // We tried to get the owner of the lock file, but we couldn't. Perhaps we couldn't even create the lock file, let's check the folder
                try
                {
                    String lockDirectoryOwner = Files.getOwner( lockPath.getParent() ).getName();
                    if ( !processUserName.equals( lockDirectoryOwner ) )
                    {
                        additionalInformation = String.format(
                                "Owner of the directory of the lock file '%s' and user running this process '%s' differs, which means this could be a " +
                                "file permission problem. Ensure that the lock file directory (and lock file it it exists) has the same owner, " +
                                "or at least has write access for the user running the Neo4j process trying to lock it",
                                lockDirectoryOwner, processUserName );
                    }
                    // else no useful additional information can be provided
                }
                catch ( IOException de )
                {
                    // We tried to get the owner of the lock directory, but couldn't. There's not much more we can do
                }
            }
        }
        return additionalInformation;
    }

    protected boolean haveLockAlready()
    {
        return lockFileLock != null && lockFileChannel != null;
    }

    FileLockException unableToObtainLockException()
    {
        return unableToObtainLockException( null, null );
    }

    FileLockException unableToObtainLockException( String additionalInformation, Exception cause )
    {
        String message = String.format( "Unable to obtain lock on file: %s%s", lockFile, additionalInformation != null ? ": " + additionalInformation : "" );
        return storeLockException( message, cause );
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
