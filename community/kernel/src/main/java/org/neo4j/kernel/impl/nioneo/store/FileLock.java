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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;

import org.neo4j.helpers.Settings;
import org.neo4j.kernel.StoreLocker;

public abstract class FileLock
{
    private static FileLock wrapOrNull( final java.nio.channels.FileLock lock )
    {
        if ( lock == null )
        {
            return null;
        }

        return new FileLock()
        {
            @Override
            public void release() throws IOException
            {
                lock.release();
            }
        };
    }

    public static FileLock getOsSpecificFileLock( File fileName, FileChannel channel )
            throws IOException
    {
        if ( Settings.osIsWindows() )
        {
            /*
             * We need to grab only one lock for the whole store. Even though every store will try to grab one
             * we will honor only the top level, dedicated store lock. This has the benefit that older versions of
             * Neo4j that do not have a dedicated locker still lock on the parent file of neostore so this will still
             * block when new instances are started on top of in use older stores and vice versa.
             */
            if ( fileName.getName().equals( StoreLocker.STORE_LOCK_FILENAME ) )
            {
                return getLockFileBasedFileLock( fileName.getParentFile() );
            }

            // For the rest just return placebo locks
            return new PlaceboFileLock();
        }
        else if ( fileName.getName().equals( NeoStore.DEFAULT_NAME ) )
        {
            FileLock regular = wrapOrNull( channel.tryLock() );
            if ( regular == null ) return null;
            FileLock extra = getLockFileBasedFileLock( fileName.getParentFile() );
            if ( extra == null )
            {
                regular.release();
                return null;
            }
            return new DoubleFileLock( regular, extra );
        }
        else
        {
            return wrapOrNull( channel.tryLock() );
        }
    }

    private static FileLock getLockFileBasedFileLock( File storeDir ) throws IOException
    {
        File lockFile = new File( storeDir, "lock" );
        if ( !lockFile.exists() )
        {
            if ( !lockFile.createNewFile() )
            {
                throw new IOException( "Couldn't create lock file " + lockFile.getAbsolutePath() );
            }
        }
        FileChannel fileChannel = new RandomAccessFile( lockFile, "rw" ).getChannel();
        java.nio.channels.FileLock fileChannelLock = null;
        try
        {
            fileChannelLock = fileChannel.tryLock();
        }
        catch ( OverlappingFileLockException e )
        {
            // OK, let fileChannelLock continue to be null and we'll deal with it below
        }
        if ( fileChannelLock == null )
        {
            fileChannel.close();
            return null;
        }
        return new WindowsFileLock( lockFile, fileChannel, fileChannelLock );
    }

    public abstract void release() throws IOException;

    private static class PlaceboFileLock extends FileLock
    {
        @Override
        public void release() throws IOException
        {
        }
    }

    private static class DoubleFileLock extends FileLock
    {
        private final FileLock regular;
        private final FileLock extra;

        DoubleFileLock( FileLock regular, FileLock extra )
        {
            this.regular = regular;
            this.extra = extra;
        }

        @Override
        public void release() throws IOException
        {
            regular.release();
            extra.release();
        }
    }

    private static class WindowsFileLock extends FileLock
    {
        private final File lockFile;
        private final FileChannel fileChannel;
        private final java.nio.channels.FileLock fileChannelLock;

        public WindowsFileLock( File lockFile, FileChannel fileChannel, java.nio.channels.FileLock lock )
                throws IOException
        {
            this.lockFile = lockFile;
            this.fileChannel = fileChannel;
            this.fileChannelLock = lock;
        }

        @Override
        public void release() throws IOException
        {
            try
            {
                fileChannelLock.release();
            }
            finally
            {
                try
                {
                    fileChannel.close();
                }
                finally
                {
                    if ( !lockFile.delete() )
                    {
                        throw new IOException( "Couldn't delete lock file " + lockFile.getAbsolutePath() );
                    }
                }
            }
        }
    }
}
