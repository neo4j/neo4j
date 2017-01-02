/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.commandline.dbms;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.StoreLocker;

class StoreLockChecker implements Closeable
{

    private final FileSystemAbstraction fileSystem;
    private final StoreLocker storeLocker;

    private StoreLockChecker( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
        this.storeLocker = new StoreLocker( fileSystem );
    }

    /**
     * Create store lock checker with lock on a provided path if it exists and writable
     * @param databaseDirectory database path
     * @return lock checker or empty closeable in case if path does not exists or is not writable
     * @throws CannotWriteException
     *
     * @see StoreLocker
     * @see Files
     */
    static Closeable check( Path databaseDirectory ) throws CannotWriteException
    {
        Path lockFile = databaseDirectory.resolve( StoreLocker.STORE_LOCK_FILENAME );
        if ( Files.exists( lockFile ) )
        {
            if ( Files.isWritable( lockFile ) )
            {
                StoreLockChecker storeLocker = new StoreLockChecker( new DefaultFileSystemAbstraction() );
                storeLocker.checkLock( databaseDirectory.toFile() );
                return storeLocker;
            }
            else
            {
                throw new CannotWriteException( lockFile );
            }
        }
        return () ->
        {
        };
    }

    private void checkLock( File file )
    {
        storeLocker.checkLock( file );
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( storeLocker, fileSystem );
    }
}
