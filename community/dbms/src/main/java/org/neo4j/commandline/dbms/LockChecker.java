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
package org.neo4j.commandline.dbms;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.kernel.internal.locker.GlobalLocker;
import org.neo4j.kernel.internal.locker.Locker;

public final class LockChecker implements Closeable
{
    private final FileSystemAbstraction fileSystem;
    private final Locker locker;

    private LockChecker( FileSystemAbstraction fileSystem, Locker locker )
    {
        this.fileSystem = fileSystem;
        this.locker = locker;
    }

    public static Closeable checkDbmsLock( Neo4jLayout neo4jLayout ) throws CannotWriteException
    {
        var fileSystem = new DefaultFileSystemAbstraction();
        var locker = new GlobalLocker( fileSystem, neo4jLayout );
        return check( locker, fileSystem );
    }

    public static Closeable checkDatabaseLock( DatabaseLayout databaseLayout ) throws CannotWriteException
    {
        var fileSystem = new DefaultFileSystemAbstraction();
        var locker = new DatabaseLocker( fileSystem, databaseLayout );
        return check( locker, fileSystem );
    }

    private static Closeable check( Locker locker, FileSystemAbstraction fileSystem ) throws CannotWriteException
    {
        var lockFile = locker.lockFile().toPath();

        if ( Files.isWritable( lockFile.getParent() ) )
        {
            if ( Files.exists( lockFile ) && !Files.isWritable( lockFile ) )
            {
                throw new CannotWriteException( lockFile );
            }
            LockChecker lockChecker = new LockChecker( fileSystem, locker );
            try
            {
                lockChecker.checkLock();
                return lockChecker;
            }
            catch ( FileLockException le )
            {
                try
                {
                    locker.close();
                }
                catch ( IOException e )
                {
                    le.addSuppressed( e );
                }
                throw le;
            }
        }
        else
        {
            throw new CannotWriteException( lockFile );
        }
    }

    private void checkLock()
    {
        locker.checkLock();
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( locker, fileSystem );
    }
}
