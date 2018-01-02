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
package org.neo4j.commandline.dbms;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.internal.StoreLocker;

class StoreLockChecker
{
    Closeable withLock( Path databaseDirectory ) throws CommandFailed, CannotWriteException
    {
        Path lockFile = databaseDirectory.resolve( StoreLocker.STORE_LOCK_FILENAME );
        if ( Files.exists( lockFile ) )
        {
            if ( Files.isWritable( lockFile ) )
            {
                StoreLocker storeLocker = new StoreLocker( new DefaultFileSystemAbstraction() );

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
}
