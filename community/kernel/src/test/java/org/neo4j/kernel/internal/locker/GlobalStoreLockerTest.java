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

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Neo4jLayoutExtension
class GlobalStoreLockerTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private Neo4jLayout neo4jLayout;

    @Test
    void failToLockSameFolderAcrossIndependentLockers() throws Exception
    {
        try ( GlobalLocker storeLocker = new GlobalLocker( fileSystem, neo4jLayout ) )
        {
            storeLocker.checkLock();

            assertThrows( FileLockException.class, () ->
            {
                try ( GlobalLocker locker = new GlobalLocker( fileSystem, neo4jLayout ) )
                {
                    locker.checkLock();
                }
            } );

            assertThrows( FileLockException.class, () ->
            {
                try ( GlobalLocker locker = new GlobalLocker( fileSystem, neo4jLayout ) )
                {
                    locker.checkLock();
                }
            } );
        }
    }

    @Test
    void allowToLockSameDirectoryIfItWasUnlocked() throws IOException
    {
        try ( GlobalLocker storeLocker = new GlobalLocker( fileSystem, neo4jLayout ) )
        {
            storeLocker.checkLock();
        }
        try ( GlobalLocker storeLocker = new GlobalLocker( fileSystem, neo4jLayout ) )
        {
            storeLocker.checkLock();
        }
    }

    @Test
    void allowMultipleCallstoActuallyStoreLocker() throws IOException
    {
        try ( GlobalLocker storeLocker = new GlobalLocker( fileSystem, neo4jLayout ) )
        {
            storeLocker.checkLock();
            storeLocker.checkLock();
            storeLocker.checkLock();
            storeLocker.checkLock();
            storeLocker.checkLock();
        }
    }
}
