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
package org.neo4j.kernel.internal.locker;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

import java.io.File;
import java.io.IOException;
import javax.annotation.Resource;

import org.neo4j.kernel.StoreLockException;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.jupiter.api.Assertions.fail;

@EnableRuleMigrationSupport
@ExtendWith( TestDirectoryExtension.class )
public class GlobalStoreLockerTest
{

    @Resource
    public TestDirectory testDirectory;
    @Rule
    public final FileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Test
    public void failToLockSameFolderAcrossIndependentLockers() throws Exception
    {
        File directory = testDirectory.directory( "store-dir" );
        try ( GlobalStoreLocker storeLocker = new GlobalStoreLocker( fileSystemRule.get(), directory ) )
        {
            storeLocker.checkLock();

            try ( GlobalStoreLocker locker = new GlobalStoreLocker( fileSystemRule.get(), directory ) )
            {
                locker.checkLock();
                fail("directory should be locked");
            }
            catch ( StoreLockException expected )
            {
                // expected
            }

            try ( GlobalStoreLocker locker = new GlobalStoreLocker( fileSystemRule.get(), directory ) )
            {
                locker.checkLock();
                fail("directory should be locked");
            }
            catch ( StoreLockException expected )
            {
                // expected
            }
        }
    }

    @Test
    public void allowToLockSameDirectoryIfItWasUnlocked() throws IOException
    {
        File directory = testDirectory.directory( "doubleLock" );
        try ( GlobalStoreLocker storeLocker = new GlobalStoreLocker( fileSystemRule.get(), directory ) )
        {
            storeLocker.checkLock();
        }
        try ( GlobalStoreLocker storeLocker = new GlobalStoreLocker( fileSystemRule.get(), directory ) )
        {
            storeLocker.checkLock();
        }
    }

    @Test
    public void allowMultipleCallstoActuallStoreLocker() throws IOException
    {
        File directory = testDirectory.directory( "multipleCalls" );
        try ( GlobalStoreLocker storeLocker = new GlobalStoreLocker( fileSystemRule.get(), directory ) )
        {
            storeLocker.checkLock();
            storeLocker.checkLock();
            storeLocker.checkLock();
            storeLocker.checkLock();
            storeLocker.checkLock();
        }
    }
}
