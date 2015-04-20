/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.TargetDirectory;

import static java.lang.String.format;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.kernel.CannedFileSystemAbstraction.NOTHING;
import static org.neo4j.kernel.StoreLocker.STORE_LOCK_FILENAME;

public class StoreLockerTest
{
    private static final TargetDirectory target = TargetDirectory.forTest( StoreLockerTest.class );

    @Test
    public void shouldObtainLockWhenStoreFileNotLocked() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( true, null, null, true, NOTHING );
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction );

        try
        {
            storeLocker.checkLock( target.cleanDirectory( "unused" ) );

            // Ok
        }
        catch ( StoreLockException e )
        {
            fail();
        }
    }

    @Test
    public void shouldCreateStoreDirAndObtainLockWhenStoreDirDoesNotExist() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( false, null, null, true, NOTHING );
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction );

        try
        {
            storeLocker.checkLock( target.cleanDirectory( "unused" ) );
            // Ok
        }
        catch ( StoreLockException e )
        {
            fail();
        }
    }

    @Test
    public void shouldNotObtainLockWhenStoreDirCannotBeCreated() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( false,
                new IOException( "store dir could not be created" ), null, true, NOTHING );
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction );
        File storeDir = target.cleanDirectory( "unused" );

        try
        {
            storeLocker.checkLock( storeDir );
            fail();
        }
        catch ( StoreLockException e )
        {
            String msg = format( "Unable to create path for store dir: %s. Please ensure no other process is using this database, and that the directory is writable (required even for read-only access)", storeDir );
            assertThat( e.getMessage(), is( msg ) );
        }
    }

    @Test
    public void shouldNotObtainLockWhenUnableToOpenLockFile() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( true, null,
                new IOException( "cannot open lock file" ), true, NOTHING );
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction );
        File storeDir = target.cleanDirectory( "unused" );

        try
        {
            storeLocker.checkLock( storeDir );
            fail();
        }
        catch ( StoreLockException e )
        {
            String msg = format( "Unable to obtain lock on store lock file: %s. Please ensure no other process is using this database, and that the directory is writable (required even for read-only access)", new File( storeDir,
                    STORE_LOCK_FILENAME ) );
            assertThat( e.getMessage(), is( msg ) );
        }
    }

    @Test
    public void shouldNotObtainLockWhenStoreAlreadyInUse() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( true, null, null, false, NOTHING );
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction );

        try
        {
            storeLocker.checkLock( target.cleanDirectory( "unused" ) );
            fail();
        }
        catch ( StoreLockException e )
        {
            assertThat( e.getMessage(), containsString( "Unable to obtain lock on store lock file" ) );
        }
    }
}
