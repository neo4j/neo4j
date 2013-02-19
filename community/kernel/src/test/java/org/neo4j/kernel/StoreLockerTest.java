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
package org.neo4j.kernel;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

public class StoreLockerTest
{
    private static final File SOME_DIRECTORY = new File( "target/StoreLockerTest/unused" );

    @Test
    public void shouldObtainLockWhenStoreFileNotLocked() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( true, null, null, true );
        StoreLocker storeLocker = new StoreLocker( new Config(), fileSystemAbstraction, DEV_NULL );

        boolean locked = storeLocker.lock( SOME_DIRECTORY );

        assertThat( locked, is( true ) );
    }

    @Test
    public void shouldCreateStoreDirAndObtainLockWhenStoreDirDoesNotExist() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( false, null, null, true );
        StoreLocker storeLocker = new StoreLocker( new Config(), fileSystemAbstraction, DEV_NULL );

        boolean locked = storeLocker.lock( SOME_DIRECTORY );

        assertThat( locked, is( true ) );
    }

    @Test
    public void shouldNotObtainLockWhenStoreDirCannotBeCreated() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( false,
                new IOException( "store dir could not be created" ), null, true );
        StoreLocker storeLocker = new StoreLocker( new Config(), fileSystemAbstraction, DEV_NULL );

        boolean locked = storeLocker.lock( SOME_DIRECTORY );

        assertThat( locked, is( false ) );
    }

    @Test
    public void shouldNotObtainLockWhenStoreDirDoesNotExistAndInReadOnlyMode() throws Exception
    {
        Map<String, String> inputParams = new HashMap<String, String>();
        inputParams.put( GraphDatabaseSettings.read_only.name(), "true" );
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( false, null, null, true );
        StoreLocker storeLocker = new StoreLocker( new Config( inputParams ), fileSystemAbstraction, DEV_NULL );

        boolean locked = storeLocker.lock( SOME_DIRECTORY );

        assertThat( locked, is( false ) );
    }

    @Test
    public void shouldNotObtainLockWhenUnableToOpenLockFile() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( true, null,
                new IOException( "cannot open lock file" ), true );
        StoreLocker storeLocker = new StoreLocker( new Config(), fileSystemAbstraction, DEV_NULL );

        boolean locked = storeLocker.lock( SOME_DIRECTORY );

        assertThat( locked, is( false ) );
    }

    @Test
    public void shouldNotObtainLockWhenStoreAlreadyInUse() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new CannedFileSystemAbstraction( true, null, null, false );
        StoreLocker storeLocker = new StoreLocker( new Config(), fileSystemAbstraction, DEV_NULL );

        boolean locked = storeLocker.lock( SOME_DIRECTORY );

        assertThat( locked, is( false ) );
    }
}