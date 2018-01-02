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
package org.neo4j.kernel.api.index.util;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.test.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static java.lang.String.format;

public class FailureStorageTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private FolderLayout folderLayout;
    private final long indexId = 1;

    @Before
    public void before()
    {
        File rootDirectory = new File( "dir" );
        fs.get().mkdirs( rootDirectory );
        folderLayout = new FolderLayout( rootDirectory );
    }

    @Test
    public void shouldReserveFailureFile() throws Exception
    {
        // GIVEN
        FailureStorage storage = new FailureStorage( fs.get(), folderLayout );

        // WHEN
        storage.reserveForIndex( indexId );

        // THEN
        File failureFile = storage.failureFile( indexId );
        assertTrue( fs.get().fileExists( failureFile ) );
        assertTrue( fs.get().getFileSize( failureFile ) > 100 );
    }

    @Test
    public void shouldStoreFailure() throws Exception
    {
        // GIVEN
        FailureStorage storage = new FailureStorage( fs.get(), folderLayout );
        storage.reserveForIndex( indexId );
        String failure = format( "A failure message%nspanning%nmultiple lines." );

        // WHEN
        storage.storeIndexFailure( indexId, failure );

        // THEN
        File failureFile = storage.failureFile( indexId );
        assertTrue( fs.get().fileExists( failureFile ) );
        assertTrue( fs.get().getFileSize( failureFile ) > 100 );
        assertEquals( failure, storage.loadIndexFailure( indexId ) );
    }

    @Test
    public void shouldClearFailure() throws Exception
    {
        // GIVEN
        FailureStorage storage = new FailureStorage( fs.get(), folderLayout );
        storage.reserveForIndex( indexId );
        String failure = format( "A failure message%nspanning%nmultiple lines." );
        storage.storeIndexFailure( indexId, failure );
        File failureFile = storage.failureFile( indexId );
        assertTrue( fs.get().fileExists( failureFile ) );
        assertTrue( fs.get().getFileSize( failureFile ) > 100 );

        // WHEN
        storage.clearForIndex( indexId );

        // THEN
        assertFalse( fs.get().fileExists( failureFile ) );
    }
}
