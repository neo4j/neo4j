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
package org.neo4j.kernel.api.impl.index.storage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.kernel.api.impl.index.storage.layout.IndexFolderLayout;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.CoreMatchers.containsString;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FailureStorageTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private IndexFolderLayout indexFolderLayout;

    @Before
    public void before()
    {
        File rootDirectory = new File( "dir" );
        fs.get().mkdirs( rootDirectory );
        indexFolderLayout = new IndexFolderLayout( rootDirectory );
    }

    @Test
    public void shouldReserveFailureFile() throws Exception
    {
        // GIVEN
        FailureStorage storage = new FailureStorage( fs.get(), indexFolderLayout );

        // WHEN
        storage.reserveForIndex();

        // THEN
        File failureFile = storage.failureFile();
        assertTrue( fs.get().fileExists( failureFile ) );
        assertTrue( fs.get().getFileSize( failureFile ) > 100 );
    }

    @Test
    public void shouldStoreFailure() throws Exception
    {
        // GIVEN
        FailureStorage storage = new FailureStorage( fs.get(), indexFolderLayout );
        storage.reserveForIndex();
        String failure = format( "A failure message%nspanning%nmultiple lines." );

        // WHEN
        storage.storeIndexFailure( failure );

        // THEN
        File failureFile = storage.failureFile();
        assertTrue( fs.get().fileExists( failureFile ) );
        assertTrue( fs.get().getFileSize( failureFile ) > 100 );
        assertEquals( failure, storage.loadIndexFailure() );
    }

    @Test
    public void shouldClearFailure() throws Exception
    {
        // GIVEN
        FailureStorage storage = new FailureStorage( fs.get(), indexFolderLayout );
        storage.reserveForIndex();
        String failure = format( "A failure message%nspanning%nmultiple lines." );
        storage.storeIndexFailure( failure );
        File failureFile = storage.failureFile();
        assertTrue( fs.get().fileExists( failureFile ) );
        assertTrue( fs.get().getFileSize( failureFile ) > 100 );

        // WHEN
        storage.clearForIndex();

        // THEN
        assertFalse( fs.get().fileExists( failureFile ) );
    }

    @Test
    public void shouldAppendFailureIfAlreadyExists() throws Exception
    {
        // GIVEN
        FailureStorage storage = new FailureStorage( fs.get(), indexFolderLayout );
        storage.reserveForIndex();
        String failure1 = "Once upon a time there was a first failure";
        String failure2 = "Then there was another";
        storage.storeIndexFailure( failure1 );

        // WHEN
        storage.storeIndexFailure( failure2 );

        // THEN
        String allFailures = storage.loadIndexFailure();
        assertThat( allFailures, containsString( failure1 ) );
        assertThat( allFailures, containsString( failure2 ) );
    }
}
