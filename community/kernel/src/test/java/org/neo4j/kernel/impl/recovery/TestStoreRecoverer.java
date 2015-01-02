/**
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
package org.neo4j.kernel.impl.recovery;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class TestStoreRecoverer
{
    @Test
    public void shouldNotWantToRecoverIntactStore() throws Exception
    {
        File store = null;
        store = createIntactStore();

        StoreRecoverer recoverer = new StoreRecoverer( fileSystem );

        assertThat( recoverer.recoveryNeededAt( store, new HashMap<String, String>() ), is( false ) );
    }

    @Test
    public void shouldWantToRecoverBrokenStore() throws Exception
    {
        File store = createIntactStore();
        fileSystem.deleteFile( new File( store, "nioneo_logical.log.active" ) );

        StoreRecoverer recoverer = new StoreRecoverer( fileSystem );

        assertThat( recoverer.recoveryNeededAt( store, new HashMap<String, String>() ), is( true ) );
    }

    @Test
    public void shouldBeAbleToRecoverBrokenStore() throws Exception
    {
        File store = createIntactStore();
        fileSystem.deleteFile( new File( store, "nioneo_logical.log.active" ) );

        StoreRecoverer recoverer = new StoreRecoverer( fileSystem );

        assertThat( recoverer.recoveryNeededAt( store, new HashMap<String, String>() ), is( true ) );

        // Don't call recoverer.recover, because currently it's hard coded to start an embedded db
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( store.getPath() ).shutdown();

        assertThat( recoverer.recoveryNeededAt( store, new HashMap<String, String>() ), is( false ) );
    }

    private File createIntactStore() throws IOException
    {
        File storeDir = new File( "dir" );
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir.getPath() ).shutdown();
        return storeDir;
    }

    private final EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
}
