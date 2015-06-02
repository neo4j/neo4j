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
package org.neo4j.kernel.impl.recovery;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TestStoreRecoverer
{
    @Test
    public void shouldNotWantToRecoverIntactStore() throws Exception
    {
        File store = createIntactStore();

        StoreRecoverer recoverer = new StoreRecoverer( fileSystem );

        assertThat( recoverer.recoveryNeededAt( store ), is( false ) );
    }

    @Test
    public void shouldWantToRecoverBrokenStore() throws Exception
    {
        File store = createIntactStore();
        FileSystemAbstraction fileSystemAbstraction = createSomeDataAndCrash( store, fileSystem );

        StoreRecoverer recoverer = new StoreRecoverer( fileSystemAbstraction );

        assertThat( recoverer.recoveryNeededAt( store ), is( true ) );
    }

    @Test
    public void shouldBeAbleToRecoverBrokenStore() throws Exception
    {
        File storeDir = createIntactStore();
        FileSystemAbstraction fileSystemAbstraction = createSomeDataAndCrash( storeDir, fileSystem );

        StoreRecoverer recoverer = new StoreRecoverer( fileSystemAbstraction );

        assertThat( recoverer.recoveryNeededAt( storeDir ), is( true ) );

        // Don't call recoverer.recover, because currently it's hard coded to start an embedded db
        new TestGraphDatabaseFactory().setFileSystem( fileSystemAbstraction ).newImpermanentDatabase( storeDir ).shutdown();

        assertThat( recoverer.recoveryNeededAt( storeDir ), is( false ) );
    }

    private File createIntactStore()
    {
        File storeDir = new File( "dir" ).getAbsoluteFile();
        fileSystem.mkdirs( storeDir );
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir ).shutdown();
        return storeDir;
    }

    private FileSystemAbstraction createSomeDataAndCrash( File store, EphemeralFileSystemAbstraction fileSystem )
            throws IOException
    {
        final GraphDatabaseService db =
                new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( store );


        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }


        EphemeralFileSystemAbstraction snapshot = fileSystem.snapshot();
        db.shutdown();
        return snapshot;
    }

    private final EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
}
