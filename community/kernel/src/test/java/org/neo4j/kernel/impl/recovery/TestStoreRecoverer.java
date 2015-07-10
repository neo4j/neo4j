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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.embedded.CommunityTestGraphDatabase;
import org.neo4j.embedded.TestGraphDatabase;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TestStoreRecoverer

{
    private final EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public TargetDirectory.TestDirectory testDirectory =
            TargetDirectory.testDirForTestWithEphemeralFS( fileSystem, getClass() );

    private File storeDir;

    @Before
    public void setup()
    {
        storeDir = testDirectory.graphDbDir();
        CommunityTestGraphDatabase.build().withFileSystem( fileSystem ).open( storeDir ).shutdown();
    }

    @Test
    public void shouldNotWantToRecoverIntactStore() throws Exception
    {
        StoreRecoverer recoverer = new StoreRecoverer( fileSystem, pageCacheRule.getPageCache( fileSystem ) );

        assertThat( recoverer.recoveryNeededAt( storeDir ), is( false ) );
    }

    @Test
    public void shouldWantToRecoverBrokenStore() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = createSomeDataAndCrash( storeDir, fileSystem );

        StoreRecoverer recoverer =
                new StoreRecoverer( fileSystemAbstraction, pageCacheRule.getPageCache( fileSystemAbstraction ) );

        assertThat( recoverer.recoveryNeededAt( storeDir ), is( true ) );
    }

    @Test
    public void shouldBeAbleToRecoverBrokenStore() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = createSomeDataAndCrash( storeDir, fileSystem );

        StoreRecoverer recoverer =
                new StoreRecoverer( fileSystemAbstraction, pageCacheRule.getPageCache( fileSystemAbstraction ) );

        assertThat( recoverer.recoveryNeededAt( storeDir ), is( true ) );

        // Don't call recoverer.recover, because currently it's hard coded to start an embedded db
        CommunityTestGraphDatabase.build().withFileSystem( fileSystemAbstraction ).open( storeDir ).shutdown();

        assertThat( recoverer.recoveryNeededAt( storeDir ), is( false ) );
    }

    private FileSystemAbstraction createSomeDataAndCrash( File store, EphemeralFileSystemAbstraction fileSystem )
            throws IOException
    {
        final TestGraphDatabase db =
                CommunityTestGraphDatabase.build().withFileSystem( fileSystem ).open( store );


        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }


        EphemeralFileSystemAbstraction snapshot = fileSystem.snapshot();
        db.shutdown();
        return snapshot;
    }
}