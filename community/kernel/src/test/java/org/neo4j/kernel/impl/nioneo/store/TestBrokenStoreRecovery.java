/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import org.junit.Rule;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.test.EphemeralFileSystemRule.shutdownDbAction;

public class TestBrokenStoreRecovery
{
    public final @Rule EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private File storeDir = new File( "dir" );

    /**
     * Creates a store with a truncated property store file that remains like
     * that during recovery by truncating the logical log as well. Id
     * regeneration should proceed without exceptions, even though the last
     * property record is incomplete.
     */
    @Test
    public void testTruncatedPropertyStore() throws Exception
    {
        EphemeralFileSystemAbstraction snapshot = produceUncleanStore();
        File propertyStoreFile = new File( storeDir, "neostore.propertystore.db" );
        trimFileToSize( snapshot, propertyStoreFile, 42 );
        File log = new File( storeDir, "nioneo_logical.log.1" );
        trimFileToSize( snapshot, log, 78 );

        // Previously recovery threw exception, it shouldn't.
        GraphDatabaseService db = newDb();
        try
        {
            // Also assert that the id generator now has the right high id.
            assertEquals( 0L, ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(
                    XaDataSourceManager.class ).getNeoStoreDataSource().getNeoStore().getPropertyStore().getHighId() );
        }
        finally
        {
            db.shutdown();
        }
    }

    private GraphDatabaseService newDb()
    {
        return new TestGraphDatabaseFactory().setFileSystem( fsRule.get() )
                .newImpermanentDatabase( storeDir.getAbsolutePath() );
    }

    private EphemeralFileSystemAbstraction produceUncleanStore()
    {
        GraphDatabaseService db = newDb();
        Transaction tx = db.beginTx();
        try
        {
            db.createNode().setProperty( "name", "Something" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // We simulate a crash here
        return fsRule.snapshot( shutdownDbAction( db ) );
    }

    private static void trimFileToSize( FileSystemAbstraction fs, File theFile, int toSize )
            throws IOException
    {
        assertTrue( fs.fileExists( theFile.getAbsoluteFile() ) );
        StoreChannel channel = fs.open( theFile.getAbsoluteFile(), "rw" );
        try
        {
            channel.truncate( toSize );
        }
        finally
        {
            channel.close();
        }
    }
}
