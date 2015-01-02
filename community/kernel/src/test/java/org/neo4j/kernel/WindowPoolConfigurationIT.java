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
package org.neo4j.kernel;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertNotNull;

public class WindowPoolConfigurationIT
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( WindowPoolConfigurationIT.class );

    @Test
    public void testTinyStoreWithLessMemory() throws IOException
    {
        verifyPersistenceWindowPoolCanBeConfigured( 100, 0.9 );
    }

    @Test
    public void testTinyStoreWithMoreMemory() throws IOException
    {
        verifyPersistenceWindowPoolCanBeConfigured( 100, 1.1 );
    }

    @Test
    public void testSmallStoreWithLessMemory() throws IOException
    {
        verifyPersistenceWindowPoolCanBeConfigured( 1000, 0.9 );
    }

    @Test
    public void testSmallStoreWithMoreMemory() throws IOException
    {
        verifyPersistenceWindowPoolCanBeConfigured( 1000, 1.1 );
    }

    @Test
    public void testMediumStoreWithLessMemory() throws IOException
    {
        verifyPersistenceWindowPoolCanBeConfigured( 10000, 0.9 );
    }

    @Test
    public void testMediumStoreWithMoreMemory() throws IOException
    {
        verifyPersistenceWindowPoolCanBeConfigured( 10000, 1.1 );
    }

    private void verifyPersistenceWindowPoolCanBeConfigured( int nodeCount, double memoryFactor ) throws IOException
    {
        File dir = testDirectory.directory();
        FileUtils.deleteRecursively( dir );
        dir.mkdirs();

        setUpTestData( dir, nodeCount );

        initializeDatabaseWithTestConfiguration( dir, nodeCount, memoryFactor );
    }

    private void setUpTestData( File dir, int nodeCount )
    {
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.getAbsolutePath() )
                .newGraphDatabase();
        createTestData( db, nodeCount );
        db.shutdown();
    }

    private void createTestData( GraphDatabaseService db, int nodeCount )
    {
        Transaction tx = db.beginTx();

        for ( int j = 0; j < nodeCount; j++ )
        {
            db.createNode();
        }

        tx.success();
        tx.finish();
    }

    private void initializeDatabaseWithTestConfiguration( File dir, int nodeCount, double memoryFactor )
    {
        int nodeStoreMappedBytes = ((int) ((nodeCount * NodeStore.RECORD_SIZE) * memoryFactor)) - (NodeStore.RECORD_SIZE / 2);
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir.getAbsolutePath() )
                .setConfig( GraphDatabaseSettings.nodestore_mapped_memory_size, "" + nodeStoreMappedBytes )
                .setConfig( GraphDatabaseSettings.cache_type, "none" )
                .newGraphDatabase();
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.getNodeById( nodeCount - 1 );
            assertNotNull( node );
            tx.success();
        }
        finally
        {
            tx.finish();
            db.shutdown();
        }
    }
}
