/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.versionLongToString;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.find19FormatStoreDirectory;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

public class StoreMigratorFrom19IT
{
    @Test
    public void shouldMigrate() throws IOException
    {
        // WHEN
        File oldStoreDirectory = find19FormatStoreDirectory();
        upgrader( new StoreMigrator( monitor, fs ) ).migrateIfNeeded( oldStoreDirectory );

        // THEN
        assertEquals( 100, monitor.events.size() );
        assertTrue( monitor.started );
        assertTrue( monitor.finished );
        GraphDatabaseService database = cleanup.add( new GraphDatabaseFactory().newEmbeddedDatabase(
                oldStoreDirectory.getAbsolutePath() ) );

        try
        {
            DatabaseContentVerifier verifier = new DatabaseContentVerifier( database );
            verifier.verifyNodes();
            verifier.verifyRelationships();
//            verifier.verifyNodeIdsReused();
//            verifier.verifyRelationshipIdsReused();
        }
        finally
        {
            // CLEANUP
            database.shutdown();
        }

//        NeoStore neoStore = cleanup.add( storeFactory.newNeoStore( oldStoreDirectory ) );
//        verifyNeoStore( neoStore );
//        neoStore.close();
    }

    private StoreUpgrader upgrader( StoreMigrator storeMigrator )
    {
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fs, StoreUpgrader.NO_MONITOR );
        upgrader.addParticipant( storeMigrator );
        return upgrader;
    }
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final File storeDir = TargetDirectory.forTest( getClass() ).makeGraphDbDir();
    private final ListAccumulatorMigrationProgressMonitor monitor = new ListAccumulatorMigrationProgressMonitor();
    private final IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
    private StoreFactory storeFactory;
    private File storeFileName;

    @Before
    public void setUp()
    {
        Config config = MigrationTestUtils.defaultConfig();
        storeFileName = new File( storeDir, NeoStore.DEFAULT_NAME );
        storeFactory = new StoreFactory( config, idGeneratorFactory,
                new DefaultWindowPoolFactory(), fs, StringLogger.DEV_NULL, new DefaultTxHook() );
    }

    private void verifyNeoStore( NeoStore neoStore )
    {
        assertEquals( 1317392957120L, neoStore.getCreationTime() );
        assertEquals( -472309512128245482l, neoStore.getRandomNumber() );
        assertEquals( 3l, neoStore.getVersion() );
        assertEquals( ALL_STORES_VERSION, versionLongToString( neoStore.getStoreVersion() ) );
        assertEquals( 1007l, neoStore.getLastCommittedTx() );
    }

    private static class DatabaseContentVerifier
    {
        private final String longString = MigrationTestUtils.makeLongString();
        private final int[] longArray = MigrationTestUtils.makeLongArray();
        private final GraphDatabaseService database;

        public DatabaseContentVerifier( GraphDatabaseService database )
        {
            this.database = database;
        }

        private void verifyRelationships()
        {
            try ( Transaction tx = database.beginTx() )
            {
                int traversalCount = 0;
                for ( Relationship rel : GlobalGraphOperations.at( database ).getAllRelationships() )
                {
                    traversalCount++;
                    verifyRelationshipProperties( rel );
                }
                tx.success();
                assertEquals( 100000, traversalCount );
            }
        }

        private void verifyNodes()
        {
            int nodeCount = 0;
            try ( Transaction tx = database.beginTx() )
            {
                for ( Node node : GlobalGraphOperations.at( database ).getAllNodes() )
                {
                    nodeCount++;
                    if ( node.getId() > 0 )
                    {
                        verifyNodeProperties( node );
                    }
                }
                tx.success();
            }
            assertEquals( 110002, nodeCount );
        }

        private void verifyNodeProperties( PropertyContainer node )
        {
            if ( node.hasProperty( "someOtherKey" ) )
            {
                assertArrayEquals( new byte[] { 1,2,3 }, (byte[]) node.getProperty( "someOtherKey" ) );
            }
        }

        private void verifyRelationshipProperties( PropertyContainer relationship )
        {
            assertEquals( "someValue", relationship.getProperty( "someKey" ) );
        }

        private void verifyNodeIdsReused()
        {
            try ( Transaction transaction = database.beginTx() )
            {
                database.getNodeById( 1 );
                fail( "Node 1 should not exist" );
            }
            catch ( NotFoundException e )
            {   // expected
            }

            try ( Transaction transaction = database.beginTx() )
            {
                Node newNode = database.createNode();
                assertEquals( 1, newNode.getId() );
                transaction.success();
            }
        }

        private void verifyRelationshipIdsReused()
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node1 = database.createNode();
                Node node2 = database.createNode();
                Relationship relationship1 = node1.createRelationshipTo( node2, withName( "REUSE" ) );
                assertEquals( 0, relationship1.getId() );
                transaction.success();
            }
        }
    }

    private class ListAccumulatorMigrationProgressMonitor implements MigrationProgressMonitor
    {
        private final List<Integer> events = new ArrayList<>();
        private boolean started = false;
        private boolean finished = false;

        @Override
        public void started()
        {
            started = true;
        }

        @Override
        public void percentComplete( int percent )
        {
            events.add( percent );
        }

        @Override
        public void finished()
        {
            finished = true;
        }
    }

    public final @Rule CleanupRule cleanup = new CleanupRule();
}
