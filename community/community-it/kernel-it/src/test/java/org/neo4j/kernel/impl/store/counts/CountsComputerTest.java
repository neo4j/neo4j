/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.store.counts;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@PageCacheExtension
@Neo4jLayoutExtension
class CountsComputerTest
{
    private static final NullLogProvider LOG_PROVIDER = NullLogProvider.getInstance();
    private static final Config CONFIG = Config.defaults();

    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseLayout databaseLayout;

    private DatabaseManagementServiceBuilder dbBuilder;

    @BeforeEach
    void setup()
    {
        dbBuilder = new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .impermanent();
    }

    @Test
    void skipPopulationWhenNodeAndRelationshipStoresAreEmpty()
    {
        DatabaseManagementService managementService = dbBuilder.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        long lastCommittedTransactionId = getLastTxId( db );
        managementService.shutdown();

        InvocationTrackingProgressReporter progressReporter = new InvocationTrackingProgressReporter();
        rebuildCounts( lastCommittedTransactionId, progressReporter );

        checkEmptyCountStore();
        assertTrue( progressReporter.isCompleteInvoked() );
        assertFalse( progressReporter.isStartInvoked() );
    }

    @Test
    void shouldCreateAnEmptyCountsStoreFromAnEmptyDatabase()
    {
        DatabaseManagementService managementService = dbBuilder.build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        long lastCommittedTransactionId = getLastTxId( db );
        managementService.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        checkEmptyCountStore();
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreNodesInTheDB() throws IOException
    {
        DatabaseManagementService managementService = dbBuilder.build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( Label.label( "A" ) );
            tx.createNode( Label.label( "C" ) );
            tx.createNode( Label.label( "D" ) );
            tx.createNode();
            tx.commit();
        }
        long lastCommittedTransactionId = getLastTxId( db );
        managementService.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( GBPTreeCountsStore store = createCountsStore() )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 4, store.nodeCount( -1 ) );
            assertEquals( 1, store.nodeCount( 0 ) );
            assertEquals( 1, store.nodeCount( 1 ) );
            assertEquals( 1, store.nodeCount( 2 ) );
            assertEquals( 0, store.nodeCount( 3 ) );
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreUnusedNodeRecordsInTheDB() throws IOException
    {
        DatabaseManagementService managementService = dbBuilder.build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( Label.label( "A" ) );
            tx.createNode( Label.label( "C" ) );
            Node node = tx.createNode( Label.label( "D" ) );
            tx.createNode();
            node.delete();
            tx.commit();
        }
        long lastCommittedTransactionId = getLastTxId( db );
        managementService.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( GBPTreeCountsStore store = createCountsStore() )
        {
            assertEquals( lastCommittedTransactionId, store.txId() );
            assertEquals( 3, store.nodeCount( -1 ) );
            assertEquals( 1, store.nodeCount( 0 ) );
            assertEquals( 1, store.nodeCount( 1 ) );
            assertEquals( 0, store.nodeCount( 2 ) );
            assertEquals( 0, store.nodeCount( 3 ) );
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreUnusedRelationshipRecordsInTheDB() throws IOException
    {
        DatabaseManagementService managementService = dbBuilder.build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = tx.createNode( Label.label( "A" ) );
            Node nodeC = tx.createNode( Label.label( "C" ) );
            Relationship rel = nodeA.createRelationshipTo( nodeC, RelationshipType.withName( "TYPE1" ) );
            nodeC.createRelationshipTo( nodeA, RelationshipType.withName( "TYPE2" ) );
            rel.delete();
            tx.commit();
        }
        long lastCommittedTransactionId = getLastTxId( db );
        managementService.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( GBPTreeCountsStore store = createCountsStore() )
        {
            assertEquals( lastCommittedTransactionId, store.txId() );
            assertEquals( 2, store.nodeCount( -1 ) );
            assertEquals( 1, store.nodeCount( 0 ) );
            assertEquals( 1, store.nodeCount( 1 ) );
            assertEquals( 0, store.nodeCount( 2 ) );
            assertEquals( 0, store.nodeCount( 3 ) );
            assertEquals( 0, store.relationshipCount( -1, 0, -1 ) );
            assertEquals( 1, store.relationshipCount( -1, 1, -1 ) );
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreNodesAndRelationshipsInTheDB() throws IOException
    {
        DatabaseManagementService managementService = dbBuilder.build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = tx.createNode( Label.label( "A" ) );
            Node nodeC = tx.createNode( Label.label( "C" ) );
            Node nodeD = tx.createNode( Label.label( "D" ) );
            Node node = tx.createNode();
            nodeA.createRelationshipTo( nodeD, RelationshipType.withName( "TYPE" ) );
            node.createRelationshipTo( nodeC, RelationshipType.withName( "TYPE2" ) );
            tx.commit();
        }
        long lastCommittedTransactionId = getLastTxId( db );
        managementService.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( GBPTreeCountsStore store = createCountsStore() )
        {
            assertEquals( lastCommittedTransactionId, store.txId() );
            assertEquals( 4, store.nodeCount( -1 ) );
            assertEquals( 1, store.nodeCount( 0 ) );
            assertEquals( 1, store.nodeCount( 1 ) );
            assertEquals( 1, store.nodeCount( 2 ) );
            assertEquals( 0, store.nodeCount( 3 ) );
            assertEquals( 2, store.relationshipCount( -1, -1, -1 ) );
            assertEquals( 1, store.relationshipCount( -1, 0, -1 ) );
            assertEquals( 1, store.relationshipCount( -1, 1, -1 ) );
            assertEquals( 0, store.relationshipCount( -1, 2, -1 ) );
            assertEquals( 1, store.relationshipCount( -1, 1, 1 ) );
            assertEquals( 0, store.relationshipCount( -1, 0, 1 ) );
        }
    }

    @Test
    void shouldCreateACountStoreWhenDBContainsDenseNodes() throws IOException
    {
        DatabaseManagementService managementService = dbBuilder.
                setConfig( GraphDatabaseSettings.dense_node_threshold, 2 ).build();
        final GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = tx.createNode( Label.label( "A" ) );
            Node nodeC = tx.createNode( Label.label( "C" ) );
            Node nodeD = tx.createNode( Label.label( "D" ) );
            nodeA.createRelationshipTo( nodeA, RelationshipType.withName( "TYPE1" ) );
            nodeA.createRelationshipTo( nodeC, RelationshipType.withName( "TYPE2" ) );
            nodeA.createRelationshipTo( nodeD, RelationshipType.withName( "TYPE3" ) );
            nodeD.createRelationshipTo( nodeC, RelationshipType.withName( "TYPE4" ) );
            tx.commit();
        }
        long lastCommittedTransactionId = getLastTxId( db );
        managementService.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( GBPTreeCountsStore store = createCountsStore() )
        {
            assertEquals( lastCommittedTransactionId, store.txId() );
            assertEquals( 3, store.nodeCount( -1 ) );
            assertEquals( 1, store.nodeCount( 0 ) );
            assertEquals( 1, store.nodeCount( 1 ) );
            assertEquals( 1, store.nodeCount( 2 ) );
            assertEquals( 0, store.nodeCount( 3 ) );
            assertEquals( 4, store.relationshipCount( -1, -1, -1 ) );
            assertEquals( 1, store.relationshipCount( -1, 0, -1 ) );
            assertEquals( 1, store.relationshipCount( -1, 1, -1 ) );
            assertEquals( 1, store.relationshipCount( -1, 2, -1 ) );
            assertEquals( 1, store.relationshipCount( -1, 3, -1 ) );
            assertEquals( 0, store.relationshipCount( -1, 4, -1 ) );
            assertEquals( 1, store.relationshipCount( -1, 1, 1 ) );
            assertEquals( 2, store.relationshipCount( -1, -1, 1 ) );
            assertEquals( 3, store.relationshipCount( 0, -1, -1 ) );
        }
    }

    private File countsStoreFile()
    {
        return databaseLayout.countStore();
    }

    private static long getLastTxId( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
    }

    private void checkEmptyCountStore()
    {
        try ( GBPTreeCountsStore store = createCountsStore() )
        {
            store.start();
            assertEquals( BASE_TX_ID, store.txId() );
            // check that nothing is stored in the counts store by trying all combinations of tokens in the lower range
            for ( int s = 0; s < 10; s++ )
            {
                assertEquals( store.nodeCount( s ), 0 );
                for ( int e = 0; e < 10; e++ )
                {
                    for ( int t = 0; t < 10; t++ )
                    {
                        assertEquals( store.relationshipCount( s, t, e ), 0 );
                    }
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void cleanupCountsForRebuilding()
    {
        fileSystem.deleteFile( countsStoreFile() );
    }

    private GBPTreeCountsStore createCountsStore() throws IOException
    {
        return createCountsStore( CountsBuilder.EMPTY );
    }

    private GBPTreeCountsStore createCountsStore( CountsBuilder builder ) throws IOException
    {
        return new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), immediate(), builder, false, GBPTreeCountsStore.NO_MONITOR );
    }

    private void rebuildCounts( long lastCommittedTransactionId )
    {
        rebuildCounts( lastCommittedTransactionId, ProgressReporter.SILENT );
    }

    private void rebuildCounts( long lastCommittedTransactionId, ProgressReporter progressReporter )
    {
        cleanupCountsForRebuilding();

        IdGeneratorFactory idGenFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
        StoreFactory storeFactory = new StoreFactory( databaseLayout, CONFIG, idGenFactory, pageCache, fileSystem, LOG_PROVIDER );
        try ( NeoStores neoStores = storeFactory.openAllNeoStores() )
        {
            NodeStore nodeStore = neoStores.getNodeStore();
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            int highLabelId = (int) neoStores.getLabelTokenStore().getHighId();
            int highRelationshipTypeId = (int) neoStores.getRelationshipTypeTokenStore().getHighId();
            CountsComputer countsComputer = new CountsComputer(
                    lastCommittedTransactionId, nodeStore, relationshipStore, highLabelId, highRelationshipTypeId, NumberArrayFactory.AUTO_WITHOUT_PAGECACHE,
                    progressReporter );
            try ( GBPTreeCountsStore countsStore = createCountsStore( countsComputer ) )
            {
                countsStore.start();
                countsStore.checkpoint( IOLimiter.UNLIMITED );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private static class InvocationTrackingProgressReporter implements ProgressReporter
    {
        private boolean startInvoked;
        private boolean completeInvoked;

        @Override
        public void start( long max )
        {
            startInvoked = true;
        }

        @Override
        public void progress( long add )
        {
        }

        @Override
        public void completed()
        {
            completeInvoked = true;
        }

        boolean isStartInvoked()
        {
            return startInvoked;
        }

        boolean isCompleteInvoked()
        {
            return completeInvoked;
        }
    }
}
