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

import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
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
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@PageCacheExtension
class CountsComputerTest
{
    private static final NullLogProvider LOG_PROVIDER = NullLogProvider.getInstance();
    private static final Config CONFIG = Config.defaults();

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    private DatabaseManagementServiceBuilder dbBuilder;

    @BeforeEach
    void setup()
    {
        dbBuilder = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
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
    void shouldCreateACountsStoreWhenThereAreNodesInTheDB()
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

        try ( CountsStore store = createCountsStore() )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 4, nodeCount( store, -1 ) );
            assertEquals( 1, nodeCount( store, 0 ) );
            assertEquals( 1, nodeCount( store, 1 ) );
            assertEquals( 1, nodeCount( store, 2 ) );
            assertEquals( 0, nodeCount( store, 3 ) );
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreUnusedNodeRecordsInTheDB()
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

        try ( CountsStore store = createCountsStore() )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 3, nodeCount( store, -1 ) );
            assertEquals( 1, nodeCount( store, 0 ) );
            assertEquals( 1, nodeCount( store, 1 ) );
            assertEquals( 0, nodeCount( store, 2 ) );
            assertEquals( 0, nodeCount( store, 3 ) );
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreUnusedRelationshipRecordsInTheDB()
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

        try ( CountsStore store = createCountsStore() )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 2, nodeCount( store, -1 ) );
            assertEquals( 1, nodeCount( store, 0 ) );
            assertEquals( 1, nodeCount( store, 1 ) );
            assertEquals( 0, nodeCount( store, 2 ) );
            assertEquals( 0, nodeCount( store, 3 ) );
            assertEquals( 0, relationshipCount( store, -1, 0, -1 ) );
            assertEquals( 1, relationshipCount( store, -1, 1, -1 ) );
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreNodesAndRelationshipsInTheDB()
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

        try ( CountsStore store = createCountsStore() )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 4, nodeCount( store, -1 ) );
            assertEquals( 1, nodeCount( store, 0 ) );
            assertEquals( 1, nodeCount( store, 1 ) );
            assertEquals( 1, nodeCount( store, 2 ) );
            assertEquals( 0, nodeCount( store, 3 ) );
            assertEquals( 2, relationshipCount( store, -1, -1, -1 ) );
            assertEquals( 1, relationshipCount( store, -1, 0, -1 ) );
            assertEquals( 1, relationshipCount( store, -1, 1, -1 ) );
            assertEquals( 0, relationshipCount( store, -1, 2, -1 ) );
            assertEquals( 1, relationshipCount( store, -1, 1, 1 ) );
            assertEquals( 0, relationshipCount( store, -1, 0, 1 ) );
        }
    }

    @Test
    void shouldCreateACountStoreWhenDBContainsDenseNodes()
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

        try ( CountsStore store = createCountsStore() )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 3, nodeCount( store, -1 ) );
            assertEquals( 1, nodeCount( store, 0 ) );
            assertEquals( 1, nodeCount( store, 1 ) );
            assertEquals( 1, nodeCount( store, 2 ) );
            assertEquals( 0, nodeCount( store, 3 ) );
            assertEquals( 4, relationshipCount( store, -1, -1, -1 ) );
            assertEquals( 1, relationshipCount( store, -1, 0, -1 ) );
            assertEquals( 1, relationshipCount( store, -1, 1, -1 ) );
            assertEquals( 1, relationshipCount( store, -1, 2, -1 ) );
            assertEquals( 1, relationshipCount( store, -1, 3, -1 ) );
            assertEquals( 0, relationshipCount( store, -1, 4, -1 ) );
            assertEquals( 1, relationshipCount( store, -1, 1, 1 ) );
            assertEquals( 2, relationshipCount( store, -1, -1, 1 ) );
            assertEquals( 3, relationshipCount( store, 0, -1, -1 ) );
        }
    }

    private File countsStoreFile()
    {
        return testDirectory.databaseLayout().countStore();
    }

    private static long getLastTxId( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
    }

    private void checkEmptyCountStore()
    {
        try ( CountsStore store = createCountsStore() )
        {
            store.start();
            assertEquals( BASE_TX_ID, store.txId() );
            // check that nothing is stored in the counts store by trying all combinations of tokens in the lower range
            for ( int s = 0; s < 10; s++ )
            {
                assertEquals( store.nodeCount( s, Registers.newDoubleLongRegister() ).readSecond(), 0 );
                for ( int e = 0; e < 10; e++ )
                {
                    for ( int t = 0; t < 10; t++ )
                    {
                        assertEquals( store.relationshipCount( s, t, e, Registers.newDoubleLongRegister() ).readSecond(), 0 );
                    }
                }
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private void cleanupCountsForRebuilding()
    {
        fileSystem.deleteFile( countsStoreFile() );
    }

    private GBPTreeCountsStore createCountsStore()
    {
        return createCountsStore( CountsBuilder.EMPTY );
    }

    private GBPTreeCountsStore createCountsStore( CountsBuilder builder )
    {
        return new GBPTreeCountsStore( pageCache, testDirectory.databaseLayout().countStore(), immediate(), builder, false, GBPTreeCountsStore.NO_MONITOR );
    }

    private void rebuildCounts( long lastCommittedTransactionId )
    {
        rebuildCounts( lastCommittedTransactionId, ProgressReporter.SILENT );
    }

    private void rebuildCounts( long lastCommittedTransactionId, ProgressReporter progressReporter )
    {
        cleanupCountsForRebuilding();

        IdGeneratorFactory idGenFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
        StoreFactory storeFactory = new StoreFactory( testDirectory.databaseLayout(), CONFIG, idGenFactory, pageCache, fileSystem, LOG_PROVIDER );
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
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private static long nodeCount( CountsAccessor store, int labelId )
    {
        Register.DoubleLongRegister value = Registers.newDoubleLongRegister();
        store.nodeCount( labelId, value );
        return value.readSecond();
    }

    private static long relationshipCount( CountsAccessor store, int startLabelId, int typeId, int endLabelId )
    {
        Register.DoubleLongRegister value = Registers.newDoubleLongRegister();
        store.relationshipCount( startLabelId, typeId, endLabelId, value );
        return value.readSecond();
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
