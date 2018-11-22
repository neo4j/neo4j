/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.common.ProgressReporter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

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

    private GraphDatabaseBuilder dbBuilder;

    @BeforeEach
    void setup()
    {
        dbBuilder = new TestGraphDatabaseFactory().setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .newImpermanentDatabaseBuilder( testDirectory.databaseDir() );
    }

    @Test
    void skipPopulationWhenNodeAndRelationshipStoresAreEmpty()
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        InvocationTrackingProgressReporter progressReporter = new InvocationTrackingProgressReporter();
        rebuildCounts( lastCommittedTransactionId, progressReporter );

        checkEmptyCountStore();
        assertTrue( progressReporter.isCompleteInvoked() );
        assertFalse( progressReporter.isStartInvoked() );
    }

    @Test
    void shouldCreateAnEmptyCountsStoreFromAnEmptyDatabase()
    {
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        checkEmptyCountStore();
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreNodesInTheDB()
    {
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( Label.label( "A" ) );
            db.createNode( Label.label( "C" ) );
            db.createNode( Label.label( "D" ) );
            db.createNode();
            tx.success();
        }
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker() );
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 4, store.totalEntriesStored() );
            assertEquals( 4, get( store, nodeKey( -1 ) ) );
            assertEquals( 1, get( store, nodeKey( 0 ) ) );
            assertEquals( 1, get( store, nodeKey( 1 ) ) );
            assertEquals( 1, get( store, nodeKey( 2 ) ) );
            assertEquals( 0, get( store, nodeKey( 3 ) ) );
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreUnusedNodeRecordsInTheDB()
    {
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( Label.label( "A" ) );
            db.createNode( Label.label( "C" ) );
            Node node = db.createNode( Label.label( "D" ) );
            db.createNode();
            node.delete();
            tx.success();
        }
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker() );
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 3, store.totalEntriesStored() );
            assertEquals( 3, get( store, nodeKey( -1 ) ) );
            assertEquals( 1, get( store, nodeKey( 0 ) ) );
            assertEquals( 1, get( store, nodeKey( 1 ) ) );
            assertEquals( 0, get( store, nodeKey( 2 ) ) );
            assertEquals( 0, get( store, nodeKey( 3 ) ) );
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreUnusedRelationshipRecordsInTheDB()
    {
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = db.createNode( Label.label( "A" ) );
            Node nodeC = db.createNode( Label.label( "C" ) );
            Relationship rel = nodeA.createRelationshipTo( nodeC, RelationshipType.withName( "TYPE1" ) );
            nodeC.createRelationshipTo( nodeA, RelationshipType.withName( "TYPE2" ) );
            rel.delete();
            tx.success();
        }
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker() );
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 9, store.totalEntriesStored() );
            assertEquals( 2, get( store, nodeKey( -1 ) ) );
            assertEquals( 1, get( store, nodeKey( 0 ) ) );
            assertEquals( 1, get( store, nodeKey( 1 ) ) );
            assertEquals( 0, get( store, nodeKey( 2 ) ) );
            assertEquals( 0, get( store, nodeKey( 3 ) ) );
            assertEquals( 0, get( store, relationshipKey( -1, 0, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, -1 ) ) );
        }
    }

    @Test
    void shouldCreateACountsStoreWhenThereAreNodesAndRelationshipsInTheDB()
    {
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = db.createNode( Label.label( "A" ) );
            Node nodeC = db.createNode( Label.label( "C" ) );
            Node nodeD = db.createNode( Label.label( "D" ) );
            Node node = db.createNode();
            nodeA.createRelationshipTo( nodeD, RelationshipType.withName( "TYPE" ) );
            node.createRelationshipTo( nodeC, RelationshipType.withName( "TYPE2" ) );
            tx.success();
        }
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker() );
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 13, store.totalEntriesStored() );
            assertEquals( 4, get( store, nodeKey( -1 ) ) );
            assertEquals( 1, get( store, nodeKey( 0 ) ) );
            assertEquals( 1, get( store, nodeKey( 1 ) ) );
            assertEquals( 1, get( store, nodeKey( 2 ) ) );
            assertEquals( 0, get( store, nodeKey( 3 ) ) );
            assertEquals( 2, get( store, relationshipKey( -1, -1, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 0, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, -1 ) ) );
            assertEquals( 0, get( store, relationshipKey( -1, 2, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, 1 ) ) );
            assertEquals( 0, get( store, relationshipKey( -1, 0, 1 ) ) );
        }
    }

    @Test
    void shouldCreateACountStoreWhenDBContainsDenseNodes()
    {
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.
                setConfig( GraphDatabaseSettings.dense_node_threshold, "2" ).newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = db.createNode( Label.label( "A" ) );
            Node nodeC = db.createNode( Label.label( "C" ) );
            Node nodeD = db.createNode( Label.label( "D" ) );
            nodeA.createRelationshipTo( nodeA, RelationshipType.withName( "TYPE1" ) );
            nodeA.createRelationshipTo( nodeC, RelationshipType.withName( "TYPE2" ) );
            nodeA.createRelationshipTo( nodeD, RelationshipType.withName( "TYPE3" ) );
            nodeD.createRelationshipTo( nodeC, RelationshipType.withName( "TYPE4" ) );
            tx.success();
        }
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker() );
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1, store.txId() );
            assertEquals( 22, store.totalEntriesStored() );
            assertEquals( 3, get( store, nodeKey( -1 ) ) );
            assertEquals( 1, get( store, nodeKey( 0 ) ) );
            assertEquals( 1, get( store, nodeKey( 1 ) ) );
            assertEquals( 1, get( store, nodeKey( 2 ) ) );
            assertEquals( 0, get( store, nodeKey( 3 ) ) );
            assertEquals( 4, get( store, relationshipKey( -1, -1, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 0, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 2, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 3, -1 ) ) );
            assertEquals( 0, get( store, relationshipKey( -1, 4, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, 1 ) ) );
            assertEquals( 2, get( store, relationshipKey( -1, -1, 1 ) ) );
            assertEquals( 3, get( store, relationshipKey( 0, -1, -1 ) ) );
        }
    }

    private File alphaStoreFile()
    {
        return testDirectory.databaseLayout().countStoreA();
    }

    private File betaStoreFile()
    {
        return testDirectory.databaseLayout().countStoreB();
    }

    private static long getLastTxId( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
    }

    private void checkEmptyCountStore()
    {
        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker() );
            assertEquals( BASE_TX_ID, store.txId() );
            assertEquals( 0, store.totalEntriesStored() );
        }
    }

    private void cleanupCountsForRebuilding()
    {
        fileSystem.deleteFile( alphaStoreFile() );
        fileSystem.deleteFile( betaStoreFile() );
    }

    private CountsTracker createCountsTracker()
    {
        return new CountsTracker( LOG_PROVIDER, fileSystem, pageCache, CONFIG, testDirectory.databaseLayout(), EmptyVersionContextSupplier.EMPTY );
    }

    private void rebuildCounts( long lastCommittedTransactionId )
    {
        rebuildCounts( lastCommittedTransactionId, ProgressReporter.SILENT );
    }

    private void rebuildCounts( long lastCommittedTransactionId, ProgressReporter progressReporter )
    {
        cleanupCountsForRebuilding();

        IdGeneratorFactory idGenFactory = new DefaultIdGeneratorFactory( fileSystem );
        StoreFactory storeFactory = new StoreFactory( testDirectory.databaseLayout(), CONFIG, idGenFactory, pageCache, fileSystem, LOG_PROVIDER,
                EmptyVersionContextSupplier.EMPTY );
        try ( Lifespan life = new Lifespan();
              NeoStores neoStores = storeFactory.openAllNeoStores() )
        {
            NodeStore nodeStore = neoStores.getNodeStore();
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            int highLabelId = (int) neoStores.getLabelTokenStore().getHighId();
            int highRelationshipTypeId = (int) neoStores.getRelationshipTypeTokenStore().getHighId();
            CountsComputer countsComputer = new CountsComputer(
                    lastCommittedTransactionId, nodeStore, relationshipStore, highLabelId, highRelationshipTypeId, NumberArrayFactory.AUTO_WITHOUT_PAGECACHE,
                    progressReporter );
            CountsTracker countsTracker = createCountsTracker();
            life.add( countsTracker.setInitializer( countsComputer ) );
        }
    }

    private static long get( CountsTracker store, CountsKey key )
    {
        Register.DoubleLongRegister value = Registers.newDoubleLongRegister();
        store.get( key, value );
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
