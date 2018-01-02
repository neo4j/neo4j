/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class CountsComputerTest
{
    @Test
    public void shouldCreateAnEmptyCountsStoreFromAnEmptyDatabase() throws IOException
    {
        @SuppressWarnings( "deprecation" )
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        rebuildCounts( lastCommittedTransactionId );

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker() );
            // a transaction for creating the label and a transaction for the node
            assertEquals( BASE_TX_ID, store.txId() );
            assertEquals( 0, store.totalEntriesStored() );
        }
    }

    @Test
    public void shouldCreateACountsStoreWhenThereAreNodesInTheDB() throws IOException
    {
        @SuppressWarnings( "deprecation" )
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( DynamicLabel.label( "A" ) );
            db.createNode( DynamicLabel.label( "C" ) );
            db.createNode( DynamicLabel.label( "D" ) );
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
    public void shouldCreateACountsStoreWhenThereAreUnusedNodeRecordsInTheDB() throws IOException
    {
        @SuppressWarnings( "deprecation" )
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( DynamicLabel.label( "A" ) );
            db.createNode( DynamicLabel.label( "C" ) );
            Node node = db.createNode( DynamicLabel.label( "D" ) );
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
    public void shouldCreateACountsStoreWhenThereAreUnusedRelationshipRecordsInTheDB() throws IOException
    {
        @SuppressWarnings( "deprecation" )
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = db.createNode( DynamicLabel.label( "A" ) );
            Node nodeC = db.createNode( DynamicLabel.label( "C" ) );
            Relationship rel = nodeA.createRelationshipTo( nodeC, DynamicRelationshipType.withName( "TYPE1" ) );
            nodeC.createRelationshipTo( nodeA, DynamicRelationshipType.withName( "TYPE2" ) );
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
    public void shouldCreateACountsStoreWhenThereAreNodesAndRelationshipsInTheDB() throws IOException
    {
        @SuppressWarnings( "deprecation" )
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = db.createNode( DynamicLabel.label( "A" ) );
            Node nodeC = db.createNode( DynamicLabel.label( "C" ) );
            Node nodeD = db.createNode( DynamicLabel.label( "D" ) );
            Node node = db.createNode();
            nodeA.createRelationshipTo( nodeD, DynamicRelationshipType.withName( "TYPE" ) );
            node.createRelationshipTo( nodeC, DynamicRelationshipType.withName( "TYPE2" ) );
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
    public void shouldCreateACountStoreWhenDBContainsDenseNodes() throws IOException
    {
        @SuppressWarnings( "deprecation" )
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.
                setConfig( GraphDatabaseSettings.dense_node_threshold, "2" ).newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = db.createNode( DynamicLabel.label( "A" ) );
            Node nodeC = db.createNode( DynamicLabel.label( "C" ) );
            Node nodeD = db.createNode( DynamicLabel.label( "D" ) );
            nodeA.createRelationshipTo( nodeA, DynamicRelationshipType.withName( "TYPE1" ) );
            nodeA.createRelationshipTo( nodeC, DynamicRelationshipType.withName( "TYPE2" ) );
            nodeA.createRelationshipTo( nodeD, DynamicRelationshipType.withName( "TYPE3" ) );
            nodeD.createRelationshipTo( nodeC, DynamicRelationshipType.withName( "TYPE4" ) );
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

    @Rule
    public PageCacheRule pcRule = new PageCacheRule();
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTestWithEphemeralFS( fsRule.get(),
            getClass() );

    private FileSystemAbstraction fs;
    private File dir;
    private GraphDatabaseBuilder dbBuilder;
    private PageCache pageCache;
    private Config emptyConfig;

    @Before
    public void setup()
    {
        fs = fsRule.get();
        dir = testDir.directory( "dir" ).getAbsoluteFile();
        dbBuilder = new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabaseBuilder( dir );
        pageCache = pcRule.getPageCache( fs );
        emptyConfig = new Config();
    }

    private static final String COUNTS_STORE_BASE = MetaDataStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE;

    private File alphaStoreFile()
    {
        return new File( dir, COUNTS_STORE_BASE + CountsTracker.LEFT );
    }

    private File betaStoreFile()
    {
        return new File( dir, COUNTS_STORE_BASE + CountsTracker.RIGHT );
    }

    private long getLastTxId( @SuppressWarnings( "deprecation" ) GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( NeoStores.class ).getMetaDataStore().getLastCommittedTransactionId();

    }

    private void cleanupCountsForRebuilding()
    {
        fs.deleteFile( alphaStoreFile() );
        fs.deleteFile( betaStoreFile() );
    }

    private CountsTracker createCountsTracker()
    {
        return new CountsTracker( NullLogProvider.getInstance(), fs, pageCache,
                emptyConfig, new File( dir, COUNTS_STORE_BASE ) );
    }

    private void rebuildCounts( long lastCommittedTransactionId ) throws IOException
    {
        cleanupCountsForRebuilding();

        StoreFactory storeFactory = new StoreFactory( fs, dir, pageCache, NullLogProvider.getInstance() );
        try ( Lifespan life = new Lifespan();
              NeoStores neoStores = storeFactory.openAllNeoStores() )
        {
            NodeStore nodeStore = neoStores.getNodeStore();
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            int highLabelId = (int) neoStores.getLabelTokenStore().getHighId();
            int highRelationshipTypeId = (int) neoStores.getRelationshipTypeTokenStore().getHighId();
            CountsComputer countsComputer = new CountsComputer(
                    lastCommittedTransactionId, nodeStore, relationshipStore, highLabelId, highRelationshipTypeId );
            CountsTracker countsTracker = createCountsTracker();
            life.add( countsTracker.setInitializer( countsComputer ) );
        }
    }

    private long get( CountsTracker store, CountsKey key )
    {
        Register.DoubleLongRegister value = Registers.newDoubleLongRegister();
        store.get( key, value );
        return value.readSecond();
    }
}
