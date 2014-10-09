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
package org.neo4j.kernel.impl.store.counts;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.api.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.api.CountsKey.relationshipKey;
import static org.neo4j.kernel.impl.store.StoreFactory.buildTypeDescriptorAndVersion;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
import org.neo4j.kernel.impl.api.CountsAcceptor;
import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.api.CountsState;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

public class CountsComputerTest
{
    @Test
    public void shouldCreateAnEmptyCountsStoreFromAnEmptyDatabase() throws IOException
    {
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        final CountsState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore<CountsKey, Register.Long.Out> store =
                      CountsStore.open( fs, pageCache, alphaStoreFile(), RECORD_SERIALIZER, WRITER_FACTORY ) )
        {
            // a transaction for creating the label and a transaction for the node
            assertEquals( BASE_TX_ID, store.lastTxId() );
            assertEquals( 0, store.totalRecordsStored() );
        }
    }

    @Test
    public void shouldCreateACountsStoreWhenThereAreNodesInTheDB() throws IOException
    {
        final GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( DynamicLabel.label( "A" ) );
            db.createNode( DynamicLabel.label( "C" ) );
            db.createNode( DynamicLabel.label( "D" ) );
            db.createNode();
            tx.success();
        }
        final CountsState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore<CountsKey, Register.Long.Out> store =
                      CountsStore.open( fs, pageCache, betaStoreFile(), RECORD_SERIALIZER, WRITER_FACTORY ) )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1, store.lastTxId() );
            assertEquals( 4, store.totalRecordsStored() );
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
        final CountsState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore<CountsKey, Register.Long.Out> store =
                      CountsStore.open( fs, pageCache, betaStoreFile(), RECORD_SERIALIZER, WRITER_FACTORY ) )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1, store.lastTxId() );
            assertEquals( 3, store.totalRecordsStored() );
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
        final CountsState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore<CountsKey, Register.Long.Out> store =
                      CountsStore.open( fs, pageCache, betaStoreFile(), RECORD_SERIALIZER, WRITER_FACTORY ) )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1, store.lastTxId() );
            assertEquals( 11, store.totalRecordsStored() );
            assertEquals( 2, get( store, nodeKey( -1 ) ) );
            assertEquals( 1, get( store, nodeKey( 0 ) ) );
            assertEquals( 1, get( store, nodeKey( 1 ) ) );
            assertEquals( 0, get( store, nodeKey( 2 ) ) );
            assertEquals( 0, get( store, nodeKey( 3 ) ) );
            assertEquals( 0, get( store, relationshipKey( -1, 0, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( 1, 1, 0 ) ) );
        }
    }

    @Test
    public void shouldCreateACountsStoreWhenThereAreNodesAndRelationshipsInTheDB() throws IOException
    {
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
        final CountsState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore<CountsKey, Register.Long.Out> store =
                      CountsStore.open( fs, pageCache, betaStoreFile(), RECORD_SERIALIZER, WRITER_FACTORY ) )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1 + 1, store.lastTxId() );
            assertEquals( 15, store.totalRecordsStored() );
            assertEquals( 4, get( store, nodeKey( -1 ) ) );
            assertEquals( 1, get( store, nodeKey( 0 ) ) );
            assertEquals( 1, get( store, nodeKey( 1 ) ) );
            assertEquals( 1, get( store, nodeKey( 2 ) ) );
            assertEquals( 0, get( store, nodeKey( 3 ) ) );
            assertEquals( 2, get( store, relationshipKey( -1, -1, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 0, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, -1 ) ) );
            assertEquals( 0, get( store, relationshipKey( -1, 2, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( 0, 0, 2 ) ) );
            assertEquals( 0, get( store, relationshipKey( 2, 0, 0 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, 1 ) ) );
            assertEquals( 0, get( store, relationshipKey( -1, 0, 1 ) ) );
        }
    }

    @Test
    public void shouldCreateACountStoreWhenDBContainsDenseNodes() throws IOException
    {
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
        final CountsState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore<CountsKey, Register.Long.Out> store =
                      CountsStore.open( fs, pageCache, betaStoreFile(), RECORD_SERIALIZER, WRITER_FACTORY ) )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1, store.lastTxId() );
            assertEquals( 30, store.totalRecordsStored() );
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
            assertEquals( 1, get( store, relationshipKey( 0, 2, 2 ) ) );
            assertEquals( 0, get( store, relationshipKey( 2, 0, 0 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, 1 ) ) );
            assertEquals( 2, get( store, relationshipKey( -1, -1, 1 ) ) );
            assertEquals( 0, get( store, relationshipKey( 1, -1, 2 ) ) );
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

    @Before
    public void setup()
    {
        fs = fsRule.get();
        dir = testDir.directory( "dir" ).getAbsoluteFile();
        dbBuilder = new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabaseBuilder( dir.getPath() );
        pageCache = pcRule.getPageCache( fs, new Config() );
    }

    private static final String COUNTS_STORE_BASE = NeoStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE;
    private static final CountsRecordSerializer RECORD_SERIALIZER = new CountsRecordSerializer();
    private static final CountsStoreWriter.Factory WRITER_FACTORY = new CountsStoreWriter.Factory();

    private File alphaStoreFile()
    {
        return new File( dir, COUNTS_STORE_BASE + CountsTracker.ALPHA );
    }

    private File betaStoreFile()
    {
        return new File( dir, COUNTS_STORE_BASE + CountsTracker.BETA );
    }

    private long getLastTxId( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( NeoStore.class )
                .getLastCommittedTransactionId();
    }

    private void cleanupCountsForRebuilding()
    {
        fs.deleteFile( alphaStoreFile() );
        fs.deleteFile( betaStoreFile() );
        CountsTracker.createEmptyCountsStore( pageCache, new File( dir, COUNTS_STORE_BASE ),
                buildTypeDescriptorAndVersion( CountsTracker.STORE_DESCRIPTOR ) );
    }

    private void rebuildCounts( CountsState countsState, long lastCommittedTransactionId ) throws IOException
    {
        final CountsTracker tracker = new CountsTracker( fs, pageCache, new File( dir, COUNTS_STORE_BASE ) );
        countsState.accept( new CountsAcceptor.Initializer( tracker ) );
        tracker.rotate( lastCommittedTransactionId );
        tracker.close();
    }

    private <K extends Comparable<K>> long get( CountsStore<K, Register.Long.Out> store, K key )
    {
        Register.LongRegister value = Registers.newLongRegister();
        store.get( key, value );
        return value.read();
    }
}
