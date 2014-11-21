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
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.StoreFactory.buildTypeDescriptorAndVersion;
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
        final CountsRecordState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore store = CountsStore.open( fs, pageCache, alphaStoreFile() ) )
        {
            // a transaction for creating the label and a transaction for the node
            assertEquals( BASE_TX_ID, store.lastTxId() );
            assertEquals( 0, store.totalRecordsStored() );
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
        final CountsRecordState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore store = CountsStore.open( fs, pageCache, betaStoreFile() ) )
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
        final CountsRecordState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore store = CountsStore.open( fs, pageCache, betaStoreFile() ) )
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
        final CountsRecordState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore store = CountsStore.open( fs, pageCache, betaStoreFile() ) )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1, store.lastTxId() );
//            assertEquals( 11, store.totalRecordsStored() ); // we do not support yet (label,type,label) counts
            assertEquals( 9, store.totalRecordsStored() );
            assertEquals( 2, get( store, nodeKey( -1 ) ) );
            assertEquals( 1, get( store, nodeKey( 0 ) ) );
            assertEquals( 1, get( store, nodeKey( 1 ) ) );
            assertEquals( 0, get( store, nodeKey( 2 ) ) );
            assertEquals( 0, get( store, nodeKey( 3 ) ) );
            assertEquals( 0, get( store, relationshipKey( -1, 0, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, -1 ) ) );
//            assertEquals( 1, get( store, relationshipKey( 1, 1, 0 ) ) ); // we do not support yet (label,type,label) counts
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
        final CountsRecordState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore store = CountsStore.open( fs, pageCache, betaStoreFile() ) )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1 + 1, store.lastTxId() );
//            assertEquals( 15, store.totalRecordsStored() ); // we do not support yet (label,type,label) counts
            assertEquals( 13, store.totalRecordsStored() );
            assertEquals( 4, get( store, nodeKey( -1 ) ) );
            assertEquals( 1, get( store, nodeKey( 0 ) ) );
            assertEquals( 1, get( store, nodeKey( 1 ) ) );
            assertEquals( 1, get( store, nodeKey( 2 ) ) );
            assertEquals( 0, get( store, nodeKey( 3 ) ) );
            assertEquals( 2, get( store, relationshipKey( -1, -1, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 0, -1 ) ) );
            assertEquals( 1, get( store, relationshipKey( -1, 1, -1 ) ) );
            assertEquals( 0, get( store, relationshipKey( -1, 2, -1 ) ) );
//            assertEquals( 1, get( store, relationshipKey( 0, 0, 2 ) ) ); // we do not support yet (label,type,label) counts
//            assertEquals( 0, get( store, relationshipKey( 2, 0, 0 ) ) ); // we do not support yet (label,type,label) counts
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
        final CountsRecordState countsState = CountsComputer.computeCounts( db );
        long lastCommittedTransactionId = getLastTxId( db );
        db.shutdown();

        cleanupCountsForRebuilding();

        rebuildCounts( countsState, lastCommittedTransactionId );

        try ( CountsStore store = CountsStore.open( fs, pageCache, betaStoreFile() ) )
        {
            assertEquals( BASE_TX_ID + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1, store.lastTxId() );
            assertEquals( 22, store.totalRecordsStored() );
//            assertEquals( 30, store.totalRecordsStored() ); // we do not support yet (label,type,label) counts
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
//            assertEquals( 1, get( store, relationshipKey( 0, 2, 2 ) ) ); // we do not support yet (label,type,label) counts
//            assertEquals( 0, get( store, relationshipKey( 2, 0, 0 ) ) ); // we do not support yet (label,type,label) counts
            assertEquals( 1, get( store, relationshipKey( -1, 1, 1 ) ) );
            assertEquals( 2, get( store, relationshipKey( -1, -1, 1 ) ) );
//            assertEquals( 0, get( store, relationshipKey( 1, -1, 2 ) ) ); // we do not support yet (label,type,label) counts
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
        pageCache = pcRule.getPageCache( fs );
    }

    private static final String COUNTS_STORE_BASE = NeoStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE;

    private File alphaStoreFile()
    {
        return new File( dir, COUNTS_STORE_BASE + CountsTracker.ALPHA );
    }

    private File betaStoreFile()
    {
        return new File( dir, COUNTS_STORE_BASE + CountsTracker.BETA );
    }

    private long getLastTxId( @SuppressWarnings( "deprecation" ) GraphDatabaseAPI db )
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

    private void rebuildCounts( CountsRecordState countsState, long lastCommittedTransactionId ) throws IOException
    {
        CountsTracker tracker = new CountsTracker( StringLogger.DEV_NULL, fs, pageCache,
                new File( dir, COUNTS_STORE_BASE ), BASE_TX_ID );
        countsState.accept( new CountsAccessor.Initializer( tracker ) );
        tracker.rotate( lastCommittedTransactionId );
        tracker.close();
    }

    private long get( CountsStore store, CountsKey key )
    {
        Register.DoubleLongRegister value = Registers.newDoubleLongRegister();
        store.get( key, value );
        return value.readSecond();
    }
}
