/*
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
package org.neo4j.kernel.impl.store.counts;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.builder.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.TriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.AdversarialPageCacheGraphDatabaseFactory;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.store.counts.FileVersion.INITIAL_MINOR_VERSION;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class CountsRotationTest
{

    private final Label A = Label.label( "A" );
    private final Label B = Label.label( "B" );
    private final Label C = Label.label( "C" );

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
        dbBuilder = new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabaseBuilder( dir );
        pageCache = pcRule.getPageCache( fs );
    }

    @Test
    public void shouldCreateEmptyCountsTrackerStoreWhenCreatingDatabase() throws IOException
    {
        // GIVEN
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();

        // WHEN
        db.shutdown();

        // THEN
        assertTrue( fs.fileExists( alphaStoreFile() ) );
        assertFalse( fs.fileExists( betaStoreFile() ) );

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker( pageCache ) );

            assertEquals( BASE_TX_ID, store.txId() );
            assertEquals( INITIAL_MINOR_VERSION, store.minorVersion() );
            assertEquals( 0, store.totalEntriesStored() );
            assertEquals( 0, allRecords( store ).size() );
        }

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker( pageCache ) );
            assertEquals( BASE_TX_ID, store.txId() );
            assertEquals( INITIAL_MINOR_VERSION, store.minorVersion() );
            assertEquals( 0, store.totalEntriesStored() );
            assertEquals( 0, allRecords( store ).size() );
        }
    }

    @Test
    public void shouldRotateCountsStoreWhenClosingTheDatabase() throws IOException
    {
        // GIVEN
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( A );
            tx.success();
        }

        // WHEN
        db.shutdown();

        // THEN
        assertTrue( fs.fileExists( alphaStoreFile() ) );
        assertTrue( fs.fileExists( betaStoreFile() ) );

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker( pageCache ) );
            // a transaction for creating the label and a transaction for the node
            assertEquals( BASE_TX_ID + 1 + 1, store.txId() );
            assertEquals( INITIAL_MINOR_VERSION, store.minorVersion() );
            // one for all nodes and one for the created "A" label
            assertEquals( 1 + 1, store.totalEntriesStored() );
            assertEquals( 1 + 1, allRecords( store ).size() );
        }
    }

    @Test
    public void shouldRotateCountsStoreWhenRotatingLog() throws IOException
    {
        // GIVEN
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();

        // WHEN doing a transaction (actually two, the label-mini-tx also counts)
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( B );
            tx.success();
        }
        // and rotating the log (which implies flushing)
        checkPoint( db );
        // and creating another node after it
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( C );
            tx.success();
        }

        // THEN
        assertTrue( fs.fileExists( alphaStoreFile() ) );
        assertTrue( fs.fileExists( betaStoreFile() ) );

        final PageCache pageCache = db.getDependencyResolver().resolveDependency( PageCache.class );
        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker store = life.add( createCountsTracker( pageCache ) );
            // NOTE since the rotation happens before the second transaction is committed we do not see those changes
            // in the stats
            // a transaction for creating the label and a transaction for the node
            assertEquals( BASE_TX_ID + 1 + 1, store.txId() );
            assertEquals( INITIAL_MINOR_VERSION, store.minorVersion() );
            // one for all nodes and one for the created "B" label
            assertEquals( 1 + 1, store.totalEntriesStored() );
            assertEquals( 1 + 1, allRecords( store ).size() );
        }

        // on the other hand the tracker should read the correct value by merging data on disk and data in memory
        final CountsTracker tracker = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().getCounts();
        assertEquals( 1 + 1, tracker.nodeCount( -1, newDoubleLongRegister() ).readSecond() );

        final LabelTokenHolder holder = db.getDependencyResolver().resolveDependency( LabelTokenHolder.class );
        int labelId = holder.getIdByName( C.name() );
        assertEquals( 1, tracker.nodeCount( labelId, newDoubleLongRegister() ).readSecond() );

        db.shutdown();
    }

    @Test( timeout = 60_000 )
    public void possibleToShutdownDbWhenItIsNotHealthyAndNotAllTransactionsAreApplied() throws Exception
    {
        // adversary that makes page cache throw exception when node store is used
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, true ),
                NodeStore.class );
        adversary.disable();

        GraphDatabaseService db = new AdversarialPageCacheGraphDatabaseFactory( fs, adversary )
                .newEmbeddedDatabaseBuilder( dir )
                .setConfig( GraphDatabaseSettings.auth_store, new File( dir, "auth" ).getAbsolutePath() )
                .newGraphDatabase();

        CountDownLatch txStartLatch = new CountDownLatch( 1 );
        CountDownLatch txCommitLatch = new CountDownLatch( 1 );

        Future<?> result = ForkJoinPool.commonPool().submit( () -> {
            try ( Transaction tx = db.beginTx() )
            {
                txStartLatch.countDown();
                db.createNode();
                await( txCommitLatch );
                tx.success();
            }
        } );

        await( txStartLatch );

        adversary.enable();

        txCommitLatch.countDown();

        try
        {
            result.get();
            fail( "Exception expected" );
        }
        catch ( ExecutionException ee )
        {
            // transaction is expected to fail because write through the page cache fails
            assertThat( ee.getCause(), instanceOf( TransactionFailureException.class ) );
        }
        adversary.disable();

        // shutdown should complete without any problems
        db.shutdown();
    }

    private static void await( CountDownLatch latch )
    {
        try
        {
            boolean result = latch.await( 30, TimeUnit.SECONDS );
            if ( !result )
            {
                throw new RuntimeException( "Count down did not happen. Current count: " + latch.getCount() );
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private CountsTracker createCountsTracker( PageCache pageCache )
    {
        return new CountsTracker( NullLogProvider.getInstance(), fs, pageCache, Config.empty(),
                new File( dir.getPath(), COUNTS_STORE_BASE ) );
    }

    private void checkPoint( GraphDatabaseAPI db ) throws IOException
    {
        TriggerInfo triggerInfo = new SimpleTriggerInfo( "test" );
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint( triggerInfo );
    }

    private static final String COUNTS_STORE_BASE = MetaDataStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE;

    private File alphaStoreFile()
    {
        return new File( dir.getPath(), COUNTS_STORE_BASE + CountsTracker.LEFT );
    }

    private File betaStoreFile()
    {
        return new File( dir.getPath(), COUNTS_STORE_BASE + CountsTracker.RIGHT );
    }


    private Collection<Pair<? extends CountsKey, Long>> allRecords( CountsVisitor.Visitable store )
    {
        final Collection<Pair<? extends CountsKey, Long>> records = new ArrayList<>();
        store.accept( new CountsVisitor()
        {
            @Override
            public void visitNodeCount( int labelId, long count )
            {
                records.add( Pair.of( CountsKeyFactory.nodeKey( labelId ), count ) );
            }

            @Override
            public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
            {
                records.add( Pair.of( CountsKeyFactory.relationshipKey( startLabelId, typeId, endLabelId ), count ) );
            }

            @Override
            public void visitIndexStatistics( int labelId, int propertyKeyId, long updates, long size )
            {
                records.add( Pair.of( CountsKeyFactory.indexStatisticsKey( labelId, propertyKeyId ), size ) );
            }

            @Override
            public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
            {
                records.add( Pair.of( CountsKeyFactory.indexSampleKey( labelId, propertyKeyId ), size ) );
            }
        } );
        return records;
    }
}
