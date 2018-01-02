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
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.function.Function;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.TriggerInfo;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.ThreadingRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.store.counts.FileVersion.INITIAL_MINOR_VERSION;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class CountsRotationTest
{

    private final Label A = DynamicLabel.label( "A" );
    private final Label B = DynamicLabel.label( "B" );
    private final Label C = DynamicLabel.label( "C" );

    @Rule
    public PageCacheRule pcRule = new PageCacheRule();
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTestWithEphemeralFS( fsRule.get(), getClass() );

    @Rule
    public ThreadingRule threadingRule = new ThreadingRule();

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
    public void rotationShouldNotCauseUnmappedFileProblem() throws IOException
    {
        // GIVEN
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.newGraphDatabase();

        final CountsTracker countStore = db.getDependencyResolver().resolveDependency( NeoStores.class ).getCounts();
        AtomicBoolean workerContinueFlag = new AtomicBoolean( true );
        AtomicLong lookupsCounter = new AtomicLong();
        int rotations = 100;
        for ( int i = 0; i < 5; i++ )
        {
            threadingRule.execute( countStoreLookup( workerContinueFlag, lookupsCounter ), countStore );
        }

        long startTxId = countStore.txId();
        for ( int i = 1; (i < rotations) || (lookupsCounter.get() == 0); i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( B );
                tx.success();
            }
            checkPoint( db );
        }
        workerContinueFlag.set( false );

        assertEquals( "Should perform at least 100 rotations.", rotations, Math.min( rotations, countStore.txId() - startTxId) );
        assertTrue( "Should perform more then 0 lookups without exceptions.", lookupsCounter.get() > 0 );
    }

    private static Function<CountsTracker,Void> countStoreLookup( final AtomicBoolean workerContinueFlag,
            final AtomicLong lookups )
    {
        return new Function<CountsTracker,Void>()
        {
            @Override
            public Void apply( CountsTracker countsTracker )
            {
                while ( workerContinueFlag.get() )
                {
                    Register.DoubleLongRegister register = Registers.newDoubleLongRegister();
                    countsTracker.get( CountsKeyFactory.nodeKey( 0 ), register );
                    lookups.incrementAndGet();
                }
                return null;
            }
        };
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
        final CountsTracker tracker = db.getDependencyResolver().resolveDependency( NeoStores.class ).getCounts();
        assertEquals( 1 + 1, tracker.nodeCount( -1, newDoubleLongRegister() ).readSecond() );

        final LabelTokenHolder holder = db.getDependencyResolver().resolveDependency( LabelTokenHolder.class );
        int labelId = holder.getIdByName( C.name() );
        assertEquals( 1, tracker.nodeCount( labelId, newDoubleLongRegister() ).readSecond() );

        db.shutdown();
    }

    private CountsTracker createCountsTracker(PageCache pageCache)
    {
        return new CountsTracker( NullLogProvider.getInstance(), fs, pageCache, emptyConfig,
                new File( dir.getPath(), COUNTS_STORE_BASE ) );
    }

    private void checkPoint( GraphDatabaseAPI db ) throws IOException
    {
        TriggerInfo triggerInfo = new SimpleTriggerInfo( "test" );
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint( triggerInfo );
    }

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
