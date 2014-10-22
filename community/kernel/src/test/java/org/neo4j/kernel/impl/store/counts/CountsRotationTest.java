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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CountsRotationTest
{
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

        try ( CountsStore store = CountsStore.open( fs, pageCache, alphaStoreFile() ) )
        {
            assertEquals( TransactionIdStore.BASE_TX_ID, store.lastTxId() );
            assertEquals( 0, store.totalRecordsStored() );
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

        try ( CountsStore store = CountsStore.open( fs, pageCache, betaStoreFile() ) )
        {
            // a transaction for creating the label and a transaction for the node
            assertEquals( TransactionIdStore.BASE_TX_ID + 1 + 1, store.lastTxId() );
            // one for all nodes and one for the created "A" label
            assertEquals( 1 + 1, store.totalRecordsStored() );
            assertEquals( 1 + 1, allRecords( store ).size() );
        }
    }

    @Test
    public void shouldRotateCountsStoreWhenRotatingLog() throws IOException
    {
        // GIVEN
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbBuilder.
                setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1" ).newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( B );
            tx.success();
        }

        // WHEN
        // we do a log rotation _BEFORE_ this transaction
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( C );
            tx.success();
        }

        // THEN
        assertTrue( fs.fileExists( alphaStoreFile() ) );
        assertTrue( fs.fileExists( betaStoreFile() ) );

        final PageCache pageCache = db.getDependencyResolver().resolveDependency( PageCache.class );
        try ( CountsStore store = CountsStore.open( fs, pageCache, betaStoreFile() ) )
        {
            // NOTE since the rotation happens before the second transaction is committed we do not see those changes
            // in the stats
            // a transaction for creating the label and a transaction for the node
            assertEquals( TransactionIdStore.BASE_TX_ID + 1 + 1, store.lastTxId() );
            // one for all nodes and one for the created "B" label
            assertEquals( 1 + 1, store.totalRecordsStored() );
            assertEquals( 1 + 1, allRecords( store ).size() );
        }

        // on the other hand the tracker should read the correct value by merging data on disk and data in memory
        final CountsTracker tracker = db.getDependencyResolver().resolveDependency( NeoStore.class ).getCounts();
        assertEquals( 1 + 1, tracker.nodeCount( -1 ) );

        final LabelTokenHolder holder = db.getDependencyResolver().resolveDependency( LabelTokenHolder.class );
        int labelId = holder.getIdByName( C.name() );
        assertEquals( 1, tracker.nodeCount( labelId ) );

        db.shutdown();
    }

    private final Label A = DynamicLabel.label( "A" );
    private final Label B = DynamicLabel.label( "B" );
    private final Label C = DynamicLabel.label( "C" );

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

    private File alphaStoreFile()
    {
        return new File( dir.getPath(), COUNTS_STORE_BASE + CountsTracker.ALPHA );
    }

    private File betaStoreFile()
    {
        return new File( dir.getPath(), COUNTS_STORE_BASE + CountsTracker.BETA );
    }


    private Collection<Pair<CountsKey, Long>> allRecords( SortedKeyValueStore<CountsKey, Register.DoubleLongRegister> store )
    {
        final Collection<Pair<CountsKey, Long>> records = new ArrayList<>();
        store.accept( new KeyValueRecordVisitor<CountsKey, Register.DoubleLongRegister>()
        {
            private final Register.DoubleLongRegister valueRegister = Registers.newDoubleLongRegister();

            @Override
            public void visit( CountsKey key  )
            {
                records.add( Pair.of( key, valueRegister.readSecond() ) );
            }

            @Override
            public Register.DoubleLongRegister valueRegister()
            {
                return valueRegister;
            }
        } );
        return records;
    }
}
