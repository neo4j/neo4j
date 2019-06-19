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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder.DatabaseCreator;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.ByteBufferFactory;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.storemigration.ExistingTargetStrategy;
import org.neo4j.kernel.impl.storemigration.FileOperation;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.RecoveryMonitor;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.ThreadPoolJobScheduler;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.AdversarialPageCacheGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Long.max;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.kernel.configuration.Config.defaults;

public class RecoveryIT
{
    private static final String[] TOKENS = new String[] {"Token1", "Token2", "Token3", "Token4", "Token5"};

    private final TestDirectory directory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final RandomRule random = new RandomRule();
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );

    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( fileSystemRule ).around( directory );

    @Test
    public void idGeneratorsRebuildAfterRecovery() throws IOException
    {
        GraphDatabaseService database = startDatabase( directory.databaseDir() );
        int numberOfNodes = 10;
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int nodeIndex = 0; nodeIndex < numberOfNodes; nodeIndex++ )
            {
                database.createNode();
            }
            transaction.success();
        }

        // copying only transaction log simulate non clean shutdown db that should be able to recover just from logs
        File restoreDbStoreDir = copyTransactionLogs();

        GraphDatabaseService recoveredDatabase = startDatabase( restoreDbStoreDir );
        try ( Transaction tx = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfNodes, count( recoveredDatabase.getAllNodes() ) );

            // Make sure id generator has been rebuilt so this doesn't throw null pointer exception
            recoveredDatabase.createNode();
        }

        database.shutdown();
        recoveredDatabase.shutdown();
    }

    @Test
    public void reportProgressOnRecovery() throws IOException
    {
        GraphDatabaseService database = startDatabase( directory.databaseDir() );
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                database.createNode();
                transaction.success();
            }
        }

        File restoreDbStoreDir = copyTransactionLogs();
        GraphDatabaseService recoveredDatabase = startDatabase( restoreDbStoreDir );
        try ( Transaction transaction = recoveredDatabase.beginTx() )
        {
            assertEquals( 10, count( recoveredDatabase.getAllNodes() ) );
        }
        logProvider.rawMessageMatcher().assertContains( "10% completed" );
        logProvider.rawMessageMatcher().assertContains( "100% completed" );

        database.shutdown();
        recoveredDatabase.shutdown();
    }

    @Test
    public void shouldRecoverIdsCorrectlyWhenWeCreateAndDeleteANodeInTheSameRecoveryRun() throws IOException
    {
        GraphDatabaseService database = startDatabase( directory.databaseDir() );
        Label testLabel = Label.label( "testLabel" );
        final String propertyToDelete = "propertyToDelete";
        final String validPropertyName = "validProperty";

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.addLabel( testLabel );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = findNodeByLabel( database, testLabel );
            node.setProperty( propertyToDelete, createLongString() );
            node.setProperty( validPropertyName, createLongString() );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = findNodeByLabel( database, testLabel );
            node.removeProperty( propertyToDelete );
            transaction.success();
        }

        // copying only transaction log simulate non clean shutdown db that should be able to recover just from logs
        File restoreDbStoreDir = copyTransactionLogs();

        // database should be restored and node should have expected properties
        GraphDatabaseService recoveredDatabase = startDatabase( restoreDbStoreDir );
        try ( Transaction ignored = recoveredDatabase.beginTx() )
        {
            Node node = findNodeByLabel( recoveredDatabase, testLabel );
            assertFalse( node.hasProperty( propertyToDelete ) );
            assertTrue( node.hasProperty( validPropertyName ) );
        }

        database.shutdown();
        recoveredDatabase.shutdown();
    }

    @Test( timeout = 60_000 )
    public void recoveryShouldFixPartiallyAppliedSchemaIndexUpdates()
    {
        Label label = Label.label( "Foo" );
        String property = "Bar";

        // cause failure during 'relationship.delete()' command application
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, true ),
                Command.RelationshipCommand.class );
        adversary.disable();

        File databaseDir = directory.databaseDir();
        GraphDatabaseService db = AdversarialPageCacheGraphDatabaseFactory.create( fileSystemRule.get(), adversary )
                .newEmbeddedDatabaseBuilder( databaseDir )
                .newGraphDatabase();
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
                tx.success();
            }

            long relationshipId = createRelationship( db );

            TransactionFailureException txFailure = null;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( label );
                node.setProperty( property, "B" );
                db.getRelationshipById( relationshipId ).delete(); // this should fail because of the adversary
                tx.success();
                adversary.enable();
            }
            catch ( TransactionFailureException e )
            {
                txFailure = e;
            }
            assertNotNull( txFailure );
            adversary.disable();

            healthOf( db ).healed(); // heal the db so it is possible to inspect the data

            // now we can observe partially committed state: node is in the index and relationship still present
            try ( Transaction tx = db.beginTx() )
            {
                assertNotNull( findNode( db, label, property, "B" ) );
                assertNotNull( db.getRelationshipById( relationshipId ) );
                tx.success();
            }

            healthOf( db ).panic( txFailure.getCause() ); // panic the db again to force recovery on the next startup

            // restart the database, now with regular page cache
            File databaseDirectory = ((GraphDatabaseAPI) db).databaseLayout().databaseDirectory();
            db.shutdown();
            db = startDatabase( databaseDirectory );

            // now we observe correct state: node is in the index and relationship is removed
            try ( Transaction tx = db.beginTx() )
            {
                assertNotNull( findNode( db, label, property, "B" ) );
                assertRelationshipNotExist( db, relationshipId );
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldSeeSameIndexUpdatesDuringRecoveryAsFromNormalIndexApplication() throws Exception
    {
        // Previously indexes weren't really participating in recovery, instead there was an after-phase
        // where nodes that was changed during recovery were reindexed. Do be able to do this reindexing
        // the index had to support removing arbitrary entries based on node id alone. Lucene can do this,
        // but at least at the time of writing this not the native index. For this the recovery process
        // was changed to rewind neostore back to how it looked at the last checkpoint and then replay
        // transactions from that point, including indexes. This test verifies that there's no mismatch
        // between applying transactions normally and recovering them after a crash, index update wise.

        // given
        File storeDir = directory.absolutePath();
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        UpdateCapturingIndexProvider updateCapturingIndexProvider = new UpdateCapturingIndexProvider( IndexProvider.EMPTY, new HashMap<>() );
        GraphDatabaseAPI db = startDatabase( storeDir, fs, updateCapturingIndexProvider );
        Label label = TestLabels.LABEL_ONE;
        String key1 = "key1";
        String key2 = "key2";
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( key1 ).create();
            db.schema().indexFor( label ).on( key1 ).on( key2 ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
        checkPoint( db );

        produceRandomNodePropertyAndLabelUpdates( db, random.intBetween( 20, 40 ), label, key1, key2 );
        checkPoint( db );
        Map<Long,Collection<IndexEntryUpdate<?>>> updatesAtLastCheckPoint = updateCapturingIndexProvider.snapshot();

        // when
        produceRandomNodePropertyAndLabelUpdates( db, random.intBetween( 40, 100 ), label, key1, key2 );

        // Snapshot
        flush( db );
        EphemeralFileSystemAbstraction crashedFs = fs.snapshot();
        Map<Long,Collection<IndexEntryUpdate<?>>> updatesAtCrash = updateCapturingIndexProvider.snapshot();

        // Crash and start anew
        UpdateCapturingIndexProvider recoveredUpdateCapturingIndexProvider =
                new UpdateCapturingIndexProvider( IndexProvider.EMPTY, updatesAtLastCheckPoint );
        long lastCommittedTxIdBeforeRecovered = lastCommittedTxId( db );
        db.shutdown();
        fs.close();

        db = startDatabase( storeDir, crashedFs, recoveredUpdateCapturingIndexProvider );
        long lastCommittedTxIdAfterRecovered = lastCommittedTxId( db );
        Map<Long,Collection<IndexEntryUpdate<?>>> updatesAfterRecovery = recoveredUpdateCapturingIndexProvider.snapshot();

        // then
        assertEquals( lastCommittedTxIdBeforeRecovered, lastCommittedTxIdAfterRecovered );
        assertSameUpdates( updatesAtCrash, updatesAfterRecovery );
        db.shutdown();
        crashedFs.close();
    }

    @Test
    public void shouldSeeTheSameRecordsAtCheckpointAsAfterReverseRecovery() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabase( directory.databaseDir() );
        produceRandomGraphUpdates( db, 100 );
        checkPoint( db );
        EphemeralFileSystemAbstraction checkPointFs = fs.snapshot();

        // when
        produceRandomGraphUpdates( db, 100 );
        flush( db );
        EphemeralFileSystemAbstraction crashedFs = fs.snapshot();
        db.shutdown();
        fs.close();
        Monitors monitors = new Monitors();
        AtomicReference<PageCache> pageCache = new AtomicReference<>();
        AtomicReference<EphemeralFileSystemAbstraction> reversedFs = new AtomicReference<>();
        monitors.addMonitorListener( new RecoveryMonitor()
        {
            @Override
            public void reverseStoreRecoveryCompleted( long checkpointTxId )
            {
                try
                {
                    // Flush the page cache which will fished out of the PlatformModule at the point of constructing the database
                    pageCache.get().flushAndForce();
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }

                // The stores should now be equal in content to the db as it was right after the checkpoint.
                // Grab a snapshot so that we can compare later.
                reversedFs.set( crashedFs.snapshot() );
            }
        } );
        new TestGraphDatabaseFactory()
                {
                    // This nested constructing is done purely to be able to fish out PlatformModule
                    // (and its PageCache inside it). It would be great if this could be done in a prettier way.

                    @Override
                    protected DatabaseCreator createImpermanentDatabaseCreator( File storeDir, TestGraphDatabaseFactoryState state )
                    {
                        return new GraphDatabaseBuilder.DatabaseCreator()
                        {

                            @Override
                            public GraphDatabaseService newDatabase( @Nonnull Config config )
                            {
                                TestGraphDatabaseFacadeFactory factory = new TestGraphDatabaseFacadeFactory( state, true )
                                {
                                    @Override
                                    protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies )
                                    {
                                        PlatformModule platform = super.createPlatform( storeDir, config, dependencies );
                                        // nice way of getting the page cache dependency before db is created, huh?
                                        pageCache.set( platform.pageCache );
                                        return platform;
                                    }
                                };
                                return factory.newFacade( storeDir, config, newDependencies( state.databaseDependencies() ) );
                            }
                        };
                    }
                }
                .setFileSystem( crashedFs )
                .setMonitors( monitors )
                .newImpermanentDatabase( directory.databaseDir() )
                .shutdown();

        // then
        fs.close();

        try
        {
            // Here we verify that the neostore contents, record by record are exactly the same when comparing
            // the store as it was right after the checkpoint with the store as it was right after reverse recovery completed.
            assertSameStoreContents( checkPointFs, reversedFs.get(), directory.databaseLayout() );
        }
        finally
        {
            checkPointFs.close();
            reversedFs.get().close();
        }
    }

    private static long lastCommittedTxId( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
    }

    private static void assertSameStoreContents( EphemeralFileSystemAbstraction fs1, EphemeralFileSystemAbstraction fs2, DatabaseLayout databaseLayout )
    {
        NullLogProvider logProvider = NullLogProvider.getInstance();
        VersionContextSupplier contextSupplier = EmptyVersionContextSupplier.EMPTY;
        try (
                ThreadPoolJobScheduler jobScheduler = new ThreadPoolJobScheduler();
                PageCache pageCache1 = new ConfiguringPageCacheFactory( fs1, defaults(), PageCacheTracer.NULL,
                        PageCursorTracerSupplier.NULL, NullLog.getInstance(), contextSupplier, jobScheduler )
                        .getOrCreatePageCache();
                PageCache pageCache2 = new ConfiguringPageCacheFactory( fs2, defaults(), PageCacheTracer.NULL,
                        PageCursorTracerSupplier.NULL, NullLog.getInstance(), contextSupplier, jobScheduler )
                        .getOrCreatePageCache();
                NeoStores store1 = new StoreFactory( databaseLayout, defaults(), new DefaultIdGeneratorFactory( fs1 ),
                        pageCache1, fs1, logProvider, contextSupplier ).openAllNeoStores();
                NeoStores store2 = new StoreFactory( databaseLayout, defaults(), new DefaultIdGeneratorFactory( fs2 ),
                        pageCache2, fs2, logProvider, contextSupplier ).openAllNeoStores()
                )
        {
            for ( StoreType storeType : StoreType.values() )
            {
                if ( storeType.isRecordStore() )
                {
                    assertSameStoreContents( store1.getRecordStore( storeType ), store2.getRecordStore( storeType ) );
                }
            }
        }
    }

    private static <RECORD extends AbstractBaseRecord> void assertSameStoreContents( RecordStore<RECORD> store1, RecordStore<RECORD> store2 )
    {
        long highId1 = store1.getHighId();
        long highId2 = store2.getHighId();
        long maxHighId = max( highId1, highId2 );
        RECORD record1 = store1.newRecord();
        RECORD record2 = store2.newRecord();
        for ( long id = store1.getNumberOfReservedLowIds(); id < maxHighId; id++ )
        {
            store1.getRecord( id, record1, RecordLoad.CHECK );
            store2.getRecord( id, record2, RecordLoad.CHECK );
            assertEquals( record1, record2 );
        }
    }

    private static void flush( GraphDatabaseService db )
    {
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( StorageEngine.class ).flushAndForce( IOLimiter.UNLIMITED );
    }

    private static void checkPoint( GraphDatabaseService db ) throws IOException
    {
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( CheckPointer.class )
                .forceCheckPoint( new SimpleTriggerInfo( "Manual trigger" ) );
    }

    private void produceRandomGraphUpdates( GraphDatabaseService db, int numberOfTransactions )
    {
        // Load all existing nodes
        List<Node> nodes = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            try ( ResourceIterator<Node> allNodes = db.getAllNodes().iterator() )
            {
                while ( allNodes.hasNext() )
                {
                    nodes.add( allNodes.next() );
                }
            }
            tx.success();
        }

        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            int transactionSize = random.intBetween( 1, 30 );
            try ( Transaction tx = db.beginTx() )
            {
                for ( int j = 0; j < transactionSize; j++ )
                {
                    float operationType = random.nextFloat();
                    float operation = random.nextFloat();
                    if ( operationType < 0.5 )
                    {   // create
                        if ( operation < 0.5 )
                        {   // create node (w/ random label, prop)
                            Node node = db.createNode( random.nextBoolean() ? array( randomLabel() ) : new Label[0] );
                            if ( random.nextBoolean() )
                            {
                                node.setProperty( randomKey(), random.nextValueAsObject() );
                            }
                        }
                        else
                        {   // create relationship (w/ random prop)
                            if ( !nodes.isEmpty() )
                            {
                                Relationship relationship = random.among( nodes )
                                        .createRelationshipTo( random.among( nodes ), randomRelationshipType() );
                                if ( random.nextBoolean() )
                                {
                                    relationship.setProperty( randomKey(), random.nextValueAsObject() );
                                }
                            }
                        }
                    }
                    else if ( operationType < 0.8 )
                    {   // change
                        if ( operation < 0.25 )
                        {   // add label
                            random.among( nodes, node -> node.addLabel( randomLabel() ) );
                        }
                        else if ( operation < 0.5 )
                        {   // remove label
                            random.among( nodes, node -> node.removeLabel( randomLabel() ) );
                        }
                        else if ( operation < 0.75 )
                        {   // set node property
                            random.among( nodes, node -> node.setProperty( randomKey(), random.nextValueAsObject() ) );
                        }
                        else
                        {   // set relationship property
                            onRandomRelationship( nodes,
                                    relationship -> relationship.setProperty( randomKey(), random.nextValueAsObject() ) );
                        }
                    }
                    else
                    {   // delete

                        if ( operation < 0.25 )
                        {   // remove node property
                            random.among( nodes, node -> node.removeProperty( randomKey() ) );
                        }
                        else if ( operation < 0.5 )
                        {   // remove relationship property
                            onRandomRelationship( nodes, relationship -> relationship.removeProperty( randomKey() ) );
                        }
                        else if ( operation < 0.9 )
                        {   // delete relationship
                            onRandomRelationship( nodes, Relationship::delete );
                        }
                        else
                        {   // delete node
                            random.among( nodes, node ->
                            {
                                for ( Relationship relationship : node.getRelationships() )
                                {
                                    relationship.delete();
                                }
                                node.delete();
                                nodes.remove( node );
                            } );
                        }
                    }
                }
                tx.success();
            }
        }
    }

    private void onRandomRelationship( List<Node> nodes, Consumer<Relationship> action )
    {
        random.among( nodes, node -> random.among( asList( node.getRelationships() ), action ) );
    }

    private RelationshipType randomRelationshipType()
    {
        return RelationshipType.withName( random.among( TOKENS ) );
    }

    private String randomKey()
    {
        return random.among( TOKENS );
    }

    private Label randomLabel()
    {
        return Label.label( random.among( TOKENS ) );
    }

    private void assertSameUpdates( Map<Long,Collection<IndexEntryUpdate<?>>> updatesAtCrash,
            Map<Long,Collection<IndexEntryUpdate<?>>> recoveredUpdatesSnapshot )
    {
        // The UpdateCapturingIndexProvider just captures updates made to indexes. The order in this test
        // should be the same during online transaction application and during recovery since everything
        // is single threaded. However there's a bunch of placing where entries and keys and what not
        // ends up in hash maps and so may change order. The super important thing we need to verify is
        // that updates for a particular transaction are the same during normal application and recovery,
        // regardless of ordering differences within the transaction.

        Map<Long,Map<Long,Collection<IndexEntryUpdate<?>>>> crashUpdatesPerNode = splitPerNode( updatesAtCrash );
        Map<Long,Map<Long,Collection<IndexEntryUpdate<?>>>> recoveredUpdatesPerNode = splitPerNode( recoveredUpdatesSnapshot );
        assertEquals( crashUpdatesPerNode, recoveredUpdatesPerNode );
    }

    private static Map<Long,Map<Long,Collection<IndexEntryUpdate<?>>>> splitPerNode( Map<Long,Collection<IndexEntryUpdate<?>>> updates )
    {
        Map<Long,Map<Long,Collection<IndexEntryUpdate<?>>>> result = new HashMap<>();
        updates.forEach( ( indexId, indexUpdates ) -> result.put( indexId, splitPerNode( indexUpdates ) ) );
        return result;
    }

    private static Map<Long,Collection<IndexEntryUpdate<?>>> splitPerNode( Collection<IndexEntryUpdate<?>> updates )
    {
        Map<Long,Collection<IndexEntryUpdate<?>>> perNode = new HashMap<>();
        updates.forEach( update -> perNode.computeIfAbsent( update.getEntityId(), nodeId -> new ArrayList<>() ).add( update ) );
        return perNode;
    }

    private void produceRandomNodePropertyAndLabelUpdates( GraphDatabaseService db, int numberOfTransactions, Label label, String... keys )
    {
        // Load all existing nodes
        List<Node> nodes = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            try ( ResourceIterator<Node> allNodes = db.getAllNodes().iterator() )
            {
                while ( allNodes.hasNext() )
                {
                    nodes.add( allNodes.next() );
                }
            }
            tx.success();
        }

        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            int transactionSize = random.intBetween( 1, 30 );
            try ( Transaction tx = db.beginTx() )
            {
                for ( int j = 0; j < transactionSize; j++ )
                {
                    float operation = random.nextFloat();
                    if ( operation < 0.1 )
                    {   // Delete node
                        if ( !nodes.isEmpty() )
                        {
                            nodes.remove( random.nextInt( nodes.size() ) ).delete();
                        }
                    }
                    else if ( operation < 0.3 )
                    {   // Create node
                        Node node = db.createNode( random.nextBoolean() ? array( label ) : new Label[0] );
                        for ( String key : keys )
                        {
                            if ( random.nextBoolean() )
                            {
                                node.setProperty( key, random.nextValueAsObject() );
                            }
                        }
                        nodes.add( node );
                    }
                    else if ( operation < 0.4 )
                    {   // Remove label
                        random.among( nodes, node -> node.removeLabel( label ) );
                    }
                    else if ( operation < 0.6 )
                    {   // Add label
                        random.among( nodes, node -> node.addLabel( label ) );
                    }
                    else if ( operation < 0.85 )
                    {   // Set property
                        random.among( nodes, node -> node.setProperty( random.among( keys ), random.nextValueAsObject() ) );
                    }
                    else
                    {   // Remove property
                        random.among( nodes, node -> node.removeProperty( random.among( keys ) ) );
                    }
                }
                tx.success();
            }
        }
    }

    private static Node findNodeByLabel( GraphDatabaseService database, Label testLabel )
    {
        try ( ResourceIterator<Node> nodes = database.findNodes( testLabel ) )
        {
            return nodes.next();
        }
    }

    private static Node findNode( GraphDatabaseService db, Label label, String property, String value )
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( label, property, value ) )
        {
            return Iterators.single( nodes );
        }
    }

    private static long createRelationship( GraphDatabaseService db )
    {
        long relationshipId;
        try ( Transaction tx = db.beginTx() )
        {
            Node start = db.createNode( Label.label( System.currentTimeMillis() + "" ) );
            Node end = db.createNode( Label.label( System.currentTimeMillis() + "" ) );
            relationshipId = start.createRelationshipTo( end, withName( "KNOWS" ) ).getId();
            tx.success();
        }
        return relationshipId;
    }

    private static void assertRelationshipNotExist( GraphDatabaseService db, long id )
    {
        try
        {
            db.getRelationshipById( id );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NotFoundException.class ) );
        }
    }

    private static DatabaseHealth healthOf( GraphDatabaseService db )
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        return resolver.resolveDependency( DatabaseHealth.class );
    }

    private static String createLongString()
    {
        String[] strings = new String[(int) ByteUnit.kibiBytes( 2 )];
        Arrays.fill( strings, "a" );
        return Arrays.toString( strings );
    }

    private File copyTransactionLogs() throws IOException
    {
        File restoreDbStore = directory.storeDir( "restore-db" );
        File restoreDbStoreDir = directory.databaseDir( restoreDbStore );
        move( fileSystemRule.get(), this.directory.databaseDir(), restoreDbStoreDir );
        return restoreDbStoreDir;
    }

    private static void move( FileSystemAbstraction fs, File fromDirectory, File toDirectory ) throws IOException
    {
        assertTrue( fs.isDirectory( fromDirectory ) );
        assertTrue( fs.isDirectory( toDirectory ) );

        LogFiles transactionLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( fromDirectory, fs ).build();
        File[] logFiles = transactionLogFiles.logFiles();
        for ( File logFile : logFiles )
        {
            FileOperation.MOVE.perform( fs, logFile.getName(), fromDirectory, false, toDirectory,
                    ExistingTargetStrategy.FAIL );
        }
    }

    private static GraphDatabaseAPI startDatabase( File storeDir, EphemeralFileSystemAbstraction fs, UpdateCapturingIndexProvider indexProvider )
    {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .setFileSystem( fs )
                .setKernelExtensions( singletonList( new IndexExtensionFactory( indexProvider ) ) )
                .newImpermanentDatabaseBuilder( storeDir )
                .setConfig( default_schema_provider, indexProvider.getProviderDescriptor().name() )
                .newGraphDatabase();
    }

    private GraphDatabaseService startDatabase( File storeDir )
    {
        return new TestGraphDatabaseFactory().setInternalLogProvider( logProvider ).newEmbeddedDatabase( storeDir );
    }

    public class UpdateCapturingIndexProvider extends IndexProvider
    {
        private final IndexProvider actual;
        private final Map<Long,UpdateCapturingIndexAccessor> indexes = new ConcurrentHashMap<>();
        private final Map<Long,Collection<IndexEntryUpdate<?>>> initialUpdates;

        UpdateCapturingIndexProvider( IndexProvider actual, Map<Long,Collection<IndexEntryUpdate<?>>> initialUpdates )
        {
            super( actual );
            this.actual = actual;
            this.initialUpdates = initialUpdates;
        }

        @Override
        public IndexPopulator getPopulator( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory )
        {
            return actual.getPopulator( descriptor, samplingConfig, bufferFactory );
        }

        @Override
        public IndexAccessor getOnlineAccessor( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
                throws IOException
        {
            IndexAccessor actualAccessor = actual.getOnlineAccessor( descriptor, samplingConfig );
            return indexes.computeIfAbsent( descriptor.getId(), id -> new UpdateCapturingIndexAccessor( actualAccessor, initialUpdates.get( id ) ) );
        }

        @Override
        public String getPopulationFailure( StoreIndexDescriptor descriptor ) throws IllegalStateException
        {
            return actual.getPopulationFailure( descriptor );
        }

        @Override
        public InternalIndexState getInitialState( StoreIndexDescriptor descriptor )
        {
            return actual.getInitialState( descriptor );
        }

        @Override
        public IndexCapability getCapability( StoreIndexDescriptor descriptor )
        {
            return actual.getCapability( descriptor );
        }

        @Override
        public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
        {
            return actual.storeMigrationParticipant( fs, pageCache );
        }

        public Map<Long,Collection<IndexEntryUpdate<?>>> snapshot()
        {
            Map<Long,Collection<IndexEntryUpdate<?>>> result = new HashMap<>();
            indexes.forEach( ( indexId, index ) -> result.put( indexId, index.snapshot() ) );
            return result;
        }
    }

    public class UpdateCapturingIndexAccessor implements IndexAccessor
    {
        private final IndexAccessor actual;
        private final Collection<IndexEntryUpdate<?>> updates = new ArrayList<>();

        UpdateCapturingIndexAccessor( IndexAccessor actual, Collection<IndexEntryUpdate<?>> initialUpdates )
        {
            this.actual = actual;
            if ( initialUpdates != null )
            {
                this.updates.addAll( initialUpdates );
            }
        }

        @Override
        public void drop()
        {
            actual.drop();
        }

        @Override
        public IndexUpdater newUpdater( IndexUpdateMode mode )
        {
            return wrap( actual.newUpdater( mode ) );
        }

        private IndexUpdater wrap( IndexUpdater actual )
        {
            return new UpdateCapturingIndexUpdater( actual, updates );
        }

        @Override
        public void force( IOLimiter ioLimiter )
        {
            actual.force( ioLimiter );
        }

        @Override
        public void refresh()
        {
            actual.refresh();
        }

        @Override
        public void close()
        {
            actual.close();
        }

        @Override
        public IndexReader newReader()
        {
            return actual.newReader();
        }

        @Override
        public BoundedIterable<Long> newAllEntriesReader()
        {
            return actual.newAllEntriesReader();
        }

        @Override
        public ResourceIterator<File> snapshotFiles()
        {
            return actual.snapshotFiles();
        }

        @Override
        public void verifyDeferredConstraints( NodePropertyAccessor propertyAccessor ) throws IndexEntryConflictException
        {
            actual.verifyDeferredConstraints( propertyAccessor );
        }

        @Override
        public boolean isDirty()
        {
            return actual.isDirty();
        }

        public Collection<IndexEntryUpdate<?>> snapshot()
        {
            return new ArrayList<>( updates );
        }
    }

    public class UpdateCapturingIndexUpdater implements IndexUpdater
    {
        private final IndexUpdater actual;
        private final Collection<IndexEntryUpdate<?>> updatesTarget;

        UpdateCapturingIndexUpdater( IndexUpdater actual, Collection<IndexEntryUpdate<?>> updatesTarget )
        {
            this.actual = actual;
            this.updatesTarget = updatesTarget;
        }

        @Override
        public void process( IndexEntryUpdate<?> update ) throws IndexEntryConflictException
        {
            actual.process( update );
            updatesTarget.add( update );
        }

        @Override
        public void close() throws IndexEntryConflictException
        {
            actual.close();
        }
    }

    private static class IndexExtensionFactory extends KernelExtensionFactory<IndexExtensionFactory.Dependencies>
    {
        private final IndexProvider indexProvider;

        interface Dependencies
        {
        }

        IndexExtensionFactory( IndexProvider indexProvider )
        {
            super( "customExtension" );
            this.indexProvider = indexProvider;
        }

        @Override
        public Lifecycle newInstance( KernelContext context, Dependencies dependencies )
        {
            return indexProvider;
        }
    }
}
