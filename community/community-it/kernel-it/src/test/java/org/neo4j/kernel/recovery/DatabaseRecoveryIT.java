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
package org.neo4j.kernel.recovery;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

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

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.updater.DelegatingIndexUpdater;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.AdversarialPageCacheGraphDatabaseFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static java.lang.Long.max;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterables.count;

@Neo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class DatabaseRecoveryIT
{
    private static final String[] TOKENS = new String[] {"Token1", "Token2", "Token3", "Token4", "Token5"};

    @Inject
    private TestDirectory directory;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private Neo4jLayout neo4jLayout;
    @Inject
    private RandomRule random;
    @RegisterExtension
    static final PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();

    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private DatabaseManagementService managementService;

    @AfterEach
    void cleanUp()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
            managementService = null;
        }
    }

    @Test
    void idGeneratorsRebuildAfterRecovery() throws IOException
    {
        GraphDatabaseService database = startDatabase( directory.homeDir() );
        int numberOfNodes = 10;
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int nodeIndex = 0; nodeIndex < numberOfNodes; nodeIndex++ )
            {
                transaction.createNode();
            }
            transaction.commit();
        }

        var restoreDbLayout = copyStore();

        GraphDatabaseService recoveredDatabase = startDatabase( restoreDbLayout.getNeo4jLayout().homeDirectory() );
        try ( Transaction tx = recoveredDatabase.beginTx() )
        {
            assertEquals( numberOfNodes, count( tx.getAllNodes() ) );

            // Make sure id generator has been rebuilt so this doesn't throw null pointer exception
            tx.createNode();
        }
    }

    @Test
    void reportProgressOnRecovery() throws IOException
    {
        GraphDatabaseService database = startDatabase( directory.homeDir() );
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.createNode();
                transaction.commit();
            }
        }

        var restoreDbLayout = copyStore();
        DatabaseManagementService recoveredService = getManagementService( restoreDbLayout.getNeo4jLayout().homeDirectory() );
        GraphDatabaseService recoveredDatabase = recoveredService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = recoveredDatabase.beginTx() )
        {
            assertEquals( 10, count( tx.getAllNodes() ) );
        }
        logProvider.rawMessageMatcher().assertContains( "10% completed" );
        logProvider.rawMessageMatcher().assertContains( "100% completed" );

        recoveredService.shutdown();
    }

    @Test
    void shouldRecoverIdsCorrectlyWhenWeCreateAndDeleteANodeInTheSameRecoveryRun() throws IOException
    {
        GraphDatabaseService database = startDatabase( directory.homeDir() );
        Label testLabel = Label.label( "testLabel" );
        final String propertyToDelete = "propertyToDelete";
        final String validPropertyName = "validProperty";

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode();
            node.addLabel( testLabel );
            transaction.commit();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = findNodeByLabel( transaction, testLabel );
            node.setProperty( propertyToDelete, createLongString() );
            node.setProperty( validPropertyName, createLongString() );
            transaction.commit();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = findNodeByLabel( transaction, testLabel );
            node.removeProperty( propertyToDelete );
            transaction.commit();
        }

        // copying only transaction log simulate non clean shutdown db that should be able to recover just from logs
        var restoreDbLayout = copyStore();

        // database should be restored and node should have expected properties
        DatabaseManagementService recoveredService = getManagementService( restoreDbLayout.getNeo4jLayout().homeDirectory() );
        GraphDatabaseService recoveredDatabase = recoveredService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction transaction = recoveredDatabase.beginTx() )
        {
            Node node = findNodeByLabel( transaction, testLabel );
            assertFalse( node.hasProperty( propertyToDelete ) );
            assertTrue( node.hasProperty( validPropertyName ) );
        }

        recoveredService.shutdown();
    }

    @Test
    void recoveryShouldFixPartiallyAppliedSchemaIndexUpdates()
    {
        Label label = Label.label( "Foo" );
        String property = "Bar";

        // cause failure during 'relationship.delete()' command application
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, true ), Command.RelationshipCommand.class );
        adversary.disable();

        File storeDir = directory.homeDir();
        DatabaseManagementService managementService =
                AdversarialPageCacheGraphDatabaseFactory.create( storeDir, fileSystem, adversary ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
                tx.commit();
            }

            long relationshipId = createRelationship( db );

            TransactionFailureException txFailure = null;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode( label );
                node.setProperty( property, "B" );
                tx.getRelationshipById( relationshipId ).delete(); // this should fail because of the adversary
                adversary.enable();
                tx.commit();
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
                assertNotNull( findNode( label, property, "B", tx ) );
                assertNotNull( tx.getRelationshipById( relationshipId ) );
                tx.commit();
            }

            healthOf( db ).panic( txFailure.getCause() ); // panic the db again to force recovery on the next startup

            // restart the database, now with regular page cache
            managementService.shutdown();
            db = startDatabase( storeDir );

            // now we observe correct state: node is in the index and relationship is removed
            try ( Transaction tx = db.beginTx() )
            {
                assertNotNull( findNode( label, property, "B", tx ) );
                assertRelationshipNotExist( tx, relationshipId );
                tx.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldSeeSameIndexUpdatesDuringRecoveryAsFromNormalIndexApplication() throws Exception
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
            tx.schema().indexFor( label ).on( key1 ).create();
            tx.schema().indexFor( label ).on( key1 ).on( key2 ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, SECONDS );
            tx.commit();
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
        managementService.shutdown();
        fs.close();

        db = startDatabase( storeDir, crashedFs, recoveredUpdateCapturingIndexProvider );
        long lastCommittedTxIdAfterRecovered = lastCommittedTxId( db );
        Map<Long,Collection<IndexEntryUpdate<?>>> updatesAfterRecovery = recoveredUpdateCapturingIndexProvider.snapshot();

        // then
        assertEquals( lastCommittedTxIdBeforeRecovered, lastCommittedTxIdAfterRecovered );
        assertSameUpdates( updatesAtCrash, updatesAfterRecovery );
        managementService.shutdown();
        crashedFs.close();
    }

    @Test
    void shouldSeeTheSameRecordsAtCheckpointAsAfterReverseRecovery() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        managementService = new TestDatabaseManagementServiceBuilder( directory.homeDir() )
                .setFileSystem( fs )
                .impermanent()
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        produceRandomGraphUpdates( db, 100 );
        checkPoint( db );
        EphemeralFileSystemAbstraction checkPointFs = fs.snapshot();

        // when
        produceRandomGraphUpdates( db, 100 );
        flush( db );
        EphemeralFileSystemAbstraction crashedFs = fs.snapshot();

        managementService.shutdown();
        fs.close();
        Dependencies dependencies = new Dependencies();
        PageCache pageCache = pageCacheExtension.getPageCache( crashedFs );
        dependencies.satisfyDependencies( pageCache );

        Monitors monitors = new Monitors();
        AtomicReference<EphemeralFileSystemAbstraction> reversedFs = new AtomicReference<>();
        monitors.addMonitorListener( new RecoveryMonitor()
        {
            @Override
            public void reverseStoreRecoveryCompleted( long checkpointTxId )
            {
                try
                {
                    // Flush the page cache which will fished out of the GlobalModule at the point of constructing the database
                    pageCache.flushAndForce();
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
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( directory.homeDir() ).setFileSystem( crashedFs ).setExternalDependencies(
                        dependencies ).setMonitors( monitors ).impermanent().build();

        managementService.shutdown();

        // then
        fs.close();

        try
        {
            // Here we verify that the neostore contents, record by record are exactly the same when comparing
            // the store as it was right after the checkpoint with the store as it was right after reverse recovery completed.
            assertSameStoreContents( checkPointFs, reversedFs.get(), databaseLayout );
        }
        finally
        {
            IOUtils.closeAll( checkPointFs, reversedFs.get() );
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
                PageCache pageCache1 = new ConfiguringPageCacheFactory( fs1, defaults(), PageCacheTracer.NULL, NullLog.getInstance(), contextSupplier,
                        jobScheduler ).getOrCreatePageCache();
                PageCache pageCache2 = new ConfiguringPageCacheFactory( fs2, defaults(), PageCacheTracer.NULL, NullLog.getInstance(), contextSupplier,
                        jobScheduler ).getOrCreatePageCache();
                NeoStores store1 = new StoreFactory( databaseLayout, defaults(), new DefaultIdGeneratorFactory( fs1, immediate() ),
                        pageCache1, fs1, logProvider ).openAllNeoStores();
                NeoStores store2 = new StoreFactory( databaseLayout, defaults(), new DefaultIdGeneratorFactory( fs2, immediate() ),
                        pageCache2, fs2, logProvider ).openAllNeoStores()
                )
        {
            for ( StoreType storeType : StoreType.values() )
            {
                // Don't compare meta data records because they are updated somewhat outside tx application
                if ( storeType != StoreType.META_DATA )
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
            boolean deletedAndDynamicPropertyRecord = !record1.inUse() && store1 instanceof AbstractDynamicStore;
            if ( !deletedAndDynamicPropertyRecord )
            {
                assertEquals( record1, record2 );
            }
            // else this record is a dynamic record which came from a property record update, a dynamic record which will not be set back
            // to unused during reverse recovery and therefore cannot be checked with equality between the two versions of that record.
        }
    }

    private static void flush( GraphDatabaseService db ) throws IOException
    {
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( CheckPointerImpl.ForceOperation.class ).flushAndForce( IOLimiter.UNLIMITED );
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
            try ( ResourceIterator<Node> allNodes = tx.getAllNodes().iterator() )
            {
                while ( allNodes.hasNext() )
                {
                    nodes.add( allNodes.next() );
                }
            }
            tx.commit();
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
                            Node node = tx.createNode( random.nextBoolean() ? new Label[] { randomLabel() } : new Label[0] );
                            if ( random.nextBoolean() )
                            {
                                node.setProperty( randomKey(), random.nextValueAsObject() );
                            }
                        }
                        else
                        {   // create relationship (w/ random prop)
                            if ( !nodes.isEmpty() )
                            {
                                Relationship relationship = tx.getNodeById( random.among( nodes ).getId() )
                                        .createRelationshipTo( tx.getNodeById( random.among( nodes ).getId() ), randomRelationshipType() );
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
                            random.among( nodes, node -> tx.getNodeById( node.getId() ).addLabel( randomLabel() ) );
                        }
                        else if ( operation < 0.5 )
                        {   // remove label
                            random.among( nodes, node -> tx.getNodeById( node.getId() ).removeLabel( randomLabel() ) );
                        }
                        else if ( operation < 0.75 )
                        {   // set node property
                            random.among( nodes, node -> tx.getNodeById( node.getId() ).setProperty( randomKey(), random.nextValueAsObject() ) );
                        }
                        else
                        {   // set relationship property
                            onRandomRelationship( nodes, relationship ->
                                    tx.getRelationshipById( relationship.getId() ).setProperty( randomKey(),
                                            random.nextValueAsObject() ), tx );
                        }
                    }
                    else
                    {   // delete

                        if ( operation < 0.25 )
                        {   // remove node property
                            random.among( nodes, node -> tx.getNodeById( node.getId() ).removeProperty( randomKey() ) );
                        }
                        else if ( operation < 0.5 )
                        {   // remove relationship property
                            onRandomRelationship( nodes, relationship ->
                                    relationship.removeProperty( randomKey() ), tx );
                        }
                        else if ( operation < 0.9 )
                        {   // delete relationship
                            onRandomRelationship( nodes, Relationship::delete, tx );
                        }
                        else
                        {   // delete node
                            random.among( nodes, node ->
                            {
                                node = tx.getNodeById( node.getId() );
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
                tx.commit();
            }
        }
    }

    private void onRandomRelationship( List<Node> nodes, Consumer<Relationship> action, Transaction transaction )
    {
        random.among( nodes, node -> random.among( asList( transaction.getNodeById( node.getId() ).getRelationships() ), action ) );
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

    private static void assertSameUpdates( Map<Long,Collection<IndexEntryUpdate<?>>> updatesAtCrash,
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
            try ( ResourceIterator<Node> allNodes = tx.getAllNodes().iterator() )
            {
                while ( allNodes.hasNext() )
                {
                    nodes.add( allNodes.next() );
                }
            }
            tx.commit();
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
                            tx.getNodeById( nodes.remove( random.nextInt( nodes.size() ) ).getId() ).delete();
                        }
                    }
                    else if ( operation < 0.3 )
                    {   // Create node
                        Node node = tx.createNode( random.nextBoolean() ? new Label[] { label } : new Label[0] );
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
                        random.among( nodes, node -> tx.getNodeById( node.getId() ).removeLabel( label ) );
                    }
                    else if ( operation < 0.6 )
                    {   // Add label
                        random.among( nodes, node -> tx.getNodeById( node.getId() ).addLabel( label ) );
                    }
                    else if ( operation < 0.85 )
                    {   // Set property
                        random.among( nodes, node -> tx.getNodeById( node.getId() ).setProperty( random.among( keys ), random.nextValueAsObject() ) );
                    }
                    else
                    {   // Remove property
                        random.among( nodes, node -> tx.getNodeById( node.getId() ).removeProperty( random.among( keys ) ) );
                    }
                }
                tx.commit();
            }
        }
    }

    private static Node findNodeByLabel( Transaction transaction, Label testLabel )
    {
        try ( ResourceIterator<Node> nodes = transaction.findNodes( testLabel ) )
        {
            return nodes.next();
        }
    }

    private static Node findNode( Label label, String property, String value, Transaction transaction )
    {
        try ( ResourceIterator<Node> nodes = transaction.findNodes( label, property, value ) )
        {
            return Iterators.single( nodes );
        }
    }

    private static long createRelationship( GraphDatabaseService db )
    {
        long relationshipId;
        try ( Transaction tx = db.beginTx() )
        {
            Node start = tx.createNode( Label.label( System.currentTimeMillis() + "" ) );
            Node end = tx.createNode( Label.label( System.currentTimeMillis() + "" ) );
            relationshipId = start.createRelationshipTo( end, withName( "KNOWS" ) ).getId();
            tx.commit();
        }
        return relationshipId;
    }

    private static void assertRelationshipNotExist( Transaction tx, long id )
    {
        assertThrows( NotFoundException.class, () -> tx.getRelationshipById( id ) );
    }

    private static Health healthOf( GraphDatabaseService db )
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

    private DatabaseLayout copyStore() throws IOException
    {
        DatabaseLayout restoreDbLayout = Neo4jLayout.of( directory.homeDir( "restore-db" ) ).databaseLayout( DEFAULT_DATABASE_NAME );
        fileSystem.mkdirs( restoreDbLayout.databaseDirectory() );
        fileSystem.mkdirs( restoreDbLayout.getTransactionLogsDirectory() );
        copy( fileSystem, databaseLayout.getTransactionLogsDirectory(), restoreDbLayout.getTransactionLogsDirectory() );
        copy( fileSystem, databaseLayout.databaseDirectory(), restoreDbLayout.databaseDirectory() );
        return restoreDbLayout;
    }

    private static void copy( FileSystemAbstraction fs, File fromDirectory, File toDirectory ) throws IOException
    {
        assertTrue( fs.isDirectory( fromDirectory ) );
        assertTrue( fs.isDirectory( toDirectory ) );
        fs.copyRecursively( fromDirectory, toDirectory );
    }

    private GraphDatabaseAPI startDatabase( File homeDir, EphemeralFileSystemAbstraction fs, UpdateCapturingIndexProvider indexProvider )
    {

        if ( managementService != null )
        {
            managementService.shutdown();
        }
        managementService = new TestDatabaseManagementServiceBuilder( homeDir )
                .setFileSystem( fs )
                .setExtensions( singletonList( new IndexExtensionFactory( indexProvider ) ) )
                .impermanent()
                .noOpSystemGraphInitializer()
                .setConfig( default_schema_provider, indexProvider.getProviderDescriptor().name() )
                .build();

        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private GraphDatabaseService startDatabase( File homeDir )
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
        managementService = getManagementService( homeDir );
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private DatabaseManagementService getManagementService( File homeDir )
    {
        return new TestDatabaseManagementServiceBuilder( homeDir ).setInternalLogProvider( logProvider ).build();
    }

    public class UpdateCapturingIndexProvider extends IndexProvider.Delegating
    {
        private final Map<Long,UpdateCapturingIndexAccessor> indexes = new ConcurrentHashMap<>();
        private final Map<Long,Collection<IndexEntryUpdate<?>>> initialUpdates;

        UpdateCapturingIndexProvider( IndexProvider actual, Map<Long,Collection<IndexEntryUpdate<?>>> initialUpdates )
        {
            super( actual );
            this.initialUpdates = initialUpdates;
        }

        @Override
        public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
                throws IOException
        {
            IndexAccessor actualAccessor = super.getOnlineAccessor( descriptor, samplingConfig );
            return indexes.computeIfAbsent( descriptor.getId(), id -> new UpdateCapturingIndexAccessor( actualAccessor, initialUpdates.get( id ) ) );
        }

        public Map<Long,Collection<IndexEntryUpdate<?>>> snapshot()
        {
            Map<Long,Collection<IndexEntryUpdate<?>>> result = new HashMap<>();
            indexes.forEach( ( indexId, index ) -> result.put( indexId, index.snapshot() ) );
            return result;
        }
    }

    public class UpdateCapturingIndexAccessor extends IndexAccessor.Delegating
    {
        private final Collection<IndexEntryUpdate<?>> updates = new ArrayList<>();

        UpdateCapturingIndexAccessor( IndexAccessor actual, Collection<IndexEntryUpdate<?>> initialUpdates )
        {
            super( actual );
            if ( initialUpdates != null )
            {
                this.updates.addAll( initialUpdates );
            }
        }

        @Override
        public IndexUpdater newUpdater( IndexUpdateMode mode )
        {
            return wrap( super.newUpdater( mode ) );
        }

        private IndexUpdater wrap( IndexUpdater actual )
        {
            return new UpdateCapturingIndexUpdater( actual, updates );
        }

        public Collection<IndexEntryUpdate<?>> snapshot()
        {
            return new ArrayList<>( updates );
        }
    }

    public static class UpdateCapturingIndexUpdater extends DelegatingIndexUpdater
    {
        private final Collection<IndexEntryUpdate<?>> updatesTarget;

        UpdateCapturingIndexUpdater( IndexUpdater actual, Collection<IndexEntryUpdate<?>> updatesTarget )
        {
            super( actual );
            this.updatesTarget = updatesTarget;
        }

        @Override
        public void process( IndexEntryUpdate<?> update ) throws IndexEntryConflictException
        {
            super.process( update );
            updatesTarget.add( update );
        }
    }

    @RecoveryExtension
    private static class IndexExtensionFactory extends ExtensionFactory<IndexExtensionFactory.Dependencies>
    {
        private final IndexProvider indexProvider;

        interface Dependencies
        {
        }

        IndexExtensionFactory( IndexProvider indexProvider )
        {
            super( ExtensionType.DATABASE, "customExtension" );
            this.indexProvider = indexProvider;
        }

        @Override
        public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
        {
            return indexProvider;
        }
    }
}
