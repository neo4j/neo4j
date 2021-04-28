/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.database;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.io.pagecache.tracing.cursor.CursorContext;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.locking.forseti.ForsetiClient;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.event.InternalTransactionEventListener;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.lang.Integer.max;
import static java.lang.System.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_single_automatic_upgrade;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;
import static org.neo4j.kernel.KernelVersion.LATEST;
import static org.neo4j.kernel.KernelVersion.V4_2;
import static org.neo4j.test.Race.throwing;

@EphemeralTestDirectoryExtension
class DatabaseUpgradeTransactionIT
{
    @Inject
    private TestDirectory testDirectory;

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private DatabaseManagementService dbms;
    private GraphDatabaseAPI db;

    @BeforeEach
    void setUp()
    {
        restartDbms();
    }

    @AfterEach
    void tearDown()
    {
        dbms.shutdown();
    }

    @Test
    void shouldUpgradeDatabaseToLatestVersionOnFirstWriteTransaction() throws Exception
    {
        assertThat( V4_2 ).isLessThan( LATEST );

        //Given
        setKernelVersion( V4_2 );
        setDbmsRuntime( DbmsRuntimeVersion.V4_3 );
        restartDbms();
        long startTransaction = db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( V4_2 );
        createWriteTransaction(); // Just to have at least one tx from our measurement point in the old version
        setDbmsRuntime( DbmsRuntimeVersion.V4_3_D4 );

        //When
        createReadTransaction();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( V4_2 );

        //When
        createWriteTransaction();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( LATEST );
        assertUpgradeTransactionInOrder( V4_2, LATEST, startTransaction );
    }

    @Test
    void shouldUpgradeDatabaseToLatestVersionOnFirstWriteTransactionStressTest() throws Throwable
    {
        //Given
        setKernelVersion( V4_2 );
        setDbmsRuntime( DbmsRuntimeVersion.V4_2 );
        restartDbms();
        long startTransaction = db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( V4_2 );
        assertThat( getDbmsRuntime() ).isEqualTo( DbmsRuntimeVersion.V4_2 );
        createWriteTransaction(); // Just to have at least one tx from our measurement point in the old version

        //When
        Race race = new Race().withRandomStartDelays().withEndCondition( () -> LATEST.equals( getKernelVersion() ) );
        race.addContestant( () ->
        {
            dbms.database( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ).executeTransactionally( "CALL dbms.upgrade()" );
        }, 1 );
        race.addContestants( max( Runtime.getRuntime().availableProcessors() - 1, 2 ), Race.throwing( () -> {
            createWriteTransaction();
            Thread.sleep( ThreadLocalRandom.current().nextInt( 0, 2 ) );
        } ) );
        race.go( 1, TimeUnit.MINUTES );

        //Then
        assertThat( getKernelVersion() ).isEqualTo( LATEST );
        assertThat( getDbmsRuntime() ).isEqualTo( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );
        assertUpgradeTransactionInOrder( V4_2, LATEST, startTransaction );
    }

    @Test
    void shouldUpgradeDatabaseToLatestVersionOnDenseNodeTransactionStressTest() throws Throwable
    {
        //Given
        setKernelVersion( V4_2 );
        setDbmsRuntime( DbmsRuntimeVersion.V4_2 );
        restartDbms();
        long startTransaction = db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( V4_2 );
        assertThat( getDbmsRuntime() ).isEqualTo( DbmsRuntimeVersion.V4_2 );
        long nodeId = createDenseNode();

        //When
        Race race = new Race().withRandomStartDelays().withEndCondition( new BooleanSupplier()
        {
            private final AtomicLong timeOfUpgrade = new AtomicLong();

            @Override
            public boolean getAsBoolean()
            {
                if ( LATEST.equals( getKernelVersion() ) )
                {
                    // Notice the time of upgrade...
                    timeOfUpgrade.compareAndSet( 0, currentTimeMillis() );
                }
                // ... and continue a little while after it happened so that we get transactions both before and after
                return timeOfUpgrade.get() != 0 && (currentTimeMillis() - timeOfUpgrade.get()) > 1_000;
            }
        } );
        race.addContestant( throwing( () ->
        {
            while ( true )
            {
                try
                {
                    Thread.sleep( ThreadLocalRandom.current().nextInt( 0, 1_000 ) );
                    dbms.database( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ).executeTransactionally( "CALL dbms.upgrade()" );
                    return;
                }
                catch ( DeadlockDetectedException de )
                {
                    //retry
                }
            }
        } ), 1 );
        race.addContestants( max( Runtime.getRuntime().availableProcessors() - 1, 2 ), throwing( () -> {
            while ( true )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.getNodeById( nodeId ).createRelationshipTo( tx.createNode(),
                            RelationshipType.withName( "TYPE_" + ThreadLocalRandom.current().nextInt( 3 ) ) );
                    tx.commit();
                    Thread.sleep( ThreadLocalRandom.current().nextInt( 0, 2 ) );
                    return;
                }
                catch ( DeadlockDetectedException ignore )
                {
                    // ignore deadlocks
                }
            }
        } ) );
        race.go( 1, TimeUnit.MINUTES );

        //Then
        assertThat( getKernelVersion() ).isEqualTo( LATEST );
        assertThat( getDbmsRuntime() ).isEqualTo( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );
        assertUpgradeTransactionInOrder( KernelVersion.V4_2, KernelVersion.LATEST, startTransaction );
        assertDegrees( nodeId );
    }

    @Test
    void shouldNotUpgradePastDbmsRuntime()
    {
        //Given
        setKernelVersion( V4_2 );
        restartDbms();

        setDbmsRuntime( DbmsRuntimeVersion.V4_2 );

        //When
        createWriteTransaction();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( V4_2 );
    }

    @Test
    void shouldHandleDeadlocksOnUpgradeTransaction() throws Exception
    {
        // This test tries to simulate a rare but possible deadlock scenario where one ongoing transaction (in commit phase) is waiting for a lock held by the
        // transaction doing the upgrade. Since the first transaction has a shared upgrade lock, and the upgrade transaction wants the write lock,
        // this will deadlock. Depending on which "side" the deadlock happens on, one of two things can happen
        // 1. the conflicting transaction will fail with DeadlockDetectedException and the upgrade will complete successfully or
        // 2. the upgrade fails, were we let both conflicting and trigger transaction complete normally, log the failure and try upgrade later.
        // This tests the latter, as the first is not interesting from an upgrade perspective

        //Given
        setKernelVersion( V4_2 );
        setDbmsRuntime( DbmsRuntimeVersion.V4_2 );
        restartDbms();
        long lockNode1 = createWriteTransaction();
        long lockNode2 = createWriteTransaction();
        BinaryLatch l1 = new BinaryLatch();
        BinaryLatch l2 = new BinaryLatch();
        long numNodesBefore = getNodeCount();

        //Since the upgrade handler is already installed, we know this will be invoked after that, having the shared upgrade lock
        dbms.registerTransactionEventListener( db.databaseName(), new InternalTransactionEventListener.Adapter<>()
        {
            @Override
            public Object beforeCommit( TransactionData data, Transaction transaction, GraphDatabaseService databaseService )
            {
                dbms.unregisterTransactionEventListener( db.databaseName(), this ); //Unregister so only the first transaction gets this
                l2.release();
                l1.await(); //Hold here until the upgrade transaction is ongoing
                // we need to lock several entities here since deadlock termination is based on number of locks client holds.
                // and we want other transaction to be canceled
                transaction.acquireWriteLock( transaction.getNodeById( lockNode2 ) );
                transaction.acquireWriteLock( transaction.getNodeById( lockNode1 ) ); //Then wait for the lock held by that "triggering" tx
                return null;
            }
        } );

        //When
        try ( OtherThreadExecutor executor = new OtherThreadExecutor( "Executor" ) )
        {
            Future<Long> f1 = executor.executeDontWait( this::createWriteTransaction ); //This will trigger the "locking" listener but not the upgrade
            l2.await(); //wait for it to be committing
            setDbmsRuntime( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION ); //then upgrade dbms runtime to trigger db upgrade on next write

            try ( Transaction tx = db.beginTx() )
            {
                tx.acquireWriteLock( tx.getNodeById( lockNode1 ) ); //take the lock
                tx.createNode(); //and make sure it is a write to trigger upgrade
                l1.release();
                executor.waitUntilWaiting( details -> details.isAt( ForsetiClient.class, "acquireExclusive" ) );
                tx.commit();
            }
            executor.awaitFuture( f1 );
        }

        //Then
        LogAssertions.assertThat( logProvider ).containsMessageWithArguments(
                "Upgrade transaction from %s to %s not possible right now due to conflicting transaction, will retry on next write", V4_2, LATEST )
                .doesNotContainMessageWithArguments( "Upgrade transaction from %s to %s started", V4_2, LATEST );

        assertThat( getNodeCount() ).as( "Both transactions succeeded" ).isEqualTo( numNodesBefore + 2 );
        assertThat( getKernelVersion() ).isEqualTo( V4_2 );

        //When
        createWriteTransaction();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( LATEST );
        LogAssertions.assertThat( logProvider )
                .containsMessageWithArguments( "Upgrade transaction from %s to %s started", V4_2, LATEST )
                .containsMessageWithArguments( "Upgrade transaction from %s to %s completed", V4_2, LATEST );
    }

    private long getNodeCount()
    {
        try ( Transaction tx = db.beginTx() )
        {
            return Iterators.count( tx.getAllNodes().iterator() );
        }
    }

    private void createReadTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.getAllNodes().stream().forEach( Entity::getAllProperties );
            tx.commit();
        }
    }

    private long createWriteTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = tx.createNode().getId();
            tx.commit();
            return nodeId;
        }
    }

    private void restartDbms()
    {
        if ( dbms != null )
        {
            dbms.shutdown();
        }
        dbms = new TestDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( allow_single_automatic_upgrade, false )
                .setInternalLogProvider( logProvider )
                .build();
        db = (GraphDatabaseAPI) dbms.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    private void setKernelVersion( KernelVersion version )
    {
        MetaDataStore metaDataStore = db.getDependencyResolver().resolveDependency( MetaDataStore.class );
        metaDataStore.setKernelVersion( version, CursorContext.NULL );
    }

    private KernelVersion getKernelVersion()
    {
        return db.getDependencyResolver().resolveDependency( KernelVersionRepository.class ).kernelVersion();
    }

    private void setDbmsRuntime( DbmsRuntimeVersion runtimeVersion )
    {
        GraphDatabaseAPI system = (GraphDatabaseAPI) dbms.database( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        try ( var tx = system.beginTx() )
        {
            tx.findNodes( VERSION_LABEL ).stream()
                    .forEach( dbmsRuntimeNode -> dbmsRuntimeNode.setProperty( DBMS_RUNTIME_COMPONENT, runtimeVersion.getVersion() ) );
            tx.commit();
        }
    }

    private DbmsRuntimeVersion getDbmsRuntime()
    {
        GraphDatabaseAPI system = (GraphDatabaseAPI) dbms.database( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        return system.getDependencyResolver().resolveDependency( DbmsRuntimeRepository.class ).getVersion();
    }

    private void assertUpgradeTransactionInOrder( KernelVersion from, KernelVersion to, long fromTxId ) throws Exception
    {
        LogicalTransactionStore lts = db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
        ArrayList<KernelVersion> transactionVersions = new ArrayList<>();
        ArrayList<CommittedTransactionRepresentation> transactions = new ArrayList<>();
        try ( TransactionCursor transactionCursor = lts.getTransactions( fromTxId + 1 ) )
        {
            while ( transactionCursor.next() )
            {
                CommittedTransactionRepresentation representation = transactionCursor.get();
                transactions.add( representation );
                transactionVersions.add( representation.getStartEntry().getVersion() );
            }
        }
        assertThat( transactionVersions ).hasSizeGreaterThanOrEqualTo( 2 ); //at least upgrade transaction and the triggering transaction
        assertThat( transactionVersions ).isSortedAccordingTo( Comparator.comparingInt( KernelVersion::version ) ); //Sorted means everything is in order
        assertThat( transactionVersions.get( 0 ) ).isEqualTo( from ); //First should be "from" version
        assertThat( transactionVersions.get( transactionVersions.size() - 1 ) ).isEqualTo( to ); //And last the "to" version

        CommittedTransactionRepresentation upgradeTransaction = transactions.get( transactionVersions.indexOf( to ) );
        PhysicalTransactionRepresentation physicalRep = (PhysicalTransactionRepresentation) upgradeTransaction.getTransactionRepresentation();
        physicalRep.accept( element ->
        {
            assertThat( element ).isInstanceOf( Command.MetaDataCommand.class );
            return true;
        } );
    }

    private long createDenseNode()
    {
        MutableLong nodeId = new MutableLong();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            for ( int i = 0; i < 100; i++ )
            {
                node.createRelationshipTo( tx.createNode(), RelationshipType.withName( "TYPE_" + (i % 3) ) );
            }
            nodeId.setValue( node.getId() );
            tx.commit();
        }
        return nodeId.longValue();
    }

    private void assertDegrees( long nodeId )
    {
        // Why assert degrees specifically? This particular upgrade: V4_2 -> V4_3_D3 changes how dense node degrees are stored
        // so it's a really good indicator that everything there works
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            Map<String,Map<Direction,MutableLong>> actualDegrees = new HashMap<>();
            node.getRelationships().forEach(
                    r -> actualDegrees.computeIfAbsent( r.getType().name(), t -> new HashMap<>() ).computeIfAbsent( directionOf( node, r ),
                            d -> new MutableLong() ).increment() );
            MutableLong actualTotalDegree = new MutableLong();
            actualDegrees.forEach( ( typeName, directions ) ->
            {
                long actualTotalDirectionDegree = 0;
                for ( Map.Entry<Direction,MutableLong> actualDirectionDegree : directions.entrySet() )
                {
                    assertThat( node.getDegree( RelationshipType.withName( typeName ), actualDirectionDegree.getKey() ) ).isEqualTo(
                            actualDirectionDegree.getValue().longValue() );
                    actualTotalDirectionDegree += actualDirectionDegree.getValue().longValue();
                }
                assertThat( node.getDegree( RelationshipType.withName( typeName ) ) ).isEqualTo( actualTotalDirectionDegree );
                actualTotalDegree.add( actualTotalDirectionDegree );
            } );
            assertThat( node.getDegree() ).isEqualTo( actualTotalDegree.longValue() );
        }
    }

    private Direction directionOf( Node node, Relationship relationship )
    {
        return relationship.getStartNode().equals( node ) ? relationship.getEndNode().equals( node ) ? Direction.BOTH : Direction.OUTGOING : Direction.INCOMING;
    }
}
