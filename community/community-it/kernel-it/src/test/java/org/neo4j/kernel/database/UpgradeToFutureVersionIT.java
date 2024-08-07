/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.database;

import static java.lang.Integer.max;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;

import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.locking.forseti.ForsetiClient;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.event.InternalTransactionEventListener;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.UpgradeTestUtil;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.concurrent.BinaryLatch;

@TestDirectoryExtension
class UpgradeToFutureVersionIT {
    @Inject
    private TestDirectory testDirectory;

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private DatabaseManagementService dbms;
    private GraphDatabaseAPI db;
    private GraphDatabaseAPI systemDb;

    @BeforeEach
    void setUp() {
        startDbms();
    }

    @AfterEach
    void tearDown() {
        shutdownDbms();
    }

    private TestDatabaseManagementServiceBuilder configureGloriousFutureAsLatest(
            TestDatabaseManagementServiceBuilder builder) {
        return builder.setConfig(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion())
                .setConfig(
                        GraphDatabaseInternalSettings.latest_kernel_version, KernelVersion.GLORIOUS_FUTURE.version());
    }

    @Test
    void shouldUpgradeDatabaseAutomaticallyToLatestIfSet() throws Exception {
        long startTransaction = lastCommittedTransactionId();
        createWriteTransaction(); // Just to have at least one tx from our measurement point in the old version
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);

        // Switch latest to glorious future which should upgrade the dbmsRuntimeRepository
        shutdownDbms();
        startDbms(this::configureGloriousFutureAsLatest, true);

        assertThat(dbmsRuntimeVersion()).isEqualTo(DbmsRuntimeVersion.GLORIOUS_FUTURE);

        createReadTransaction();
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);

        createWriteTransaction();
        assertThat(kernelVersion()).isEqualTo(KernelVersion.GLORIOUS_FUTURE);
        assertUpgradeTransactionInOrder(startTransaction);
    }

    @Test
    void shouldUpgradeDatabaseToLatestVersionOnFirstWriteTransaction() throws Exception {
        // Switch latest to glorious future to be able to try upgrade
        shutdownDbms();
        startDbms(this::configureGloriousFutureAsLatest, false);

        long startTransaction = lastCommittedTransactionId();
        createWriteTransaction(); // Just to have at least one tx from our measurement point in the old version
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);

        systemDb.executeTransactionally("CALL dbms.upgrade()");
        assertThat(dbmsRuntimeVersion()).isEqualTo(DbmsRuntimeVersion.GLORIOUS_FUTURE);

        createReadTransaction();
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);

        createWriteTransaction();
        assertThat(kernelVersion()).isEqualTo(KernelVersion.GLORIOUS_FUTURE);
        assertUpgradeTransactionInOrder(startTransaction);
    }

    @Test
    void shouldNotBeAbleToReadFutureVersions() {
        // Just a sanity check that we have blocks in place for reading GLORIOUS_FUTURE when we shouldn't be able to

        shutdownDbms();
        startDbms(this::configureGloriousFutureAsLatest, true);
        // Write a tx in the new version
        createWriteTransaction();
        assertThat(kernelVersion()).isEqualTo(KernelVersion.GLORIOUS_FUTURE);
        shutdownDbms();
        // Start up on old version again - system db will try to start because it doesn't have a tx on the new version,
        // but will fail because the component is newer than expected
        assertThatThrownBy(this::startDbms)
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Unsupported component state for 'dbms-runtime': The sub-graph is unsupported "
                        + "because it is a newer version, this component cannot function");

        // Start up on old version again but allow system db to start
        // neo4j won't start because it can't read the latest checkpoint
        startDbms(
                (builder) -> builder.setConfig(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion()),
                false);

        DatabaseShutdownException e = assertThrows(DatabaseShutdownException.class, db::beginTx);
        assertThat(e)
                .cause()
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Checkpoint log file with version 0 has some data available after last readable log entry");
    }

    @Test
    void shouldUpgradeDatabaseToLatestVersionOnFirstWriteTransactionStressTest() throws Throwable {
        // Switch latest to glorious future to be able to try upgrade
        shutdownDbms();
        startDbms(this::configureGloriousFutureAsLatest, false);

        long startTransaction = lastCommittedTransactionId();
        createWriteTransaction(); // Just to have at least one tx from our measurement point in the old version
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);
        assertThat(dbmsRuntimeVersion()).isEqualTo(LatestVersions.LATEST_RUNTIME_VERSION);

        // When
        Race race = new Race()
                .withRandomStartDelays()
                .withEndCondition(() -> KernelVersion.GLORIOUS_FUTURE.equals(kernelVersion()));
        race.addContestant(() -> systemDb.executeTransactionally("CALL dbms.upgrade()"), 1);
        race.addContestants(max(Runtime.getRuntime().availableProcessors() - 1, 2), Race.throwing(() -> {
            try {
                createWriteTransaction();
            } catch (DeadlockDetectedException e) {
                // Sometimes the upgrade transaction throws (probably false-positive) deadlock
            }
            Thread.sleep(ThreadLocalRandom.current().nextInt(0, 2));
        }));
        race.go(1, TimeUnit.MINUTES);

        // Then
        assertThat(kernelVersion()).isEqualTo(KernelVersion.GLORIOUS_FUTURE);
        assertThat(dbmsRuntimeVersion()).isEqualTo(DbmsRuntimeVersion.GLORIOUS_FUTURE);
        assertUpgradeTransactionInOrder(startTransaction);
    }

    @Test
    void shouldNotUpgradePastDbmsRuntime() {
        shutdownDbms();
        startDbms(this::configureGloriousFutureAsLatest, false);

        createWriteTransaction();
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);
    }

    @Test
    void shouldHandleDeadlocksOnUpgradeTransaction() throws Exception {
        shutdownDbms();
        startDbms(this::configureGloriousFutureAsLatest, false);

        // This test tries to simulate a rare but possible deadlock scenario where one ongoing transaction (in commit
        // phase) is waiting for a lock held by the
        // transaction doing the upgrade. Since the first transaction has a shared upgrade lock, and the upgrade
        // transaction wants the write lock,
        // this will deadlock. Depending on which "side" the deadlock happens on, one of two things can happen
        // 1. the conflicting transaction will fail with DeadlockDetectedException and the upgrade will complete
        // successfully or
        // 2. the upgrade fails, were we let both conflicting and trigger transaction complete normally, log the failure
        // and try upgrade later.
        // This tests the latter, as the first is not interesting from an upgrade perspective

        // Given
        String lockNode1 = createWriteTransaction();
        String lockNode2 = createWriteTransaction();
        BinaryLatch l1 = new BinaryLatch();
        BinaryLatch l2 = new BinaryLatch();
        long numNodesBefore = getNodeCount();

        // Since the upgrade handler is already installed, we know this will be invoked after that, having the shared
        // upgrade lock
        dbms.registerTransactionEventListener(db.databaseName(), new InternalTransactionEventListener.Adapter<>() {
            @Override
            public Object beforeCommit(
                    TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
                // Unregister so only the first transaction gets this
                dbms.unregisterTransactionEventListener(db.databaseName(), this);
                l2.release();
                l1.await(); // Hold here until the upgrade transaction is ongoing
                // we need to lock several entities here since deadlock termination is based on number of locks client
                // holds, and we want other transaction to be canceled
                transaction.acquireWriteLock(transaction.getNodeByElementId(lockNode2));
                // Then wait for the lock held by that "triggering" tx
                transaction.acquireWriteLock(transaction.getNodeByElementId(lockNode1));
                return null;
            }
        });

        // When
        try (OtherThreadExecutor executor = new OtherThreadExecutor("Executor")) {
            // This will trigger the "locking" listener but not the upgrade
            Future<String> f1 = executor.executeDontWait(this::createWriteTransaction);
            l2.await(); // wait for it to be committing
            // then upgrade dbms runtime to trigger db upgrade on next write
            systemDb.executeTransactionally("CALL dbms.upgrade()");

            try (Transaction tx = db.beginTx()) {
                tx.acquireWriteLock(tx.getNodeByElementId(lockNode1)); // take the lock
                tx.createNode(); // and make sure it is a write to trigger upgrade
                l1.release();
                executor.waitUntilWaiting(details -> details.isAt(ForsetiClient.class, "acquireExclusive"));
                tx.commit();
            }
            executor.awaitFuture(f1);
        }

        // Then
        LogAssertions.assertThat(logProvider)
                .containsMessageWithArguments(
                        "Upgrade transaction from %s to %s not possible right now due to conflicting transaction, will retry on next write",
                        LatestVersions.LATEST_KERNEL_VERSION, KernelVersion.GLORIOUS_FUTURE)
                .doesNotContainMessageWithArguments(
                        "Upgrade transaction from %s to %s started",
                        LatestVersions.LATEST_KERNEL_VERSION, KernelVersion.GLORIOUS_FUTURE);

        assertThat(getNodeCount()).as("Both transactions succeeded").isEqualTo(numNodesBefore + 2);
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);

        // When
        createWriteTransaction();

        // Then
        assertThat(kernelVersion()).isEqualTo(KernelVersion.GLORIOUS_FUTURE);
        LogAssertions.assertThat(logProvider)
                .containsMessageWithArguments(
                        "Upgrade transaction from %s to %s started",
                        LatestVersions.LATEST_KERNEL_VERSION, KernelVersion.GLORIOUS_FUTURE)
                .containsMessageWithArguments(
                        "Upgrade transaction from %s to %s completed",
                        LatestVersions.LATEST_KERNEL_VERSION, KernelVersion.GLORIOUS_FUTURE);
    }

    private long getNodeCount() {
        try (Transaction tx = db.beginTx()) {
            return Iterables.count(tx.getAllNodes());
        }
    }

    private void createReadTransaction() {
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            allNodes.forEach(Entity::getAllProperties);
            tx.commit();
        }
    }

    private String createWriteTransaction() {
        try (Transaction tx = db.beginTx()) {
            String nodeId = tx.createNode().getElementId();
            tx.commit();
            return nodeId;
        }
    }

    private void startDbms() {
        startDbms(builder -> builder, false);
    }

    private void startDbms(Configuration configuration, boolean allowAutomaticUpgrade) {
        dbms = configuration
                .configure(new TestDatabaseManagementServiceBuilder()
                        .setDatabaseRootDirectory(testDirectory.homePath())
                        .setInternalLogProvider(logProvider)
                        .setConfig(automatic_upgrade_enabled, allowAutomaticUpgrade))
                .build();
        db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        systemDb = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
    }

    private void shutdownDbms() {
        if (dbms != null) {
            dbms.shutdown();
            dbms = null;
        }
    }

    private long lastCommittedTransactionId() {
        return get(db, TransactionIdStore.class).getLastCommittedTransactionId();
    }

    private KernelVersion kernelVersion() {
        return get(db, KernelVersionProvider.class).kernelVersion();
    }

    private DbmsRuntimeVersion dbmsRuntimeVersion() {
        return get(systemDb, DbmsRuntimeVersionProvider.class).getVersion();
    }

    private <T> T get(GraphDatabaseAPI db, Class<T> cls) {
        return db.getDependencyResolver().resolveDependency(cls);
    }

    private void assertUpgradeTransactionInOrder(long fromTxId) throws Exception {
        UpgradeTestUtil.assertUpgradeTransactionInOrder(
                LatestVersions.LATEST_KERNEL_VERSION, KernelVersion.GLORIOUS_FUTURE, fromTxId, db);
    }

    @FunctionalInterface
    interface Configuration {
        TestDatabaseManagementServiceBuilder configure(TestDatabaseManagementServiceBuilder builder);
    }
}
