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
import static java.lang.System.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.test.UpgradeTestUtil.assertUpgradeTransactionInOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.ZippedStore;
import org.neo4j.kernel.ZippedStoreCommunity;
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
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.concurrent.BinaryLatch;

@TestDirectoryExtension
public class DatabaseUpgradeTransactionIT {
    private static final ZippedStore ZIPPED_STORE = ZippedStoreCommunity.REC_AF11_V50_EMPTY;
    private static final KernelVersion OLD_KERNEL_VERSION =
            ZIPPED_STORE.statistics().kernelVersion();
    private static final DbmsRuntimeVersion OLD_DBMS_RUNTIME_VERSION = DbmsRuntimeVersion.VERSIONS.stream()
            .filter(dbmsRuntimeVersion -> dbmsRuntimeVersion.kernelVersion() == OLD_KERNEL_VERSION)
            .findFirst()
            .orElseThrow();

    @Inject
    protected TestDirectory testDirectory;

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    protected DatabaseManagementService dbms;
    protected GraphDatabaseAPI db;
    private GraphDatabaseAPI systemDb;
    protected KernelVersion oldKernelVersion;
    protected DbmsRuntimeVersion oldDbmsRuntimeVersion;

    @BeforeEach
    void setUp() throws IOException {
        createDbFiles();
        startDbms();
    }

    @AfterEach
    void tearDown() {
        if (dbms != null) {
            dbms.shutdown();
        }
    }

    protected void createDbFiles() throws IOException {
        oldKernelVersion = OLD_KERNEL_VERSION;
        oldDbmsRuntimeVersion = OLD_DBMS_RUNTIME_VERSION;
        ZIPPED_STORE.unzip(testDirectory.homePath());
    }

    protected TestDatabaseManagementServiceBuilder configure(TestDatabaseManagementServiceBuilder builder) {
        return builder.setDatabaseRootDirectory(testDirectory.homePath())
                .setInternalLogProvider(logProvider)
                .setConfig(automatic_upgrade_enabled, false);
    }

    @Test
    void shouldUpgradeDatabaseToLatestVersionOnFirstWriteTransaction() throws Exception {
        // Given
        long startTransaction = lastCommittedTransactionId();

        // Then
        assertThat(kernelVersion()).isEqualTo(oldKernelVersion);
        createWriteTransaction(); // Just to have at least one tx from our measurement point in the old version
        set(LatestVersions.LATEST_RUNTIME_VERSION);

        // When
        createReadTransaction();

        // Then
        assertThat(kernelVersion()).isEqualTo(oldKernelVersion);

        // When
        createWriteTransaction();

        // Then
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);
        assertUpgradeTransactionInOrder(oldKernelVersion, LatestVersions.LATEST_KERNEL_VERSION, startTransaction, db);
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.MATCH_ALL, names = "V5_[0-9]+")
    void shouldUpgradeDatabaseToMaxKernelVersionForDbmsRuntimeVersionOnFirstWriteTransaction(
            DbmsRuntimeVersion dbmsRuntimeVersion) throws Exception {
        // Given
        assumeThat(dbmsRuntimeVersion).as("needs to be newer to upgrade").isGreaterThan(oldDbmsRuntimeVersion);

        long startTransaction = lastCommittedTransactionId();

        // Then
        assertThat(kernelVersion()).isEqualTo(oldKernelVersion);
        assertThat(dbmsRuntimeVersion()).isEqualTo(oldDbmsRuntimeVersion);
        createWriteTransaction(); // Just to have at least one tx from our measurement point in the old version

        set(dbmsRuntimeVersion);

        // When
        createReadTransaction();

        // Then
        assertThat(kernelVersion()).isEqualTo(oldKernelVersion);

        // When
        createWriteTransaction();

        // Then
        assertThat(kernelVersion()).isEqualTo(dbmsRuntimeVersion.kernelVersion());
        assertUpgradeTransactionInOrder(oldKernelVersion, dbmsRuntimeVersion.kernelVersion(), startTransaction, db);
    }

    @Test
    void shouldUpgradeDatabaseToLatestVersionOnFirstWriteTransactionStressTest() throws Throwable {
        long startTransaction = lastCommittedTransactionId();

        // Then
        assertThat(kernelVersion()).isEqualTo(oldKernelVersion);
        assertThat(dbmsRuntimeVersion()).isEqualTo(oldDbmsRuntimeVersion);
        createWriteTransaction(); // Just to have at least one tx from our measurement point in the old version

        // When
        Race race = new Race()
                .withRandomStartDelays()
                .withEndCondition(() -> LatestVersions.LATEST_KERNEL_VERSION.equals(kernelVersion()));
        race.addContestant(() -> systemDb.executeTransactionally("CALL dbms.upgrade()"), 1);
        race.addContestants(max(Runtime.getRuntime().availableProcessors() - 1, 2), Race.throwing(() -> {
            createWriteTransaction();
            Thread.sleep(ThreadLocalRandom.current().nextInt(0, 2));
        }));
        race.go(1, TimeUnit.MINUTES);

        // Then
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);
        assertThat(dbmsRuntimeVersion()).isEqualTo(LatestVersions.LATEST_RUNTIME_VERSION);
        assertUpgradeTransactionInOrder(oldKernelVersion, LatestVersions.LATEST_KERNEL_VERSION, startTransaction, db);
    }

    @Test
    void shouldUpgradeDatabaseToLatestVersionOnDenseNodeTransactionStressTest() throws Throwable {
        long startTransaction = lastCommittedTransactionId();

        // Then
        assertThat(kernelVersion()).isEqualTo(oldKernelVersion);
        assertThat(dbmsRuntimeVersion()).isEqualTo(oldDbmsRuntimeVersion);
        String nodeId = createDenseNode();

        // When
        Race race = new Race().withRandomStartDelays().withEndCondition(new BooleanSupplier() {
            private final AtomicLong timeOfUpgrade = new AtomicLong();

            @Override
            public boolean getAsBoolean() {
                if (LatestVersions.LATEST_KERNEL_VERSION.equals(kernelVersion())) {
                    // Notice the time of upgrade...
                    timeOfUpgrade.compareAndSet(0, currentTimeMillis());
                }
                // ... and continue a little while after it happened so that we get transactions both before and after
                return timeOfUpgrade.get() != 0 && (currentTimeMillis() - timeOfUpgrade.get()) > 1_000;
            }
        });
        race.addContestant(
                throwing(() -> {
                    while (true) {
                        try {
                            Thread.sleep(ThreadLocalRandom.current().nextInt(0, 1_000));
                            systemDb.executeTransactionally("CALL dbms.upgrade()");
                            return;
                        } catch (DeadlockDetectedException de) {
                            // retry
                        }
                    }
                }),
                1);
        race.addContestants(max(Runtime.getRuntime().availableProcessors() - 1, 2), throwing(() -> {
            while (true) {
                try (Transaction tx = db.beginTx()) {
                    tx.getNodeByElementId(nodeId)
                            .createRelationshipTo(
                                    tx.createNode(),
                                    RelationshipType.withName("TYPE_"
                                            + ThreadLocalRandom.current().nextInt(3)));
                    tx.commit();
                    Thread.sleep(ThreadLocalRandom.current().nextInt(0, 2));
                    return;
                } catch (DeadlockDetectedException ignore) {
                    // ignore deadlocks
                }
            }
        }));
        race.go(10, TimeUnit.MINUTES);

        // Then
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);
        assertThat(dbmsRuntimeVersion()).isEqualTo(LatestVersions.LATEST_RUNTIME_VERSION);
        assertUpgradeTransactionInOrder(oldKernelVersion, LatestVersions.LATEST_KERNEL_VERSION, startTransaction, db);
        assertDegrees(nodeId);
    }

    @Test
    void shouldNotUpgradePastDbmsRuntime() {
        // When
        createWriteTransaction();

        // Then
        assertThat(kernelVersion()).isEqualTo(oldKernelVersion);
    }

    @Test
    void shouldHandleDeadlocksOnUpgradeTransaction() throws Exception {
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
            set(LatestVersions.LATEST_RUNTIME_VERSION);

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
                        oldKernelVersion, LatestVersions.LATEST_KERNEL_VERSION)
                .doesNotContainMessageWithArguments(
                        "Upgrade transaction from %s to %s started",
                        oldKernelVersion, LatestVersions.LATEST_KERNEL_VERSION);

        assertThat(getNodeCount()).as("Both transactions succeeded").isEqualTo(numNodesBefore + 2);
        assertThat(kernelVersion()).isEqualTo(oldKernelVersion);

        // When
        createWriteTransaction();

        // Then
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);
        LogAssertions.assertThat(logProvider)
                .containsMessageWithArguments(
                        "Upgrade transaction from %s to %s started",
                        oldKernelVersion, LatestVersions.LATEST_KERNEL_VERSION)
                .containsMessageWithArguments(
                        "Upgrade transaction from %s to %s completed",
                        oldKernelVersion, LatestVersions.LATEST_KERNEL_VERSION);
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

    protected String createWriteTransaction() {
        DeadlockDetectedException deadlockDetectedException = null;
        for (int i = 0; i < 10; i++) {
            try (Transaction tx = db.beginTx()) {
                String nodeId = tx.createNode().getElementId();
                tx.commit();
                return nodeId;
            } catch (DeadlockDetectedException e) {
                deadlockDetectedException = e;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ex);
                }
            }
        }
        throw deadlockDetectedException;
    }

    protected void startDbms() {
        dbms = configure(getBuilder()).build();
        db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        systemDb = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
    }

    protected TestDatabaseManagementServiceBuilder getBuilder() {
        return new TestDatabaseManagementServiceBuilder();
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

    protected <T> T get(GraphDatabaseAPI db, Class<T> cls) {
        return db.getDependencyResolver().resolveDependency(cls);
    }

    protected void set(DbmsRuntimeVersion runtimeVersion) {
        try (var tx = systemDb.beginTx();
                var nodes = tx.findNodes(VERSION_LABEL).stream()) {
            nodes.forEach(dbmsRuntimeNode ->
                    dbmsRuntimeNode.setProperty(DBMS_RUNTIME_COMPONENT.name(), runtimeVersion.getVersion()));
            tx.commit();
        }
    }

    private String createDenseNode() {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            String nodeId = node.getElementId();
            for (int i = 0; i < 100; i++) {
                node.createRelationshipTo(tx.createNode(), RelationshipType.withName("TYPE_" + (i % 3)));
            }
            tx.commit();
            return nodeId;
        }
    }

    private void assertDegrees(String nodeId) {
        // Why assert degrees specifically? This particular upgrade: V4_2 -> V4_3_D3 changes how dense node degrees are
        // stored so it's a really good indicator that everything there works
        try (Transaction tx = db.beginTx()) {
            Node node = tx.getNodeByElementId(nodeId);
            Map<RelationshipType, Map<Direction, MutableLong>> actualDegrees = new HashMap<>();
            Iterables.forEach(node.getRelationships(), r -> actualDegrees
                    .computeIfAbsent(r.getType(), t -> new HashMap<>())
                    .computeIfAbsent(directionOf(node, r), d -> new MutableLong())
                    .increment());
            MutableLong actualTotalDegree = new MutableLong();
            actualDegrees.forEach((type, directions) -> {
                long actualTotalDirectionDegree = 0;
                for (Map.Entry<Direction, MutableLong> actualDirectionDegree : directions.entrySet()) {
                    assertThat(node.getDegree(type, actualDirectionDegree.getKey()))
                            .isEqualTo(actualDirectionDegree.getValue().longValue());
                    actualTotalDirectionDegree +=
                            actualDirectionDegree.getValue().longValue();
                }
                assertThat(node.getDegree(type)).isEqualTo(actualTotalDirectionDegree);
                actualTotalDegree.add(actualTotalDirectionDegree);
            });
            assertThat(node.getDegree()).isEqualTo(actualTotalDegree.longValue());
        }
    }

    private static Direction directionOf(Node node, Relationship relationship) {
        return relationship.getStartNode().equals(node)
                ? relationship.getEndNode().equals(node) ? Direction.BOTH : Direction.OUTGOING
                : Direction.INCOMING;
    }
}
