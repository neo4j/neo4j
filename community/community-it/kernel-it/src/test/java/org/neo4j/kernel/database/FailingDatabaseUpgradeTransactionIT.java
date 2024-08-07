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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.ZippedStore;
import org.neo4j.kernel.ZippedStoreCommunity;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.Barrier;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class FailingDatabaseUpgradeTransactionIT {
    private static final ZippedStore ZIPPED_STORE = ZippedStoreCommunity.REC_AF11_V50_EMPTY;
    private static final KernelVersion OLD_KERNEL_VERSION =
            ZIPPED_STORE.statistics().kernelVersion();
    private static final DbmsRuntimeVersion OLD_DBMS_RUNTIME_VERSION = DbmsRuntimeVersion.VERSIONS.stream()
            .filter(dbmsRuntimeVersion -> dbmsRuntimeVersion.kernelVersion() == OLD_KERNEL_VERSION)
            .findFirst()
            .orElseThrow();
    private static final int MAX_TRANSACTIONS = 10;

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
    void shouldHandleMaxTransactionReachedInUpgrade() throws Exception {
        Barrier.Control barrier = new Barrier.Control();
        try (OtherThreadExecutor executor = new OtherThreadExecutor("Executor")) {
            Future<Object> readTx = null;
            try {
                readTx = executor.executeDontWait(() -> {
                    Collection<AutoCloseable> txs = new ArrayList<>();
                    try {
                        for (int i = 0; i < MAX_TRANSACTIONS - 1; i++) {
                            txs.add(db.beginTx());
                        }
                    } finally {
                        barrier.reached();
                        IOUtils.closeAll(txs);
                    }
                    return null;
                });

                set(LatestVersions.LATEST_RUNTIME_VERSION);

                barrier.await();
                try (Transaction tx = db.beginTx()) {
                    tx.createNode(); // Write tx to trigger upgrade
                    tx.commit();
                }
            } finally {
                barrier.release();
                assertThat(readTx).isNotNull();
                readTx.get();
            }
        }

        String dbNamePrefix = "["
                + db.getDependencyResolver()
                        .resolveDependency(NamedDatabaseId.class)
                        .logPrefix() + "]";
        LogAssertions.assertThat(logProvider)
                .containsMessageWithArguments(
                        dbNamePrefix
                                + " Upgrade transaction from %s to %s not possible right now because maximum concurrently "
                                + "executed transactions was reached, will retry on next write. If this persists "
                                + "see setting %s.",
                        oldKernelVersion,
                        LatestVersions.LATEST_KERNEL_VERSION,
                        GraphDatabaseSettings.max_concurrent_transactions.name())
                .containsMessageWithArguments(
                        dbNamePrefix + " Upgrade transaction from %s to %s started",
                        oldKernelVersion,
                        LatestVersions.LATEST_KERNEL_VERSION)
                .doesNotContainMessageWithArguments(
                        dbNamePrefix + " Upgrade transaction from %s to %s completed",
                        oldKernelVersion,
                        LatestVersions.LATEST_KERNEL_VERSION);

        final var originalNodeCount = originalNodeCount();
        assertThat(getNodeCount()).as("tx triggering upgrade succeeded").isEqualTo(originalNodeCount + 1);
        assertThat(kernelVersion()).isEqualTo(oldKernelVersion);

        // The read transaction has now finished so our 2 transactions should be able to start now.
        try (Transaction tx = db.beginTx()) {
            tx.createNode(); // Write tx to trigger upgrade
            tx.commit();
        }
        assertThat(getNodeCount()).as("tx triggering upgrade succeeded").isEqualTo(originalNodeCount + 2);
        LogAssertions.assertThat(logProvider)
                .containsMessageWithArguments(
                        dbNamePrefix + " Upgrade transaction from %s to %s completed",
                        oldKernelVersion,
                        LatestVersions.LATEST_KERNEL_VERSION);
        assertThat(kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);
    }

    private long getNodeCount() {
        try (Transaction tx = db.beginTx()) {
            return Iterables.count(tx.getAllNodes());
        }
    }

    protected void startDbms() {
        dbms = configure(createDbmsBuilder())
                .setConfig(GraphDatabaseSettings.max_concurrent_transactions, MAX_TRANSACTIONS)
                .build();
        db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        systemDb = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
    }

    protected TestDatabaseManagementServiceBuilder createDbmsBuilder() {
        return new TestDatabaseManagementServiceBuilder();
    }

    protected long originalNodeCount() {
        return ZIPPED_STORE.statistics().nodes();
    }

    private KernelVersion kernelVersion() {
        return get(db, KernelVersionProvider.class).kernelVersion();
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
}
