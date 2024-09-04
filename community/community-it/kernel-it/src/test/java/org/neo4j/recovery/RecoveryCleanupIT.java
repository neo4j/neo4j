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
package org.neo4j.recovery;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;

import java.io.File;
import java.io.IOException;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.Panic;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
class RecoveryCleanupIT {
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService db;
    private TestDatabaseManagementServiceBuilder factory;
    private final ExecutorService executor = Executors.newFixedThreadPool(2);
    private final Label label = Label.label("label");
    private final String propKey = "propKey";
    private final Map<Setting<?>, Object> testSpecificConfig = new HashMap<>();
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup() {
        factory = new TestDatabaseManagementServiceBuilder(testDirectory.homePath());
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (managementService != null) {
            managementService.shutdown();
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    void recoveryCleanupShouldBlockRecoveryWritingToCleanedIndexes()
            throws IOException, ExecutionException, InterruptedException {
        // GIVEN
        dirtyDatabase();

        // WHEN
        Barrier.Control recoveryCompleteBarrier = new Barrier.Control();
        GBPTree.Monitor recoveryBarrierMonitor = new RecoveryBarrierMonitor(recoveryCompleteBarrier);
        setMonitor(recoveryBarrierMonitor);
        Future<GraphDatabaseService> recovery = executor.submit(() -> db = startDatabase());
        recoveryCompleteBarrier.awaitUninterruptibly(); // Ensure we are mid recovery cleanup

        // THEN
        shouldWait(recovery);
        recoveryCompleteBarrier.release();
        recovery.get();
    }

    @Test
    void scanStoreMustLogCrashPointerCleanupDuringRecovery() throws Exception {
        // given
        dirtyDatabase();

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider(true);
        factory.setUserLogProvider(logProvider);
        factory.setInternalLogProvider(logProvider);
        startDatabase();
        managementService.shutdown();

        // then
        assertThat(logProvider)
                .containsMessageWithAll(
                        "Schema index cleanup job registered",
                        "descriptor",
                        "type='LOOKUP'",
                        "schema=(:<any-labels>)",
                        "indexFile=")
                .containsMessageWithAll(
                        "Schema index cleanup job started",
                        "descriptor",
                        "type='LOOKUP'",
                        "schema=(:<any-labels>)",
                        "indexFile=")
                .containsMessageWithAll(
                        "Schema index cleanup job closed",
                        "descriptor",
                        "type='LOOKUP'",
                        "schema=(:<any-labels>)",
                        "indexFile=")
                .containsMessageWithAll(
                        "Schema index cleanup job finished",
                        "descriptor",
                        "type='LOOKUP'",
                        "schema=(:<any-labels>)",
                        "indexFile=",
                        "Number of pages visited",
                        "Number of cleaned crashed pointers",
                        "Time spent");
    }

    @Test
    void nativeIndexRangeMustLogCrashPointerCleanupDuringRecovery() throws Exception {
        // given
        dirtyDatabase();

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider(true);
        factory.setInternalLogProvider(logProvider);
        startDatabase();
        managementService.shutdown();

        // then
        String providerString = AllIndexProviderDescriptors.RANGE_DESCRIPTOR.name();
        assertThat(logProvider)
                .containsMessageWithAll(indexRecoveryLogMatcher("Schema index cleanup job registered", providerString))
                .containsMessageWithAll(indexRecoveryLogMatcher("Schema index cleanup job started", providerString))
                .containsMessageWithAll(indexRecoveryFinishedLogMatcher(providerString))
                .containsMessageWithAll(indexRecoveryLogMatcher("Schema index cleanup job closed", providerString));
    }

    private static String[] indexRecoveryLogMatcher(String logMessage, String providerString) {
        return new String[] {logMessage, "descriptor", "type='RANGE'", "indexFile=", File.separator + providerString};
    }

    private static String[] indexRecoveryFinishedLogMatcher(String providerString) {
        return new String[] {
            "Schema index cleanup job finished",
            "descriptor",
            "type='RANGE'",
            "indexFile=",
            File.separator + providerString,
            "Number of pages visited",
            "Number of cleaned crashed pointers",
            "Time spent"
        };
    }

    private void dirtyDatabase() throws IOException {
        db = startDatabase();

        Panic databasePanic = databaseHealth(db);
        index(db);
        someData(db);
        checkpoint(db);
        someData(db);
        databasePanic.panic(new Throwable("Trigger recovery on next startup"));
        managementService.shutdown();
        db = null;
    }

    private <T> void setTestConfig(Setting<T> setting, T value) {
        testSpecificConfig.put(setting, value);
    }

    private void setMonitor(Object monitor) {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener(monitor);
        factory.setMonitors(monitors);
    }

    private void index(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(label).on(propKey).create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }
    }

    private static void checkpoint(GraphDatabaseService db) throws IOException {
        CheckPointer checkPointer = checkPointer(db);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("test"));
    }

    private void someData(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            tx.createNode(label).setProperty(propKey, 1);
            tx.createNode(label).setProperty(propKey, "string");
            tx.createNode(label).setProperty(propKey, Values.pointValue(CARTESIAN, 0.5, 0.5));
            tx.createNode(label).setProperty(propKey, LocalTime.of(0, 0));
            tx.commit();
        }
    }

    private static void shouldWait(Future<?> future) {
        assertThrows(TimeoutException.class, () -> future.get(200L, TimeUnit.MILLISECONDS));
    }

    private GraphDatabaseService startDatabase() {
        factory.setConfig(testSpecificConfig);
        managementService = factory.build();
        return managementService.database(DEFAULT_DATABASE_NAME);
    }

    private static Panic databaseHealth(GraphDatabaseService db) {
        return dependencyResolver(db).resolveDependency(DatabaseHealth.class);
    }

    private static CheckPointer checkPointer(GraphDatabaseService db) {
        DependencyResolver dependencyResolver = dependencyResolver(db);
        return dependencyResolver
                .resolveDependency(Database.class)
                .getDependencyResolver()
                .resolveDependency(CheckPointer.class);
    }

    private static DependencyResolver dependencyResolver(GraphDatabaseService db) {
        return ((GraphDatabaseAPI) db).getDependencyResolver();
    }

    private static class RecoveryBarrierMonitor extends GBPTree.Monitor.Adaptor {
        private final Barrier.Control barrier;

        RecoveryBarrierMonitor(Barrier.Control barrier) {
            this.barrier = barrier;
        }

        @Override
        public void cleanupFinished(
                long numberOfPagesVisited,
                long numberOfTreeNodes,
                long numberOfCleanedCrashPointers,
                long durationMillis) {
            barrier.reached();
        }
    }
}
