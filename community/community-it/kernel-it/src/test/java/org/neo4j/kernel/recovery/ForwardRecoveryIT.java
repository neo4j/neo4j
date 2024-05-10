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
package org.neo4j.kernel.recovery;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.kernel.database.DatabaseTracers.EMPTY;
import static org.neo4j.kernel.recovery.Recovery.context;
import static org.neo4j.kernel.recovery.RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.monitoring.Monitors;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
@Neo4jLayoutExtension
class ForwardRecoveryIT {

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private TestDirectory testDirectory;

    private DatabaseLayout databaseLayout;
    private DatabaseManagementService managementService;
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private Monitors monitors;
    private RecoveryMonitorListener recoveryMonitorListener;

    @BeforeEach
    void setUp() {
        databaseLayout = neo4jLayout.databaseLayout(DEFAULT_DATABASE_NAME);
        monitors = new Monitors();
        recoveryMonitorListener = new RecoveryMonitorListener(logProvider);
        monitors.addMonitorListener(new LoggingLogFileMonitor(logProvider.getLog(getClass())));
        monitors.addMonitorListener(recoveryMonitorListener);
    }

    @AfterEach
    void tearDown() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @Test
    void forwardModeRecovery() throws Exception {
        var database = createDatabase();
        generateData(database);

        database.getDependencyResolver()
                .resolveDependency(CheckPointer.class)
                .forceCheckPoint(new SimpleTriggerInfo("test"));

        var copyPath = copyStoreFiles();

        // create 2 nodes per round of 10 iterations * 10 times
        for (int i = 0; i < 10; i++) {
            generateData(database);
        }

        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem);
        restoreStoreFiles(copyPath);
        logProvider.clear();

        recoverDatabase();

        var restartedDb = createDatabase();
        try (var transaction = restartedDb.beginTx()) {
            // 11 rounds in total of 2 nodes creation 10 times
            assertEquals(220, transaction.getAllNodes().stream().count());
        }
        LogAssertions.assertThat(logProvider)
                .containsMessages("Recovery in 'forward' mode completed.")
                .containsMessages(
                        "100 transactions applied, 0 not completed transactions rolled back, skipped applying 0 previously rolled back transactions.");
    }

    @Test
    void forwardModeRecoveryProgress() throws Exception {
        var database = createDatabase();
        generateData(database);

        database.getDependencyResolver()
                .resolveDependency(CheckPointer.class)
                .forceCheckPoint(new SimpleTriggerInfo("test"));

        var copyPath = copyStoreFiles();

        // create 2 nodes per round of 10 iterations * 10 times
        for (int i = 0; i < 10; i++) {
            generateData(database);
        }

        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem);
        restoreStoreFiles(copyPath);
        logProvider.clear();

        recoverDatabase();

        assertFalse(recoveryMonitorListener.isReverseCompletionCalled());
        // in total we should have only 100 batches: 10 * 10
        assertEquals(100, recoveryMonitorListener.getObservedBatches());
        assertThat(recoveryMonitorListener.getAfterTwentyBatchesMessage()).contains("20% completed");
    }

    private void restoreStoreFiles(Path copyPath) throws IOException {
        fileSystem.copyRecursively(copyPath, databaseLayout.databaseDirectory());
    }

    private Path copyStoreFiles() throws IOException {
        Path storeCopy = testDirectory.directory("storeCopy");
        fileSystem.copyRecursively(databaseLayout.databaseDirectory(), storeCopy);
        return storeCopy;
    }

    private void recoverDatabase() throws Exception {
        Config config = Config.newBuilder().build();
        LogFiles logFiles = buildLogFiles(EMPTY);
        assertTrue(isRecoveryRequired(databaseLayout, config, logFiles, EMPTY));

        Recovery.performRecovery(context(
                        fileSystem,
                        pageCache,
                        EMPTY,
                        config,
                        databaseLayout,
                        INSTANCE,
                        IOController.DISABLED,
                        logProvider,
                        logFiles.getTailMetadata())
                .recoveryMode(RecoveryMode.FORWARD)
                .monitors(monitors)
                .extensionFactories(Iterables.cast(Services.loadAll(ExtensionFactory.class)))
                .startupChecker(RecoveryStartupChecker.EMPTY_CHECKER));
        assertFalse(isRecoveryRequired(databaseLayout, config, buildLogFiles(), EMPTY));
    }

    private boolean isRecoveryRequired(DatabaseLayout layout, Config config, LogFiles logFiles, DatabaseTracers tracers)
            throws Exception {
        return Recovery.isRecoveryRequired(
                fileSystem, pageCache, layout, config, Optional.of(logFiles.getTailMetadata()), INSTANCE, tracers);
    }

    private LogFiles buildLogFiles() throws IOException {
        return buildLogFiles(EMPTY);
    }

    private LogFiles buildLogFiles(DatabaseTracers databaseTracers) throws IOException {
        return LogFilesBuilder.activeFilesBuilder(
                        databaseLayout, fileSystem, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withCommandReaderFactory(StorageEngineFactory.selectStorageEngine(fileSystem, databaseLayout, null)
                        .commandReaderFactory())
                .withDatabaseTracers(databaseTracers)
                .build();
    }

    private GraphDatabaseAPI createDatabase() {
        return createDatabase(logical_log_rotation_threshold.defaultValue());
    }

    protected GraphDatabaseAPI createDatabase(long logThreshold) {
        var builder = createBuilder(logThreshold);
        managementService = builder.build();
        return (GraphDatabaseAPI) managementService.database(databaseLayout.getDatabaseName());
    }

    private TestDatabaseManagementServiceBuilder createBuilder(long logThreshold) {
        return new TestDatabaseManagementServiceBuilder(neo4jLayout)
                .setConfig(preallocate_logical_logs, false)
                .setInternalLogProvider(logProvider)
                .setMonitors(monitors)
                .setConfig(GraphDatabaseSettings.keep_logical_logs, "keep_all")
                .setConfig(logical_log_rotation_threshold, logThreshold);
    }

    private static void generateData(GraphDatabaseService database) {
        for (int i = 0; i < 10; i++) {
            try (var transaction = database.beginTx()) {
                Node node1 = transaction.createNode();
                Node node2 = transaction.createNode();
                node1.createRelationshipTo(node2, withName("Type" + i));
                node2.setProperty("a", randomAlphanumeric(5));
                transaction.commit();
            }
        }
    }

    private static class RecoveryMonitorListener implements RecoveryMonitor {

        private final AssertableLogProvider monitorLog;
        private volatile boolean reverseCompletionCalled;
        private volatile String afterTwentyBatchesMessage;
        private final AtomicInteger batchesCounter = new AtomicInteger();

        public RecoveryMonitorListener(AssertableLogProvider logProvider) {
            monitorLog = logProvider;
        }

        @Override
        public void batchRecovered(CommittedCommandBatch committedBatch) {
            batchesCounter.incrementAndGet();
            if (batchesCounter.get() == 21) {
                afterTwentyBatchesMessage =
                        Iterables.last(monitorLog.serialize().lines().toList());
            }
        }

        @Override
        public void reverseStoreRecoveryCompleted(long lowestRecoveredAppendIndex) {
            reverseCompletionCalled = true;
        }

        public boolean isReverseCompletionCalled() {
            return reverseCompletionCalled;
        }

        public String getAfterTwentyBatchesMessage() {
            return afterTwentyBatchesMessage;
        }

        public int getObservedBatches() {
            return batchesCounter.get();
        }
    }
}
