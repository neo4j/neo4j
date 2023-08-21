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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.kernel.recovery.RecoveryHelpers.getLatestCheckpoint;
import static org.neo4j.kernel.recovery.RecoveryHelpers.logsContainCheckpoint;
import static org.neo4j.kernel.recovery.RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile;
import static org.neo4j.test.UpgradeTestUtil.assertKernelVersion;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
class RecoveryToFutureOverUpgradedVersionsIT {
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private Neo4jLayout neo4jLayout;

    private DatabaseManagementService managementService;
    private GraphDatabaseAPI systemDb;

    @BeforeEach
    void setUp() {
        startDbms(builder -> builder, false);
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

    @ParameterizedTest
    @ValueSource(strings = {DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME})
    void recoverDatabaseOnOldVersionNoCheckpoints(String dbName) throws Throwable {
        GraphDatabaseAPI testDb = (GraphDatabaseAPI) managementService.database(dbName);
        DatabaseLayout dbLayout = testDb.databaseLayout();
        createWriteTransaction(testDb);
        shutdownDbms();

        // shutdown and init checkpoints
        removeLastCheckpointRecordFromLastLogFile(dbLayout, fileSystem);
        removeLastCheckpointRecordFromLastLogFile(dbLayout, fileSystem);

        assertFalse(logsContainCheckpoint(dbLayout, fileSystem));

        startDbms(this::configureGloriousFutureAsLatest, true);
        testDb = (GraphDatabaseAPI) managementService.database(dbName);
        assertKernelVersion(testDb, LatestVersions.LATEST_KERNEL_VERSION);
    }

    @ParameterizedTest
    @ValueSource(strings = {DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME})
    void recoverDatabaseOnOldVersionNoCheckpointsAndContainsUpgradeTransaction(String dbName) throws Throwable {
        shutdownDbms();
        startDbms(this::configureGloriousFutureAsLatest, false);

        GraphDatabaseAPI testDb = (GraphDatabaseAPI) managementService.database(dbName);
        DatabaseLayout dbLayout = testDb.databaseLayout();
        createWriteTransaction(testDb);
        systemDb.executeTransactionally("CALL dbms.upgrade()");
        createWriteTransaction(testDb);
        assertKernelVersion(testDb, KernelVersion.GLORIOUS_FUTURE);

        shutdownDbms();
        var config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.latest_kernel_version, KernelVersion.GLORIOUS_FUTURE.version())
                .build();
        // shutdown and init checkpoints
        removeLastCheckpointRecordFromLastLogFile(dbLayout, fileSystem, config);
        removeLastCheckpointRecordFromLastLogFile(dbLayout, fileSystem, config);

        removeLastCheckpointRecordFromLastLogFile(dbLayout, fileSystem, config);
        assertFalse(logsContainCheckpoint(dbLayout, fileSystem));

        startDbms(this::configureGloriousFutureAsLatest, true);
        testDb = (GraphDatabaseAPI) managementService.database(dbName);
        assertKernelVersion(testDb, KernelVersion.GLORIOUS_FUTURE);
    }

    @ParameterizedTest
    @ValueSource(strings = {DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME})
    void recoverDatabaseOnOldVersionOneCheckpoint(String dbName) throws Throwable {
        GraphDatabaseAPI testDb = (GraphDatabaseAPI) managementService.database(dbName);
        DatabaseLayout dbLayout = testDb.databaseLayout();

        createWriteTransaction(testDb);
        CheckPointer checkPointer = testDb.getDependencyResolver().resolveDependency(CheckPointer.class);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("extra checkpoint"));
        createWriteTransaction(testDb);
        assertKernelVersion(testDb, LatestVersions.LATEST_KERNEL_VERSION);
        shutdownDbms();

        removeLastCheckpointRecordFromLastLogFile(dbLayout, fileSystem);
        assertThat(getLatestCheckpoint(dbLayout, fileSystem).kernelVersion())
                .isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);

        startDbms(this::configureGloriousFutureAsLatest, true);
        testDb = (GraphDatabaseAPI) managementService.database(dbName);
        assertKernelVersion(testDb, LatestVersions.LATEST_KERNEL_VERSION);
    }

    @ParameterizedTest
    @ValueSource(strings = {DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME})
    void recoverDatabaseOnOldVersionOneCheckpointAndContainsUpgradeTransaction(String dbName) throws Throwable {
        shutdownDbms();
        startDbms(this::configureGloriousFutureAsLatest, false);

        GraphDatabaseAPI testDb = (GraphDatabaseAPI) managementService.database(dbName);
        DatabaseLayout dbLayout = testDb.databaseLayout();

        createWriteTransaction(testDb);
        CheckPointer checkPointer = testDb.getDependencyResolver().resolveDependency(CheckPointer.class);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("extra checkpoint"));
        systemDb.executeTransactionally("CALL dbms.upgrade()");
        createWriteTransaction(testDb);
        assertKernelVersion(testDb, KernelVersion.GLORIOUS_FUTURE);
        shutdownDbms();

        var config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.latest_kernel_version, KernelVersion.GLORIOUS_FUTURE.version())
                .build();
        removeLastCheckpointRecordFromLastLogFile(dbLayout, fileSystem, config);
        assertThat(getLatestCheckpoint(dbLayout, fileSystem, config).kernelVersion())
                .isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);

        startDbms(this::configureGloriousFutureAsLatest, true);
        testDb = (GraphDatabaseAPI) managementService.database(dbName);
        assertKernelVersion(testDb, KernelVersion.GLORIOUS_FUTURE);
    }

    @Test
    void recoverOldSystemDatabaseNoTransactionLogs() throws Throwable {
        String dbName = SYSTEM_DATABASE_NAME;
        GraphDatabaseAPI testDb = (GraphDatabaseAPI) managementService.database(dbName);
        DatabaseLayout dbLayout = testDb.databaseLayout();
        createWriteTransaction(testDb);
        shutdownDbms();

        removeTransactionLogs(dbLayout);

        startDbms(
                builder -> configureGloriousFutureAsLatest(builder)
                        .setConfig(GraphDatabaseSettings.fail_on_missing_files, false),
                false);
        testDb = (GraphDatabaseAPI) managementService.database(dbName);

        // It doesn't matter that it is an old database and that no upgrade has
        // really happened (dbmsRuntimeVersionComponent is still old).
        // For system database we have no idea about the contents of the database while starting it
        // and have to pick a version when there are no logs at all - the latest.
        assertKernelVersion(testDb, KernelVersion.GLORIOUS_FUTURE);
    }

    @Test
    void recoverOldDatabaseNoTransactionLogs() throws Throwable {
        String dbName = DEFAULT_DATABASE_NAME;
        GraphDatabaseAPI testDb = (GraphDatabaseAPI) managementService.database(dbName);
        DatabaseLayout dbLayout = testDb.databaseLayout();
        createWriteTransaction(testDb);
        shutdownDbms();

        removeTransactionLogs(dbLayout);

        startDbms(
                builder -> configureGloriousFutureAsLatest(builder)
                        .setConfig(GraphDatabaseSettings.fail_on_missing_files, false),
                false);
        testDb = (GraphDatabaseAPI) managementService.database(dbName);

        // For a regular database where we have no logs at all we should pick the version that
        // dbmsRuntimeVersionComponent tells us that we are on.
        assertKernelVersion(testDb, LatestVersions.LATEST_KERNEL_VERSION);
    }

    private void removeTransactionLogs(DatabaseLayout dbLayout) throws IOException {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder(dbLayout.getTransactionLogsDirectory(), fileSystem)
                .withCommandReaderFactory(StorageEngineFactory.selectStorageEngine(fileSystem, dbLayout, null)
                        .commandReaderFactory())
                .build();
        for (Path logFile : fileSystem.listFiles(logFiles.logFilesDirectory())) {
            fileSystem.deleteFile(logFile);
        }
    }

    private void startDbms(Configuration configuration, boolean allowAutomaticUpgrade) {
        managementService = configuration
                .configure(new TestDatabaseManagementServiceBuilder(neo4jLayout)
                        .setConfig(preallocate_logical_logs, false)
                        .setConfig(GraphDatabaseSettings.keep_logical_logs, "keep_all")
                        .setConfig(automatic_upgrade_enabled, allowAutomaticUpgrade))
                .build();
        systemDb = (GraphDatabaseAPI) managementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
    }

    private void shutdownDbms() {
        if (managementService != null) {
            managementService.shutdown();
            managementService = null;
        }
    }

    private void createWriteTransaction(GraphDatabaseAPI db) {
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
    }

    @FunctionalInterface
    interface Configuration {
        TestDatabaseManagementServiceBuilder configure(TestDatabaseManagementServiceBuilder builder);
    }
}
