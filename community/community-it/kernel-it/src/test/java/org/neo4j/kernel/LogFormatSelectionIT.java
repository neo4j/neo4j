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
package org.neo4j.kernel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.cli.CommandTestUtils.capturingExecutionContext;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.internal.helpers.collection.MapUtil.store;
import static org.neo4j.kernel.recovery.RecoveryHelpers.getLatestCheckpoint;
import static org.neo4j.kernel.recovery.RecoveryHelpers.removeLastCheckpointRecordFromLogFile;
import static org.neo4j.storemigration.StoreMigrationTestUtils.runStoreMigrationCommandFromSameJvm;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;
import static org.neo4j.test.UpgradeTestUtil.assertKernelVersion;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.Transaction;
import org.neo4j.importer.ImportCommand;
import org.neo4j.internal.batchimport.input.csv.Type;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storemigration.StoreMigrationTestUtils;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.UpgradeTestUtil;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.wal.LogFormatVersionProvider;
import org.neo4j.wal.WALService;
import org.neo4j.wal.entry.LogFormat;
import org.neo4j.wal.files.LogFilesBuilder;
import picocli.CommandLine;

@Neo4jLayoutExtension
class LogFormatSelectionIT {

    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fs;

    private TestDatabaseManagementServiceBuilder builder;
    private DatabaseManagementService managementService;

    public static Stream<Arguments> formatSwitchAllowedAndDbName() {
        return Stream.of(
                Arguments.arguments(DEFAULT_DATABASE_NAME, true),
                Arguments.arguments(DEFAULT_DATABASE_NAME, false),
                Arguments.arguments(SYSTEM_DATABASE_NAME, true),
                Arguments.arguments(SYSTEM_DATABASE_NAME, false));
    }

    @AfterEach
    void shutdown() {
        if (managementService != null) {
            managementService.shutdown();
            managementService = null;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME})
    void logFormatProviderShouldBeUpdatedOnUpgrade(String dbName) throws Throwable {
        ZippedStoreCommunity.REC_AF11_V50_ALL.unzip(neo4jLayout.homeDirectory());

        createBuilderNoAutomaticUpgrade();
        managementService = builder.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(dbName);
        assertKernelVersionAndLogFormat(db, KernelVersion.V5_0);

        UpgradeTestUtil.upgradeDatabase(
                managementService, db, KernelVersion.V5_0, LatestVersions.LATEST_KERNEL_VERSION);
        assertKernelVersionAndLogFormat(db, LatestVersions.LATEST_KERNEL_VERSION);
        WALService walService = db.getDependencyResolver().resolveDependency(WALService.class);
        shutdown();
        checkLogFormatOfLatestFiles(walService, LatestVersions.LATEST_KERNEL_VERSION);
    }

    @ParameterizedTest
    @MethodSource("formatSwitchAllowedAndDbName")
    void upgradeToFuture(String dbName, boolean allowFormatSwitchOnUpgrade) throws IOException {
        LogFormat expectedFormat = allowFormatSwitchOnUpgrade
                ? LogFormat.fromKernelVersion(KernelVersion.GLORIOUS_FUTURE)
                : LogFormat.fromKernelVersion(LatestVersions.LATEST_KERNEL_VERSION);

        createBuilder();
        builder.setConfig(GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, false);
        managementService = builder.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(dbName);

        // Some data
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
        shutdown();
        createBuilderNoAutomaticUpgrade();
        builder.setConfig(
                GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, allowFormatSwitchOnUpgrade);
        managementService = configureGloriousFutureAsLatest(builder).build();
        db = (GraphDatabaseAPI) managementService.database(dbName);

        assertKernelVersionAndLogFormat(db, LatestVersions.LATEST_KERNEL_VERSION);

        UpgradeTestUtil.upgradeDatabase(
                managementService, db, LatestVersions.LATEST_KERNEL_VERSION, KernelVersion.GLORIOUS_FUTURE);
        assertKernelVersion(db, KernelVersion.GLORIOUS_FUTURE);
        assertLogFormat(db, expectedFormat);

        WALService walService = db.getDependencyResolver().resolveDependency(WALService.class);
        shutdown();
        checkLogFormatOfLatestFiles(walService, expectedFormat);
    }

    @ParameterizedTest
    @MethodSource("formatSwitchAllowedAndDbName")
    void formatOnNewDb(String dbName, boolean allowFormatSwitchOnUpgrade) throws IOException {
        // Should only allow to use the new format if setting says okay
        LogFormat expectedFormat = allowFormatSwitchOnUpgrade
                ? LogFormat.V11
                : LogFormat.fromKernelVersion(LatestVersions.LATEST_KERNEL_VERSION);

        createBuilder();
        builder.setConfig(
                GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, allowFormatSwitchOnUpgrade);
        managementService = configureGloriousFutureAsLatest(builder).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(dbName);

        // Some data
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }

        assertKernelVersion(db, KernelVersion.GLORIOUS_FUTURE);
        assertLogFormat(db, expectedFormat);
        WALService walService = db.getDependencyResolver().resolveDependency(WALService.class);
        shutdown();
        checkLogFormatOfLatestFiles(walService, expectedFormat);
    }

    @SkipOnSpd(
            reason =
                    "Cluster startup/recovery doesn't seem to handle missing tx logs, even if told that it's OK. See ClusterIllegalSeedingIT#shouldFailToStartReadReplicaWithNoLogsButStandaloneShouldPass."
                            + " To start with, the recovery facade for cluster members runs recovery with an explicit THROWING_PROVIDER_RECOVERY.",
            notes = {SkipOnSpd.Note.notSupported})
    @ParameterizedTest
    @MethodSource("formatSwitchAllowedAndDbName")
    void startUpWithoutLogFiles(String dbName, boolean allowFormatSwitchOnUpgrade) throws IOException {
        LogFormat expectedFormat = allowFormatSwitchOnUpgrade
                ? LogFormat.fromKernelVersion(KernelVersion.GLORIOUS_FUTURE)
                : LogFormat.fromKernelVersion(LatestVersions.LATEST_KERNEL_VERSION);

        createBuilder();
        managementService = builder.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(dbName);

        // Some data
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
        shutdown();
        fs.deleteRecursively(neo4jLayout.databaseLayout(dbName).getTransactionLogsDirectory());

        createBuilderNoAutomaticUpgrade();
        builder.setConfig(
                GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, allowFormatSwitchOnUpgrade);
        builder.setConfig(GraphDatabaseSettings.fail_on_missing_files, false);
        managementService = configureGloriousFutureAsLatest(builder).build();
        db = (GraphDatabaseAPI) managementService.database(dbName);

        if (!SYSTEM_DATABASE_NAME.equals(dbName)) { // System takes latest version on startup without logs
            assertKernelVersion(db, LatestVersions.LATEST_KERNEL_VERSION);
            var config = db.getDependencyResolver().resolveDependency(Config.class);
            assertLogFormat(db, LogFormat.fromConfigAndKernelVersion(config, LATEST_KERNEL_VERSION));

            UpgradeTestUtil.upgradeDatabase(
                    managementService, db, LatestVersions.LATEST_KERNEL_VERSION, KernelVersion.GLORIOUS_FUTURE);
        }
        assertKernelVersion(db, KernelVersion.GLORIOUS_FUTURE);
        assertLogFormat(db, expectedFormat);

        WALService walService = db.getDependencyResolver().resolveDependency(WALService.class);
        shutdown();
        checkLogFormatOfLatestFiles(walService, expectedFormat);
    }

    @ParameterizedTest
    @MethodSource("formatSwitchAllowedAndDbName")
    @SkipOnSpd(reason = "System is being auto-upgraded on startup (not sure there is a way to turn that off)")
    void recoveryOverUpgradeTransaction(String dbName, boolean allowFormatSwitchOnUpgrade) throws Throwable {
        LogFormat expectedFormat = newFormatExpected(allowFormatSwitchOnUpgrade);

        createBuilder();
        managementService = builder.build();
        shutdown();

        builder = configureGloriousFutureAsLatest(builder)
                .setConfig(
                        GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create,
                        allowFormatSwitchOnUpgrade);
        managementService = builder.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(dbName);
        DatabaseLayout dbLayout = db.databaseLayout();
        UpgradeTestUtil.upgradeDatabase(
                managementService, db, LatestVersions.LATEST_KERNEL_VERSION, KernelVersion.GLORIOUS_FUTURE);
        shutdown();

        removeLastCheckpointRecordFromLogFile(
                dbLayout,
                fs,
                Config.defaults(
                        GraphDatabaseInternalSettings.latest_kernel_version, KernelVersion.GLORIOUS_FUTURE.version()));
        assertThat(getLatestCheckpoint(dbLayout, fs).kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);

        // Turn on the setting now and see that old decision is respected during recovery anyway.
        managementService = builder.setConfig(
                        GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, true)
                .build();
        db = (GraphDatabaseAPI) managementService.database(dbName);
        WALService walService = db.getDependencyResolver().resolveDependency(WALService.class);

        shutdown();
        checkLogFormatOfLatestFiles(walService, expectedFormat);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SkipOnSpd(reason = "Can't migrate spd databases")
    void migrate(boolean allowFormatSwitchOnUpgrade) throws Throwable {
        LogFormat expectedFormat = allowFormatSwitchOnUpgrade
                ? LogFormat.fromKernelVersion(KernelVersion.GLORIOUS_FUTURE)
                : LogFormat.fromKernelVersion(LatestVersions.LATEST_KERNEL_VERSION);

        createBuilder();
        builder.setConfig(GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, false);
        managementService = builder.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);

        WALService walService = db.getDependencyResolver().resolveDependency(WALService.class);
        shutdown();

        Path config = neo4jLayout.homeDirectory().resolve("migration-config.conf");
        MapUtil.store(
                Map.of(
                        GraphDatabaseInternalSettings.latest_kernel_version.name(),
                        "" + KernelVersion.GLORIOUS_FUTURE.versionAsInt(),
                        GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create.name(),
                        Boolean.toString(allowFormatSwitchOnUpgrade)),
                config);
        String[] args = {"--verbose", "--additional-config", config.toString(), DEFAULT_DATABASE_NAME};

        StoreMigrationTestUtils.Result result = runStoreMigrationCommandFromSameJvm(neo4jLayout, args);
        assertThat(result.exitCode()).withFailMessage(result.err()).isEqualTo(0);

        checkLogFormatOfLatestFiles(walService, expectedFormat);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void importSelectsLogFormatBasedOnSetting(boolean allowNewFormat) throws Exception {
        // Should only allow to use the new format if setting says okay
        LogFormat expectedFormat =
                allowNewFormat ? LogFormat.V11 : LogFormat.fromKernelVersion(LatestVersions.LATEST_KERNEL_VERSION);

        // GIVEN
        Path dbConfig = testDirectory.file("neo4j.properties");
        store(
                Map.of(
                        neo4j_home.name(),
                        testDirectory.absolutePath().toString(),
                        preallocate_logical_logs.name(),
                        FALSE,
                        GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create.name(),
                        Boolean.toString(allowNewFormat),
                        GraphDatabaseInternalSettings.latest_kernel_version.name(),
                        "" + KernelVersion.GLORIOUS_FUTURE.versionAsInt(),
                        GraphDatabaseInternalSettings.latest_runtime_version.name(),
                        "" + DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion()),
                dbConfig);

        // WHEN
        var ctx = capturingExecutionContext(
                testDirectory.absolutePath(),
                testDirectory.absolutePath().resolve("conf"),
                testDirectory.getFileSystem());
        runImport(
                ctx,
                "--report-file",
                testDirectory.file("import.report").toAbsolutePath().toString(),
                "--additional-config",
                dbConfig.toAbsolutePath().toString(),
                "--nodes",
                nodeData().toAbsolutePath().toString());

        // THEN
        assertTrue(ctx.outAsString().contains("IMPORT DONE"));
        WALService walService = LogFilesBuilder.readableBuilder(
                        neo4jLayout.databaseLayout(DEFAULT_DATABASE_NAME),
                        fs,
                        KernelVersionProvider.THROWING_PROVIDER,
                        LogFormatVersionProvider.THROWING_PROVIDER)
                .build();
        checkLogFormatOfLatestFiles(walService, expectedFormat);
    }

    private static LogFormat newFormatExpected(boolean allowFormatSwitchOnUpgrade) {
        if (allowFormatSwitchOnUpgrade) {
            return LogFormat.V11;
        }
        return LATEST_LOG_FORMAT;
    }

    private void runImport(ExecutionContext ctx, String... arguments) throws Exception {
        final var cmd = new ImportCommand.Full(ctx);
        new CommandLine(cmd).setUseSimplifiedAtFiles(true).parseArgs(arguments);
        cmd.execute();
    }

    private Path nodeData() throws Exception {
        Path file = testDirectory.file("nodes.csv");
        try (PrintStream writer = new PrintStream(Files.newOutputStream(file), false, StandardCharsets.UTF_8)) {
            writeNodeHeader(writer);
            writer.println("NODE1" + COMMAS.delimiter() + "name" + COMMAS.delimiter() + "LabelName");
        }
        return file;
    }

    private static void writeNodeHeader(PrintStream writer) {
        writer.println("id:" + Type.ID.name() + COMMAS.delimiter() + "name" + COMMAS.delimiter() + "labels:LABEL");
    }

    private static void checkLogFormatOfLatestFiles(WALService walService, KernelVersion kernelVersion)
            throws IOException {
        checkLogFormatOfLatestFiles(walService, LogFormat.fromKernelVersion(kernelVersion));
    }

    private static void checkLogFormatOfLatestFiles(WALService walService, LogFormat expectedFormat)
            throws IOException {
        assertThat(walService
                        .getLogFile()
                        .extractHeader(walService.getLogFile().getLogRangeInfo().highestVersion())
                        .getLogFormatVersion())
                .isEqualTo(expectedFormat);
        assertThat(walService
                        .getCheckpointFile()
                        .extractHeader(
                                walService.getCheckpointFile().getLogRangeInfo().highestVersion())
                        .getLogFormatVersion())
                .isEqualTo(expectedFormat);
    }

    private TestDatabaseManagementServiceBuilder configureGloriousFutureAsLatest(
            TestDatabaseManagementServiceBuilder builder) {
        return builder.setConfig(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion())
                .setConfig(
                        GraphDatabaseInternalSettings.latest_kernel_version, KernelVersion.GLORIOUS_FUTURE.version());
    }

    void assertKernelVersionAndLogFormat(GraphDatabaseAPI db, KernelVersion expectedKernelVersion) {
        assertKernelVersion(db, expectedKernelVersion);
        assertLogFormat(db, LogFormat.fromKernelVersion(expectedKernelVersion));
    }

    private static void assertLogFormat(GraphDatabaseAPI db, LogFormat expectedFormat) {
        assertThat(db.getDependencyResolver()
                        .resolveDependency(LogFormatVersionProvider.class)
                        .getCurrentLogFormat())
                .isEqualTo(expectedFormat);
    }

    private void createBuilder() {
        builder = new TestDatabaseManagementServiceBuilder(neo4jLayout)
                .setConfig(preallocate_logical_logs, false)
                .setConfig(GraphDatabaseSettings.keep_logical_logs, "keep_all");
    }

    private void createBuilderNoAutomaticUpgrade() {
        builder = new TestDatabaseManagementServiceBuilder(neo4jLayout)
                .setConfig(preallocate_logical_logs, false)
                .setConfig(GraphDatabaseSettings.keep_logical_logs, "keep_all")
                .setConfig(GraphDatabaseInternalSettings.automatic_upgrade_enabled, false);
    }
}
