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
package org.neo4j.kernel.diagnostics.providers;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.DiagnosticsProvider;
import org.neo4j.io.device.DeviceMapper;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DbmsDiagnosticsManagerTest {
    private static final NamedDatabaseId DEFAULT_DATABASE_ID = from(DEFAULT_DATABASE_NAME, UUID.randomUUID());

    @Inject
    private TestDirectory directory;

    private DbmsDiagnosticsManager diagnosticsManager;
    private AssertableLogProvider logProvider;
    private DatabaseContextProvider<StandaloneDatabaseContext> databaseContextProvider;
    private StorageEngine storageEngine;
    private StorageEngineFactory storageEngineFactory;
    private Database defaultDatabase;
    private StandaloneDatabaseContext defaultContext;
    private Dependencies dependencies;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws IOException {
        logProvider = new AssertableLogProvider();
        databaseContextProvider = mock(DatabaseContextProvider.class);

        storageEngine = mock(StorageEngine.class);
        storageEngineFactory = mock(StorageEngineFactory.class);
        defaultContext = mock(StandaloneDatabaseContext.class);
        defaultDatabase = prepareDatabase();
        when(storageEngineFactory.listStorageFiles(any(), any())).thenReturn(Collections.emptyList());

        dependencies = dependenciesOf(Config.defaults(), databaseContextProvider);

        when(defaultContext.database()).thenReturn(defaultDatabase);
        when(defaultContext.optionalDatabase()).thenReturn(Optional.of(defaultDatabase));
        when(databaseContextProvider.getDatabaseContext(DEFAULT_DATABASE_ID)).thenReturn(Optional.of(defaultContext));
        when(databaseContextProvider.registeredDatabases())
                .thenReturn(new TreeMap<>(singletonMap(DEFAULT_DATABASE_ID, defaultContext)));

        diagnosticsManager = new DbmsDiagnosticsManager(dependencies, new SimpleLogService(logProvider));
    }

    @Test
    void dumpSystemDiagnostics() {
        assertThat(logProvider).doesNotHaveAnyLogs();

        diagnosticsManager.dumpSystemDiagnostics();

        assertContainsSystemDiagnostics();
    }

    @Test
    void dumpDatabaseDiagnostics() {
        assertThat(logProvider).doesNotHaveAnyLogs();

        diagnosticsManager.dumpDatabaseDiagnostics(defaultDatabase);

        assertContainsDatabaseDiagnostics();
    }

    @Test
    void dumpDatabaseDiagnosticsNotInterleavedWithEachother() throws Throwable {
        Database secondDatabase = prepareDatabase(DatabaseIdFactory.from("second", UUID.randomUUID()));
        assertThat(logProvider).doesNotHaveAnyLogs();

        Race race = new Race();
        race.addContestant(() -> diagnosticsManager.dumpDatabaseDiagnostics(defaultDatabase));
        race.addContestant(() -> diagnosticsManager.dumpDatabaseDiagnostics(secondDatabase));
        race.go();

        // Assert that diagnostics messages from the two databases are not interleaved.
        // If they are the sequence of the diagnostics provider headers will not be ordered correctly.
        assertThat(logProvider.serialize())
                .containsSubsequence(
                        "Database: ",
                        "Version",
                        "Store files",
                        "Transaction log",
                        "Database: ",
                        "Version",
                        "Store files",
                        "Transaction log");
    }

    @Test
    void dumpDatabaseDiagnosticsNotInterleaved() throws Throwable {
        assertThat(logProvider).doesNotHaveAnyLogs();

        Race race = new Race().withRandomStartDelays();
        race.addContestant(() -> diagnosticsManager.dumpDatabaseDiagnostics(defaultDatabase));
        race.addContestant(() -> logProvider.getLog("test").info("Testlog message"));
        race.go();

        // Assert that diagnostics messages from one database is not interleaved with other log messages.
        assertThat(logProvider.serialize())
                .satisfiesAnyOf(
                        string -> assertThat(string)
                                .containsSubsequence(
                                        "Database: ", "Version", "Store files", "Transaction log", "Testlog message"),
                        string -> assertThat(string)
                                .containsSubsequence(
                                        "Testlog message", "Database: ", "Version", "Store files", "Transaction log"));
    }

    @Test
    void dumpDatabaseDiagnosticInSegments() throws Exception {
        try (JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler()) {
            var dependencies = dependenciesOf(
                    Config.defaults(GraphDatabaseInternalSettings.split_diagnostics, true),
                    databaseContextProvider,
                    jobScheduler);
            var diagnosticsManager = new DbmsDiagnosticsManager(dependencies, new SimpleLogService(logProvider));
            diagnosticsManager.dumpDatabaseDiagnostics(defaultDatabase);

            assertThat(logProvider)
                    .containsMessagesEventually(
                            MINUTES.toMillis(1), "Database: ", "Version", "Store files", "Transaction log");
            assertThat(logProvider)
                    .messageCount("[ Database:", "[ Store files ]", "[ Transaction log ]")
                    .isEqualTo(3);
        }
    }

    @Test
    void dumpDatabaseDiagnosticInSegmentsNotInterleaved() throws Throwable {
        try (JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler()) {
            var dependencies = dependenciesOf(
                    Config.defaults(GraphDatabaseInternalSettings.split_diagnostics, true),
                    databaseContextProvider,
                    jobScheduler);
            var diagnosticsManager = new DbmsDiagnosticsManager(dependencies, new SimpleLogService(logProvider));

            int numDbs = 5;
            List<NamedDatabaseId> dbs = new ArrayList<>();
            Race race = new Race();
            for (int i = 0; i < numDbs; i++) {
                NamedDatabaseId dbId = from("database" + i, UUID.randomUUID());
                Database database = prepareDatabase(dbId);
                dbs.add(dbId);
                race.addContestant(() -> diagnosticsManager.dumpDatabaseDiagnostics(database));
            }
            race.go();

            String[] messages = dbs.stream().map(NamedDatabaseId::logPrefix).toArray(String[]::new);
            assertThat(logProvider).containsMessagesEventually(MINUTES.toMillis(1), messages);

            // Assert that segments are not interleaved
            var logCalls = logProvider.getLogCalls().stream()
                    .map(AssertableLogProvider.LogCall::toString)
                    .map(s -> s.substring(s.indexOf('[') + 1, s.indexOf(']')))
                    .toList();
            List<String> order = new ArrayList<>();
            String prev = null;
            for (String logCall : logCalls) {
                if (!logCall.equals(prev)) {
                    prev = logCall;
                    order.add(logCall);
                }
            }
            assertThat(order).hasSize(numDbs);
        }
    }

    @Test
    void dumpDatabaseDiagnosticsContainsDbName() {
        assertThat(logProvider).doesNotHaveAnyLogs();

        diagnosticsManager.dumpDatabaseDiagnostics(defaultDatabase);

        // Assert that database diagnostics contain the database name on each line
        assertThat(logProvider)
                .eachMessageContains(defaultDatabase.getNamedDatabaseId().logPrefix());
    }

    @Test
    void dumpDiagnosticsEvenOnFailure() {
        DiagnosticsProvider diagnosticsProvider = new DiagnosticsProvider() {
            @Override
            public String getDiagnosticsName() {
                return "foo";
            }

            @Override
            public void dump(DiagnosticsLogger logger) {
                throw new RuntimeException("error during dump");
            }
        };

        dependencies.satisfyDependency(diagnosticsProvider);

        diagnosticsManager.dumpAll();
        assertThat(logProvider.serialize())
                .containsSubsequence(
                        "Failure while logging diagnostics",
                        "error during dump",
                        "System diagnostics",
                        "foo",
                        "Database: ");
    }

    @Test
    void dumpDiagnosticOfStoppedDatabase() {
        when(defaultDatabase.isStarted()).thenReturn(false);
        assertThat(logProvider).doesNotHaveAnyLogs();

        diagnosticsManager.dumpAll();

        assertThat(logProvider)
                .containsMessages("Database: " + DEFAULT_DATABASE_NAME.toLowerCase(), "Database is stopped.");
    }

    @Test
    void dumpDiagnosticsInConciseForm() {
        Map<NamedDatabaseId, StandaloneDatabaseContext> databaseMap = new HashMap<>();
        int numberOfDatabases = 1000;
        for (int i = 0; i < numberOfDatabases; i++) {
            Database database = mock(Database.class);
            NamedDatabaseId namedDatabaseId = from("database" + i, UUID.randomUUID());
            when(database.getNamedDatabaseId()).thenReturn(namedDatabaseId);
            databaseMap.put(namedDatabaseId, new StandaloneDatabaseContext(database));
        }
        when(databaseContextProvider.registeredDatabases()).thenReturn(new TreeMap<>(databaseMap));

        diagnosticsManager.dumpAll();
        var logAssertions = assertThat(logProvider);
        var databaseNames =
                databaseMap.keySet().stream().map(NamedDatabaseId::name).toArray(String[]::new);
        logAssertions.containsMessagesOnce(databaseNames);
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void dumpNativeAccessProviderOnLinux() {
        diagnosticsManager.dumpAll();
        assertThat(logProvider).containsMessages("Linux native access is available.");
    }

    @Test
    @DisabledOnOs(OS.LINUX)
    void dumpNativeAccessProviderOnNonLinux() {
        diagnosticsManager.dumpAll();
        assertThat(logProvider).containsMessages("Native access is not available for current platform.");
    }

    @Test
    void dumpAllDiagnostics() {
        assertThat(logProvider).doesNotHaveAnyLogs();

        diagnosticsManager.dumpAll();

        assertContainsSystemDiagnostics();
        assertContainsDatabaseDiagnostics();
    }

    @Test
    void dumpAdditionalDiagnosticsIfPresent() {
        diagnosticsManager.dumpAll();

        assertNoAdditionalDiagnostics();

        DiagnosticsProvider diagnosticsProvider = new DiagnosticsProvider() {
            @Override
            public String getDiagnosticsName() {
                return "foo";
            }

            @Override
            public void dump(DiagnosticsLogger logger) {}
        };
        dependencies.satisfyDependency(diagnosticsProvider);

        logProvider.clear();
        diagnosticsManager.dumpAll();
        assertContainingAdditionalDiagnostics(diagnosticsProvider);
    }

    private void assertContainsSystemDiagnostics() {
        assertThat(logProvider)
                .containsMessages(
                        "System diagnostics",
                        "System memory information",
                        "JVM memory information",
                        "(IANA) TimeZone database version",
                        "Operating system information",
                        "System properties",
                        "JVM information",
                        "Java classpath",
                        "Library path",
                        "Network information",
                        "DBMS config",
                        "Packaging");
    }

    private void assertContainingAdditionalDiagnostics(DiagnosticsProvider diagnosticsProvider) {
        assertThat(logProvider).containsMessages(diagnosticsProvider.getDiagnosticsName());
    }

    private void assertNoAdditionalDiagnostics() {
        assertThat(logProvider).doesNotContainMessage("Additional diagnostics");
    }

    private void assertContainsDatabaseDiagnostics() {
        assertThat(logProvider)
                .containsMessages(
                        "Database: " + DEFAULT_DATABASE_NAME.toLowerCase(),
                        "Version",
                        "Store files",
                        "Transaction log");
    }

    private Database prepareDatabase() throws IOException {
        return prepareDatabase(DEFAULT_DATABASE_ID);
    }

    private Database prepareDatabase(NamedDatabaseId databaseId) throws IOException {
        Database database = mock(Database.class);

        Dependencies databaseDependencies = new Dependencies();
        databaseDependencies.satisfyDependency(DbmsInfo.COMMUNITY);
        databaseDependencies.satisfyDependency(storageEngine);
        databaseDependencies.satisfyDependency(storageEngineFactory);
        databaseDependencies.satisfyDependency(new DefaultFileSystemAbstraction());
        databaseDependencies.satisfyDependency(DeviceMapper.UNKNOWN_MAPPER);
        LogFiles logFiles = databaseDependencies.satisfyDependency(
                logFilesBasedOnlyBuilder(directory.homePath(), directory.getFileSystem())
                        .build());
        LogTailMetadata logTailMetadata = databaseDependencies.satisfyDependency(logFiles.getTailMetadata());
        TransactionIdStore txIdStore = databaseDependencies.satisfyDependency(mock(TransactionIdStore.class));
        when(txIdStore.getLastClosedTransactionId())
                .thenReturn(logTailMetadata.getLastCommittedTransaction().id());
        when(database.getDependencyResolver()).thenReturn(databaseDependencies);
        when(database.getNamedDatabaseId()).thenReturn(databaseId);
        when(database.isStarted()).thenReturn(true);
        when(database.getDatabaseLayout()).thenReturn(DatabaseLayout.ofFlat(directory.homePath()));
        when(database.getStoreId()).thenReturn(StoreId.generateNew("engine_1", "format_1", 1, 1));
        return database;
    }
}
