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
package org.neo4j.kernel.impl.storemigration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DatabaseRecordFormat.standard;
import static org.neo4j.io.pagecache.context.EmptyVersionContextSupplier.EMPTY;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.function.Suppliers;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

@PageCacheExtension
@Neo4jLayoutExtension
class StoreMigratorTest {
    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    private final IndexProviderMap indexProviderMap = mock(IndexProviderMap.class);
    private final JobScheduler jobScheduler = new ThreadPoolJobScheduler();

    private RecordDatabaseLayout databaseLayout;

    @BeforeEach
    void setUp() {
        databaseLayout = RecordDatabaseLayout.of(neo4jLayout, DEFAULT_DATABASE_NAME);
        var dbms = new TestDatabaseManagementServiceBuilder(neo4jLayout.homeDirectory())
                .setConfig(GraphDatabaseSettings.record_format_created_db, standard)
                .build();
        dbms.shutdown();
    }

    @AfterEach
    void tearDown() throws Exception {
        jobScheduler.close();
    }

    @Test
    void shouldForbidRegistrationOfParticipantsWithSameName() throws IOException {
        var participant = mock(StoreMigrationParticipant.class);
        when(participant.getName()).thenReturn(RecordStorageMigrator.NAME);

        mockParticipantAddition(participant);
        var storeMigrator = createMigrator();

        var exception =
                assertThrows(IllegalStateException.class, () -> storeMigrator.migrateIfNeeded(PageAligned.LATEST_NAME));
        assertThat(exception)
                .hasMessage(
                        "Migration participants should have unique names. Participant with name: 'Store files' is already registered.");
    }

    @Test
    void shouldContinueMovingFilesIfInterruptedDuringMovingPhase() throws Exception {
        var storeMigrator = createMigrator();
        // a participant that will fail during the moving phase
        var failingParticipant = mock(StoreMigrationParticipant.class);
        when(failingParticipant.getName()).thenReturn("Failing");
        doThrow(new IOException("Just failing")).when(failingParticipant).moveMigratedFiles(any(), any(), any(), any());
        mockParticipantAddition(failingParticipant);

        var exception = assertThrows(
                UnableToMigrateException.class, () -> storeMigrator.migrateIfNeeded(PageAligned.LATEST_NAME));
        assertThat(exception)
                .hasMessage(
                        "A critical failure during migration has occurred. Failed to move migrated files into place")
                .hasRootCauseExactlyInstanceOf(IOException.class)
                .hasRootCauseMessage("Just failing");

        assertTrue(migrationDirPresent());

        StoreMigrationParticipant observingParticipant = Mockito.mock(StoreMigrationParticipant.class);
        mockParticipantAddition(observingParticipant);

        storeMigrator.migrateIfNeeded(PageAligned.LATEST_NAME);

        // migrate not called ...
        verify(observingParticipant, never()).migrate(any(), any(), any(), any(), any(), any(), any());
        // ... but move and clean up yes
        verify(observingParticipant).moveMigratedFiles(any(), any(), any(), any());
        verify(observingParticipant).cleanup(any(DatabaseLayout.class));

        verifyDbStartAndFormat(PageAligned.LATEST_RECORD_FORMATS.storeVersion());

        assertFalse(migrationDirPresent());
    }

    @Test
    void shouldRedoTheEntireMigrationIfInterruptedDuringMigrationPhase() throws Exception {
        var storeMigrator = createMigrator();
        // a participant that will fail during the migration phase
        var failingParticipant = mock(StoreMigrationParticipant.class);
        when(failingParticipant.getName()).thenReturn("Failing");
        doThrow(new IOException("Just failing"))
                .when(failingParticipant)
                .migrate(any(), any(), any(), any(), any(), any(), any());
        mockParticipantAddition(failingParticipant);

        var exception = assertThrows(
                UnableToMigrateException.class, () -> storeMigrator.migrateIfNeeded(PageAligned.LATEST_NAME));
        assertThat(exception)
                .hasMessage("A critical failure during migration has occurred")
                .hasRootCauseExactlyInstanceOf(IOException.class)
                .hasRootCauseMessage("Just failing");

        StoreMigrationParticipant observingParticipant = Mockito.mock(StoreMigrationParticipant.class);
        mockParticipantAddition(observingParticipant);

        storeMigrator.migrateIfNeeded(PageAligned.LATEST_NAME);

        verify(observingParticipant).migrate(any(), any(), any(), any(), any(), any(), any());
        verify(observingParticipant).moveMigratedFiles(any(), any(), any(), any());
        verify(observingParticipant).cleanup(any(DatabaseLayout.class));

        verifyDbStartAndFormat(PageAligned.LATEST_RECORD_FORMATS.storeVersion());

        assertFalse(migrationDirPresent());
    }

    @Test
    void shouldHandleNewMigrationIfInterruptedDuringMigrationPhase() throws Exception {
        var storeMigrator = createMigratorWithDevFormats();
        // a participant that will fail during the migration phase
        var failingParticipant = mock(StoreMigrationParticipant.class);
        when(failingParticipant.getName()).thenReturn("Failing");
        doThrow(new IOException("Just failing"))
                .when(failingParticipant)
                .migrate(any(), any(), any(), any(), any(), any(), any());
        mockParticipantAddition(failingParticipant);

        var exception =
                assertThrows(UnableToMigrateException.class, () -> storeMigrator.migrateIfNeeded(Standard.LATEST_NAME));
        assertThat(exception)
                .hasMessage("A critical failure during migration has occurred")
                .hasRootCauseExactlyInstanceOf(IOException.class)
                .hasRootCauseMessage("Just failing");

        StoreMigrationParticipant observingParticipant = Mockito.mock(StoreMigrationParticipant.class);
        mockParticipantAddition(observingParticipant);

        storeMigrator.migrateIfNeeded(PageAligned.LATEST_NAME);

        // The newly requested migration should be done, the old one should just have been ignored
        verify(observingParticipant)
                .migrate(
                        any(),
                        any(),
                        any(),
                        argThat(new VersionMatcher(Standard.LATEST_STORE_VERSION)),
                        argThat(new VersionMatcher(PageAlignedTestFormat.WithMajorVersionBump.VERSION_STRING)),
                        any(),
                        any());
        verify(observingParticipant).moveMigratedFiles(any(), any(), any(), any());
        verify(observingParticipant).cleanup(any(DatabaseLayout.class));

        verifyDbStartAndFormat(PageAlignedTestFormat.WithMajorVersionBump.VERSION_STRING);

        assertFalse(migrationDirPresent());
    }

    @Test
    void shouldHandleNewMigrationIfInterruptedDuringMovingPhase() throws Exception {
        var storeMigrator = createMigratorWithDevFormats();
        // a participant that will fail during the migration phase
        var failingParticipant = mock(StoreMigrationParticipant.class);
        when(failingParticipant.getName()).thenReturn("Failing");
        doThrow(new IOException("Just failing")).when(failingParticipant).moveMigratedFiles(any(), any(), any(), any());
        mockParticipantAddition(failingParticipant);

        var exception =
                assertThrows(UnableToMigrateException.class, () -> storeMigrator.migrateIfNeeded(Standard.LATEST_NAME));
        assertThat(exception)
                .hasMessage(
                        "A critical failure during migration has occurred. Failed to move migrated files into place")
                .hasRootCauseExactlyInstanceOf(IOException.class)
                .hasRootCauseMessage("Just failing");

        assertTrue(migrationDirPresent());

        StoreMigrationParticipant observingParticipant = Mockito.mock(StoreMigrationParticipant.class);
        mockParticipantAddition(observingParticipant);

        storeMigrator.migrateIfNeeded(PageAligned.LATEST_NAME);

        // Moving of the ongoing migration should be done and then the actual migration
        verify(observingParticipant)
                .moveMigratedFiles(
                        any(),
                        any(),
                        eq(Standard.LATEST_STORE_VERSION),
                        eq(StandardFormatWithMinorVersionBump.VERSION_STRING));
        verify(observingParticipant)
                .migrate(
                        any(),
                        any(),
                        any(),
                        argThat(new VersionMatcher(StandardFormatWithMinorVersionBump.VERSION_STRING)),
                        argThat(new VersionMatcher(PageAlignedTestFormat.WithMajorVersionBump.VERSION_STRING)),
                        any(),
                        any());
        verify(observingParticipant)
                .moveMigratedFiles(
                        any(),
                        any(),
                        eq(StandardFormatWithMinorVersionBump.VERSION_STRING),
                        eq(PageAlignedTestFormat.WithMajorVersionBump.VERSION_STRING));
        verify(observingParticipant, times(2)).cleanup(any(DatabaseLayout.class));

        verifyDbStartAndFormat(PageAlignedTestFormat.WithMajorVersionBump.VERSION_STRING);

        assertFalse(migrationDirPresent());
    }

    @Test
    void shouldHandleNewUpgradeIfInterruptedDuringMigrationPhase() throws Exception {
        var storeMigrator = createMigratorWithDevFormats();
        // a participant that will fail during the migration phase
        var failingParticipant = mock(StoreMigrationParticipant.class);
        when(failingParticipant.getName()).thenReturn("Failing");
        doThrow(new IOException("Just failing"))
                .when(failingParticipant)
                .migrate(any(), any(), any(), any(), any(), any(), any());
        mockParticipantAddition(failingParticipant);

        var exception = assertThrows(
                UnableToMigrateException.class, () -> storeMigrator.migrateIfNeeded(PageAligned.LATEST_NAME));
        assertThat(exception)
                .hasMessage("A critical failure during migration has occurred")
                .hasRootCauseExactlyInstanceOf(IOException.class)
                .hasRootCauseMessage("Just failing");

        StoreMigrationParticipant observingParticipant = Mockito.mock(StoreMigrationParticipant.class);
        mockParticipantAddition(observingParticipant);

        // Try to upgrade to latest standard (dev format)
        storeMigrator.upgradeIfNeeded();

        // The newly requested upgrade should be done, the old migration should just have been ignored
        verify(observingParticipant)
                .migrate(
                        any(),
                        any(),
                        any(),
                        argThat(new VersionMatcher(Standard.LATEST_STORE_VERSION)),
                        argThat(new VersionMatcher(StandardFormatWithMinorVersionBump.VERSION_STRING)),
                        any(),
                        any());
        verify(observingParticipant).moveMigratedFiles(any(), any(), any(), any());
        verify(observingParticipant).cleanup(any(DatabaseLayout.class));

        verifyDbStartAndFormat(StandardFormatWithMinorVersionBump.VERSION_STRING);

        assertFalse(migrationDirPresent());
    }

    @Test
    void shouldAbortNewUpgradeIfOtherInterruptedDuringMovingPhase() throws Exception {
        var storeMigrator = createMigrator();
        // a participant that will fail during the migration phase
        var failingParticipant = mock(StoreMigrationParticipant.class);
        when(failingParticipant.getName()).thenReturn("Failing");
        doThrow(new IOException("Just failing")).when(failingParticipant).moveMigratedFiles(any(), any(), any(), any());
        mockParticipantAddition(failingParticipant);

        var exception = assertThrows(
                UnableToMigrateException.class, () -> storeMigrator.migrateIfNeeded(PageAligned.LATEST_NAME));
        assertThat(exception)
                .hasMessage(
                        "A critical failure during migration has occurred. Failed to move migrated files into place")
                .hasRootCauseExactlyInstanceOf(IOException.class)
                .hasRootCauseMessage("Just failing");

        assertTrue(migrationDirPresent());

        var newStoreMigrator = createMigratorWithDevFormats();
        StoreMigrationParticipant observingParticipant = Mockito.mock(StoreMigrationParticipant.class);
        mockParticipantAddition(observingParticipant);

        // There was a started migration that failed in moving, and it was a migration to a different version than what
        // we are trying to upgrade to - fail
        assertThatThrownBy(newStoreMigrator::upgradeIfNeeded)
                .isInstanceOf(UnableToMigrateException.class)
                .hasMessageContaining("A partially complete migration to "
                        + PageAligned.LATEST_RECORD_FORMATS.storeVersion() + " found when trying to migrate to "
                        + PageAlignedTestFormat.WithMinorVersionBump.VERSION_STRING);
    }

    public static class VersionMatcher implements ArgumentMatcher<StoreVersion> {
        private final String expected;

        VersionMatcher(String expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(StoreVersion right) {
            return expected.equals(right.storeVersion());
        }
    }

    @Test
    void shouldGiveProgressMonitorProgressMessages() throws Exception {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        LogService logService = mock(LogService.class);
        when(logService.getInternalLogProvider()).thenReturn(logProvider);
        when(logService.getInternalLog(any())).thenReturn(logProvider.getLog("something"));

        var storeMigrator = createMigrator(logService);
        storeMigrator.migrateIfNeeded(PageAligned.LATEST_NAME);

        assertThat(logProvider)
                .containsMessages(
                        // a couple of randomly selected expected log entries:
                        "Migrating Store files",
                        "10% completed",
                        "40% completed",
                        "70% completed",
                        "Successfully finished migration of database");
    }

    private boolean migrationDirPresent() {
        var path = databaseLayout.file(StoreMigrator.MIGRATION_DIRECTORY);
        return Files.exists(path);
    }

    private void verifyDbStartAndFormat(String expectedStoreVersion) {
        var dbms = new TestDatabaseManagementServiceBuilder(neo4jLayout.homeDirectory()).build();
        try {
            var db = dbms.database(DEFAULT_DATABASE_NAME);
            // let's check the DB is operational
            db.beginTx().close();

            var dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
            var storageEngineFactory = dependencyResolver.resolveDependency(StorageEngineFactory.class);
            var cursorContextFactory = dependencyResolver.resolveDependency(CursorContextFactory.class);
            var storeVersionCheck = storageEngineFactory.versionCheck(
                    dependencyResolver.resolveDependency(FileSystemAbstraction.class),
                    dependencyResolver.resolveDependency(DatabaseLayout.class),
                    dependencyResolver.resolveDependency(Config.class),
                    dependencyResolver.resolveDependency(PageCache.class),
                    dependencyResolver.resolveDependency(LogService.class),
                    cursorContextFactory);
            try (var cursorContext = cursorContextFactory.create("Test")) {
                var storeVersion = storeVersionCheck.storeVersion(cursorContext).orElseThrow();
                assertEquals(expectedStoreVersion, storeVersion);
            }
        } finally {
            dbms.shutdown();
        }
    }

    private void mockParticipantAddition(StoreMigrationParticipant participant) {
        reset(indexProviderMap);
        IndexProvider indexProvider = mock(IndexProvider.class);
        doAnswer(invocation -> {
                    Object[] args = invocation.getArguments();
                    Consumer<IndexProvider> consumer = (Consumer<IndexProvider>) args[0];
                    consumer.accept(indexProvider);
                    return null;
                })
                .when(indexProviderMap)
                .accept(any());
        when(indexProvider.storeMigrationParticipant(any(), any(), any(), any()))
                .thenReturn(participant);
    }

    private StoreMigrator createMigrator() throws IOException {
        return createMigrator(NullLogService.getInstance());
    }

    private StoreMigrator createMigrator(LogService logService) throws IOException {
        return createMigrator(logService, Config.defaults());
    }

    private StoreMigrator createMigratorWithDevFormats() throws IOException {
        return createMigrator(
                NullLogService.getInstance(),
                Config.defaults(GraphDatabaseInternalSettings.include_versions_under_development, true));
    }

    private StoreMigrator createMigrator(LogService logService, Config config) throws IOException {
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.defaultStorageEngine();
        var contextFactory = new CursorContextFactory(PageCacheTracer.NULL, EMPTY);

        var logTail = new LogTailExtractor(fs, pageCache, config, storageEngineFactory, DatabaseTracers.EMPTY)
                .getTailMetadata(databaseLayout, INSTANCE);
        var supplier = Suppliers.lazySingleton(() -> logTail);

        return new StoreMigrator(
                fs,
                config,
                logService,
                pageCache,
                DatabaseTracers.EMPTY,
                jobScheduler,
                databaseLayout,
                storageEngineFactory,
                indexProviderMap,
                contextFactory,
                INSTANCE,
                supplier);
    }
}
