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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class RecoveryRequiredCheckerTest {
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private RandomSupport random;

    private DatabaseLayout databaseLayout;

    private Path storeDir;

    private StorageEngineFactory storageEngineFactory;

    @BeforeEach
    void setup() {
        storeDir = testDirectory.homePath();
    }

    @Test
    void shouldNotWantToRecoverIntactStore() throws Exception {
        startStopAndCreateDefaultData();

        try (PageCache pageCache = pageCacheExtension.getPageCache(fileSystem)) {
            RecoveryRequiredChecker recoverer =
                    getRecoveryCheckerWithDefaultConfig(fileSystem, pageCache, storageEngineFactory);
            assertThat(recoverer.isRecoveryRequiredAt(databaseLayout, INSTANCE)).isEqualTo(false);
        }
    }

    @Test
    void shouldWantToRecoverBrokenStore() throws Exception {
        try (EphemeralFileSystemAbstraction ephemeralFs = createAndCrashWithDefaultConfig();
                PageCache pageCache = pageCacheExtension.getPageCache(ephemeralFs)) {
            RecoveryRequiredChecker recoverer =
                    getRecoveryCheckerWithDefaultConfig(ephemeralFs, pageCache, storageEngineFactory);

            assertThat(recoverer.isRecoveryRequiredAt(databaseLayout, INSTANCE)).isEqualTo(true);
        }
    }

    @Test
    void shouldBeAbleToRecoverBrokenStore() throws Exception {
        try (EphemeralFileSystemAbstraction ephemeralFs = createAndCrashWithDefaultConfig();
                PageCache pageCache = pageCacheExtension.getPageCache(ephemeralFs)) {
            RecoveryRequiredChecker recoverer =
                    getRecoveryCheckerWithDefaultConfig(ephemeralFs, pageCache, storageEngineFactory);

            assertThat(recoverer.isRecoveryRequiredAt(databaseLayout, INSTANCE)).isEqualTo(true);

            startStopDatabase(ephemeralFs, storeDir);

            assertThat(recoverer.isRecoveryRequiredAt(databaseLayout, INSTANCE)).isEqualTo(false);
        }
    }

    @Test
    void shouldBeAbleToRecoverBrokenStoreWithLogsInSeparateAbsoluteLocation() throws Exception {
        Path customTransactionLogsLocation = testDirectory.directory(DEFAULT_TX_LOGS_ROOT_DIR_NAME);
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, testDirectory.homePath())
                .set(transaction_logs_root_path, customTransactionLogsLocation.toAbsolutePath())
                .build();
        recoverBrokenStoreWithConfig(config);
    }

    @Test
    void shouldNotWantToRecoverEmptyStore() throws Exception {
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(testDirectory.directory("dir-without-store"));

        try (PageCache pageCache = pageCacheExtension.getPageCache(fileSystem)) {
            RecoveryRequiredChecker checker = getRecoveryCheckerWithDefaultConfig(
                    fileSystem,
                    pageCache,
                    StorageEngineFactory.selectStorageEngine(fileSystem, databaseLayout, Config.defaults()));
            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));
        }
    }

    @Test
    void shouldWantToRecoverStoreWithoutOneIdFile() throws Exception {
        startStopAndCreateDefaultData();
        assertAllIdFilesExist();

        try (PageCache pageCache = pageCacheExtension.getPageCache(fileSystem)) {
            RecoveryRequiredChecker checker;
            checker = getRecoveryCheckerWithDefaultConfig(fileSystem, pageCache, storageEngineFactory);
            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));

            fileSystem.deleteFileOrThrow(Iterables.first(databaseLayout.idFiles()));

            assertTrue(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));
        }
    }

    @Test
    void shouldWantToRecoverStoreWithoutAllIdFiles() throws Exception {
        startStopAndCreateDefaultData();
        assertAllIdFilesExist();

        try (PageCache pageCache = pageCacheExtension.getPageCache(fileSystem)) {
            RecoveryRequiredChecker checker =
                    getRecoveryCheckerWithDefaultConfig(fileSystem, pageCache, storageEngineFactory);
            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));

            for (Path idFile : databaseLayout.idFiles()) {
                fileSystem.deleteFileOrThrow(idFile);
            }

            assertTrue(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));
        }
    }

    @Test
    void doNotRequireCheckpointWhenOldestNotCompletedPositionIsEqualToCheckpointedPosition() throws IOException {
        var managementService = new TestDatabaseManagementServiceBuilder(testDirectory.directory("test")).build();
        try {
            var db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);

            databaseLayout = db.databaseLayout();
            var dependencyResolver = db.getDependencyResolver();
            storageEngineFactory = dependencyResolver.resolveDependency(StorageEngineFactory.class);
            var logFiles = dependencyResolver.resolveDependency(LogFiles.class);
            var checkPointer = dependencyResolver.resolveDependency(CheckPointer.class);

            try (Transaction tx = db.beginTx()) {
                tx.createNode();
                tx.commit();
            }

            checkPointer.forceCheckPoint(new SimpleTriggerInfo("Test"));

            CheckpointInfo latestCheckpoint =
                    logFiles.getCheckpointFile().findLatestCheckpoint().orElseThrow();
            assertEquals(
                    latestCheckpoint.transactionLogPosition(),
                    latestCheckpoint.oldestNotVisibleTransactionLogPosition());
        } finally {
            managementService.shutdown();
        }

        try (PageCache pageCache = pageCacheExtension.getPageCache(fileSystem)) {
            RecoveryRequiredChecker checker =
                    getRecoveryCheckerWithDefaultConfig(fileSystem, pageCache, storageEngineFactory);
            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));
        }
    }

    @Test
    void shouldNotWantToRecoveryWhenStoreExistenceFileIsMissing() throws Exception {
        startStopAndCreateDefaultData();

        assertStoreFilesExist();

        try (PageCache pageCache = pageCacheExtension.getPageCache(fileSystem)) {
            RecoveryRequiredChecker checker =
                    getRecoveryCheckerWithDefaultConfig(fileSystem, pageCache, storageEngineFactory);
            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));

            fileSystem.deleteFileOrThrow(databaseLayout.pathForExistsMarker());

            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));
        }
    }

    @Test
    void recoveryRequiredWhenAnyMandatoryStoreFileIsMissing() throws Exception {
        startStopAndCreateDefaultData();

        assertStoreFilesExist();

        try (PageCache pageCache = pageCacheExtension.getPageCache(fileSystem)) {
            RecoveryRequiredChecker checker =
                    getRecoveryCheckerWithDefaultConfig(fileSystem, pageCache, storageEngineFactory);
            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));

            final var path = random.among(databaseLayout.mandatoryStoreFiles().stream()
                    .filter(Predicate.not(databaseLayout.pathForExistsMarker()::equals))
                    .toList());
            fileSystem.deleteFileOrThrow(path);

            assertTrue(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));
        }
    }

    @Test
    void recoveryRequiredWhenSeveralStoreFileAreMissing() throws Exception {
        startStopAndCreateDefaultData();

        assertStoreFilesExist();

        try (PageCache pageCache = pageCacheExtension.getPageCache(fileSystem)) {
            RecoveryRequiredChecker checker =
                    getRecoveryCheckerWithDefaultConfig(fileSystem, pageCache, storageEngineFactory);
            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));

            fileSystem.deleteFileOrThrow(databaseLayout.pathForStore(CommonDatabaseStores.COUNTS));
            fileSystem.deleteFileOrThrow(databaseLayout.pathForStore(CommonDatabaseStores.SCHEMAS));
            fileSystem.deleteFileOrThrow(databaseLayout.pathForStore(CommonDatabaseStores.RELATIONSHIP_TYPE_TOKENS));

            assertTrue(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));
        }
    }

    @Test
    void recoveryNotRequiredWhenCountStoreIsMissing() throws Exception {
        startStopAndCreateDefaultData();

        assertStoreFilesExist();

        try (PageCache pageCache = pageCacheExtension.getPageCache(fileSystem)) {
            RecoveryRequiredChecker checker =
                    getRecoveryCheckerWithDefaultConfig(fileSystem, pageCache, storageEngineFactory);
            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));

            fileSystem.deleteFileOrThrow(databaseLayout.pathForStore(CommonDatabaseStores.COUNTS));

            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));
        }
    }

    @Test
    void recoveryNotRequiredWhenIndexStatisticStoreIsMissing() throws Exception {
        startStopAndCreateDefaultData();

        assertStoreFilesExist();

        try (PageCache pageCache = pageCacheExtension.getPageCache(fileSystem)) {
            RecoveryRequiredChecker checker =
                    getRecoveryCheckerWithDefaultConfig(fileSystem, pageCache, storageEngineFactory);
            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));

            fileSystem.deleteFileOrThrow(databaseLayout.pathForStore(CommonDatabaseStores.INDEX_STATISTICS));

            assertFalse(checker.isRecoveryRequiredAt(databaseLayout, INSTANCE));
        }
    }

    private void recoverBrokenStoreWithConfig(Config config) throws IOException {
        try (EphemeralFileSystemAbstraction ephemeralFs = createSomeDataAndCrash(storeDir, config);
                PageCache pageCache = pageCacheExtension.getPageCache(ephemeralFs)) {
            RecoveryRequiredChecker recoveryChecker =
                    getRecoveryChecker(ephemeralFs, pageCache, storageEngineFactory, config);

            assertThat(recoveryChecker.isRecoveryRequiredAt(DatabaseLayout.of(config), INSTANCE))
                    .isEqualTo(true);

            DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(storeDir)
                    .setFileSystem(ephemeralFs)
                    .setConfig(config)
                    .build();
            managementService.shutdown();

            assertThat(recoveryChecker.isRecoveryRequiredAt(databaseLayout, INSTANCE))
                    .isEqualTo(false);
        }
    }

    private EphemeralFileSystemAbstraction createAndCrashWithDefaultConfig() throws IOException {
        return createSomeDataAndCrash(storeDir, Config.defaults());
    }

    private void assertAllIdFilesExist() {
        for (Path idFile : databaseLayout.idFiles()) {
            assertTrue(fileSystem.fileExists(idFile), "ID file " + idFile + " does not exist");
        }
    }

    private void assertStoreFilesExist() {
        for (Path file : databaseLayout.storeFiles()) {
            assertTrue(fileSystem.fileExists(file), "Store file " + file + " does not exist");
        }
    }

    private static RecoveryRequiredChecker getRecoveryCheckerWithDefaultConfig(
            FileSystemAbstraction fileSystem, PageCache pageCache, StorageEngineFactory storageEngineFactory) {
        return getRecoveryChecker(fileSystem, pageCache, storageEngineFactory, Config.defaults());
    }

    private static RecoveryRequiredChecker getRecoveryChecker(
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            StorageEngineFactory storageEngineFactory,
            Config config) {
        return new RecoveryRequiredChecker(fileSystem, pageCache, config, storageEngineFactory, DatabaseTracers.EMPTY);
    }

    private EphemeralFileSystemAbstraction createSomeDataAndCrash(Path store, Config config) throws IOException {
        try (EphemeralFileSystemAbstraction ephemeralFs = new EphemeralFileSystemAbstraction()) {
            DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(store)
                    .setFileSystem(ephemeralFs)
                    .setConfig(config)
                    .build();
            final GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);

            try (Transaction tx = db.beginTx()) {
                tx.createNode();
                tx.commit();
            }

            databaseLayout = ((GraphDatabaseAPI) db).databaseLayout();
            storageEngineFactory =
                    ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(StorageEngineFactory.class);

            EphemeralFileSystemAbstraction snapshot = ephemeralFs.snapshot();
            managementService.shutdown();
            return snapshot;
        }
    }

    private static DatabaseManagementService startDatabase(FileSystemAbstraction fileSystem, Path storeDir) {
        return new TestDatabaseManagementServiceBuilder(storeDir)
                .setFileSystem(fileSystem)
                .build();
    }

    private static void startStopDatabase(FileSystemAbstraction fileSystem, Path storeDir) {
        DatabaseManagementService managementService = startDatabase(fileSystem, storeDir);
        managementService.shutdown();
    }

    private void startStopAndCreateDefaultData() {
        DatabaseManagementService managementService = startDatabase(fileSystem, storeDir);
        try {
            GraphDatabaseService database = managementService.database(DEFAULT_DATABASE_NAME);
            try (Transaction transaction = database.beginTx()) {
                transaction.createNode();
                transaction.commit();
            }

            databaseLayout = ((GraphDatabaseAPI) database).databaseLayout();
            storageEngineFactory =
                    ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency(StorageEngineFactory.class);
        } finally {
            managementService.shutdown();
        }
    }
}
