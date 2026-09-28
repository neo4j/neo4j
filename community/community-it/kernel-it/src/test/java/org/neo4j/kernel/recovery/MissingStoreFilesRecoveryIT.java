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

import static java.lang.String.valueOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.RelationshipType.withName;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageFileSelection;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.WALService;
import org.neo4j.wal.checkpoint.CheckPointer;
import org.neo4j.wal.checkpoint.SimpleTriggerInfo;

@TestDirectoryExtension
class MissingStoreFilesRecoveryIT {
    private static final Label testNodes = Label.label("testNodes");

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    private DatabaseManagementService managementService;
    private DatabaseLayout databaseLayout;
    private TestDatabaseManagementServiceBuilder serviceBuilder;
    private NamedDatabaseId defaultNamedDatabaseId;
    private Collection<Path> storeFiles;

    @BeforeEach
    void setUp() throws IOException {
        serviceBuilder = new TestDatabaseManagementServiceBuilder(testDirectory.homePath());
        managementService = serviceBuilder.build();
        var databaseApi = defaultDatabase(managementService);
        createSomeData(databaseApi);
        databaseLayout = databaseApi.databaseLayout();

        defaultNamedDatabaseId = getDatabaseContextProvider()
                .databaseIdRepository()
                .getByName(DEFAULT_DATABASE_NAME)
                .orElseThrow();
        storeFiles = databaseApi
                .getDependencyResolver()
                .resolveDependency(StorageEngine.class)
                .listStorageFiles(new StorageFileSelection(true, true, false));

        managementService.shutdown();
    }

    @AfterEach
    void tearDown() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @SkipOnSpd(
            reason =
                    "Cluster simply cannot handle starting a db w/o tx logs - see ClusterIllegalSeedingIT#shouldFailToStartReadReplicaWithNoLogsButStandaloneShouldPass",
            notes = {SkipOnSpd.Note.incompatible})
    @Test
    void databaseStartFailingOnMissingFilesAndMissedTxLogs() throws IOException {
        Path[] storeFiles = getStoreFiles(databaseLayout);
        for (Path storeFile : storeFiles) {
            fileSystem.deleteFile(storeFile);
        }
        fileSystem.deleteRecursively(databaseLayout.getTransactionLogsDirectory());

        managementService = serviceBuilder.build();
        var dbStateService = getDatabaseStateService();
        assertThat(dbStateService.causeOfFailure(defaultNamedDatabaseId).orElseThrow())
                .rootCause()
                .hasMessageContaining(
                        "are missing and recovery is not possible. Please restore from a consistent backup.");
    }

    @Test
    void failToStartOnMissingFilesAndPartialTransactionLogs() throws IOException {
        WALService walService = prepareDatabaseWithTwoTxLogFiles();

        fileSystem.deleteFile(walService.getLogFile().getLogFileForVersion(0));
        Path[] storeFiles = getStoreFiles(databaseLayout);
        for (Path storeFile : storeFiles) {
            fileSystem.deleteFile(storeFile);
        }

        var dbStateService = getDatabaseStateService();
        var failure = dbStateService.causeOfFailure(defaultNamedDatabaseId);
        assertFalse(failure.isPresent());
        for (Path storeFile : storeFiles) {
            assertFalse(fileSystem.fileExists(storeFile));
        }
    }

    private Path[] getStoreFiles(DatabaseLayout layout) {
        return storeFiles.stream()
                .filter(Predicate.not(layout.pathForExistsMarker()::equals))
                .limit(5)
                .toArray(Path[]::new);
    }

    private WALService prepareDatabaseWithTwoTxLogFiles() throws IOException {
        managementService = serviceBuilder.build();
        var databaseApi = defaultDatabase(managementService);
        WALService walService = rotateTransactionLogs(databaseApi);
        assertNotNull(walService.getLogFile().getLogFileForVersion(1));
        createSomeData(databaseApi);
        managementService.shutdown();
        return walService;
    }

    private DatabaseContextProvider<?> getDatabaseContextProvider() {
        return defaultDatabase(managementService)
                .getDependencyResolver()
                .resolveDependency(DatabaseContextProvider.class);
    }

    private DatabaseStateService<?> getDatabaseStateService() {
        return defaultDatabase(managementService).getDependencyResolver().resolveDependency(DatabaseStateService.class);
    }

    private static GraphDatabaseAPI defaultDatabase(DatabaseManagementService managementService) {
        return (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
    }

    private static WALService rotateTransactionLogs(GraphDatabaseAPI databaseApi) throws IOException {
        WALService walService = databaseApi.getDependencyResolver().resolveDependency(WALService.class);
        LogFile logFile = walService.getLogFile();
        logFile.rotate();
        return walService;
    }

    private static void createSomeData(GraphDatabaseAPI databaseApi) throws IOException {
        insertData(databaseApi);
        CheckPointer checkPointer = databaseApi.getDependencyResolver().resolveDependency(CheckPointer.class);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("forcedCheckpointInTheMiddle"));
        insertData(databaseApi);
    }

    private static void insertData(GraphDatabaseAPI databaseApi) {
        for (int i = 0; i < 100; i++) {
            try (Transaction transaction = databaseApi.beginTx()) {
                Node nodeA = transaction.createNode(testNodes);
                Node nodeB = transaction.createNode(testNodes);
                nodeA.createRelationshipTo(nodeB, withName(valueOf(i)));
                transaction.commit();
            }
        }
    }
}
