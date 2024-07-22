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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.cloud.storage.StorageUtils.WRITE_OPTIONS;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;
import static org.neo4j.kernel.recovery.RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.AppendBatchInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

@PageCacheExtension
@Neo4jLayoutExtension
public class LogTailAppendIndexIT {
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private Neo4jLayout neo4jLayout;

    private DatabaseManagementService dbms;

    @AfterEach
    void tearDown() {
        if (dbms != null) {
            dbms.shutdown();
            dbms = null;
        }
    }

    @Test
    void emptyLogsLastBatch() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        var layout = db.getDependencyResolver().resolveDependency(DatabaseLayout.class);
        dbms.shutdown();

        fileSystem.delete(layout.getTransactionLogsDirectory());

        LogFiles logFiles = buildDefaultLogFiles(layout);
        var lastBatch = logFiles.getTailMetadata().lastBatch();
        assertEquals(BASE_APPEND_INDEX, lastBatch.appendIndex());
        assertEquals(new LogPosition(0, 64), lastBatch.logPositionAfter());
    }

    @Test
    void logLastBatchNoCheckpoints() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        var marker = RelationshipType.withName("marker");
        var dependencyResolver = db.getDependencyResolver();

        for (int i = 0; i < 10; i++) {
            createNodesWithRelationship(db, marker);
        }
        var lastBatchBeforeRestart =
                dependencyResolver.resolveDependency(TransactionIdStore.class).getLastCommittedBatch();

        var layout = dependencyResolver.resolveDependency(DatabaseLayout.class);
        var originalLogFiles = dependencyResolver.resolveDependency(LogFiles.class);
        Path[] checkpointFiles = originalLogFiles.getCheckpointFile().getDetachedCheckpointFiles();
        dbms.shutdown();

        for (Path checkpointFile : checkpointFiles) {
            fileSystem.delete(checkpointFile);
        }

        LogFiles logFiles = buildDefaultLogFiles(layout);
        assertEquals(lastBatchBeforeRestart, logFiles.getTailMetadata().lastBatch());
    }

    @Test
    void logLastBatchNoCheckpointsSeveralFiles() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        var marker = RelationshipType.withName("marker");
        var dependencyResolver = db.getDependencyResolver();
        var originalLogFiles = dependencyResolver.resolveDependency(LogFiles.class);
        for (int j = 0; j < 12; j++) {
            for (int i = 0; i < 10; i++) {
                createNodesWithRelationship(db, marker);
            }
            originalLogFiles.getLogFile().rotate();
        }

        var lastBatchBeforeRestart =
                dependencyResolver.resolveDependency(TransactionIdStore.class).getLastCommittedBatch();
        var layout = dependencyResolver.resolveDependency(DatabaseLayout.class);
        Path[] checkpointFiles = originalLogFiles.getCheckpointFile().getDetachedCheckpointFiles();
        dbms.shutdown();

        for (Path checkpointFile : checkpointFiles) {
            fileSystem.delete(checkpointFile);
        }

        LogFiles logFiles = buildDefaultLogFiles(layout);
        assertEquals(lastBatchBeforeRestart, logFiles.getTailMetadata().lastBatch());
    }

    @Test
    void logLastBatchWithCheckpointInTheEnd() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        var marker = RelationshipType.withName("marker");
        var dependencyResolver = db.getDependencyResolver();

        for (int i = 0; i < 10; i++) {
            createNodesWithRelationship(db, marker);
        }

        var lastBatchBeforeRestart =
                dependencyResolver.resolveDependency(TransactionIdStore.class).getLastCommittedBatch();
        var layout = dependencyResolver.resolveDependency(DatabaseLayout.class);
        dbms.shutdown();

        LogFiles logFiles = buildDefaultLogFiles(layout);
        assertEquals(lastBatchBeforeRestart, logFiles.getTailMetadata().lastBatch());
    }

    @Test
    void logLastBatchWithSeveralFilesAndCheckpointInTheEnd() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        var marker = RelationshipType.withName("marker");
        var dependencyResolver = db.getDependencyResolver();
        var originalLogFiles = dependencyResolver.resolveDependency(LogFiles.class);
        for (int j = 0; j < 12; j++) {
            for (int i = 0; i < 10; i++) {
                createNodesWithRelationship(db, marker);
            }
            originalLogFiles.getLogFile().rotate();
        }

        var lastBatchBeforeRestart =
                dependencyResolver.resolveDependency(TransactionIdStore.class).getLastCommittedBatch();
        var layout = dependencyResolver.resolveDependency(DatabaseLayout.class);
        dbms.shutdown();

        LogFiles logFiles = buildDefaultLogFiles(layout);
        assertEquals(lastBatchBeforeRestart, logFiles.getTailMetadata().lastBatch());
    }

    @Test
    void logLastBatchWithCheckpointAndRecordsAfterTheCheckpoint() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        var marker = RelationshipType.withName("marker");
        var dependencyResolver = db.getDependencyResolver();

        for (int j = 0; j < 12; j++) {
            for (int i = 0; i < 10; i++) {
                createNodesWithRelationship(db, marker);
            }
            dependencyResolver.resolveDependency(CheckPointer.class).forceCheckPoint(new SimpleTriggerInfo("Test"));
        }
        for (int i = 0; i < 10; i++) {
            createNodesWithRelationship(db, marker);
        }

        var lastBatchBeforeRestart =
                dependencyResolver.resolveDependency(TransactionIdStore.class).getLastCommittedBatch();
        var layout = dependencyResolver.resolveDependency(DatabaseLayout.class);
        dbms.shutdown();

        removeLastCheckpointRecordFromLastLogFile(layout, fileSystem);

        LogFiles logFiles = buildDefaultLogFiles(layout);
        assertEquals(lastBatchBeforeRestart, logFiles.getTailMetadata().lastBatch());
    }

    @Test
    void logLastBatchNoCheckpointsSeveralFilesAndCorruptionInFiles() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        var marker = RelationshipType.withName("marker");
        var dependencyResolver = db.getDependencyResolver();
        var originalLogFiles = dependencyResolver.resolveDependency(LogFiles.class);
        AppendBatchInfo lastBatchBeforeRestart = null;
        Path fileToManipulate = null;
        for (int j = 0; j < 12; j++) {
            for (int i = 0; i < 10; i++) {
                createNodesWithRelationship(db, marker);
            }
            if (j == 5) {
                lastBatchBeforeRestart = dependencyResolver
                        .resolveDependency(TransactionIdStore.class)
                        .getLastCommittedBatch();
                fileToManipulate = originalLogFiles
                        .getLogFile()
                        .getLogFileForVersion(
                                lastBatchBeforeRestart.logPositionAfter().getLogVersion());
            } else {
                originalLogFiles.getLogFile().rotate();
            }
        }

        var layout = dependencyResolver.resolveDependency(DatabaseLayout.class);
        Path[] checkpointFiles = originalLogFiles.getCheckpointFile().getDetachedCheckpointFiles();
        dbms.shutdown();

        for (Path checkpointFile : checkpointFiles) {
            fileSystem.delete(checkpointFile);
        }
        try (StoreFileChannel channel = fileSystem.open(fileToManipulate, WRITE_OPTIONS)) {
            channel.position(lastBatchBeforeRestart.logPositionAfter().getByteOffset())
                    .writeAll(ByteBuffer.wrap(new byte[1024]));
        }

        LogFiles logFiles = buildDefaultLogFiles(layout);
        assertEquals(lastBatchBeforeRestart, logFiles.getTailMetadata().lastBatch());
    }

    private static void createNodesWithRelationship(GraphDatabaseService db, RelationshipType marker) {
        try (Transaction transaction = db.beginTx()) {
            var start = transaction.createNode();
            var end = transaction.createNode();
            start.createRelationshipTo(end, marker);
            transaction.commit();
        }
    }

    private DatabaseManagementService buildDbms() {
        return new TestDatabaseManagementServiceBuilder(neo4jLayout)
                .setConfig(fail_on_missing_files, false)
                .build();
    }

    private LogFiles buildDefaultLogFiles(DatabaseLayout databaseLayout) throws IOException {
        return LogFilesBuilder.builder(databaseLayout, fileSystem, LATEST_KERNEL_VERSION_PROVIDER)
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withStorageEngineFactory(StorageEngineFactory.selectStorageEngine(Config.defaults()))
                .build();
    }
}
