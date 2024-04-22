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
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;
import static org.neo4j.kernel.recovery.RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

@PageCacheExtension
@Neo4jLayoutExtension
public class RecoveryAppendIndexIT {

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private Neo4jLayout neo4jLayout;

    private DatabaseManagementService dbms;
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setUp() {
        databaseLayout = neo4jLayout.databaseLayout(DEFAULT_DATABASE_NAME);
    }

    @AfterEach
    void tearDown() {
        if (dbms != null) {
            dbms.shutdown();
            dbms = null;
        }
    }

    @Test
    void restartDatabaseWithCorrectAppendIndex() {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        RelationshipType marker = RelationshipType.withName("marker");

        for (int i = 0; i < 10; i++) {
            createNodesWithRelationship(db, marker);
        }
        long lastAppendIndex = db.getDependencyResolver()
                .resolveDependency(MetadataProvider.class)
                .getLastAppendIndex();

        restartDbms();

        var restartedDb = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        long restartedLastAppendIndex = restartedDb
                .getDependencyResolver()
                .resolveDependency(MetadataProvider.class)
                .getLastAppendIndex();

        assertEquals(lastAppendIndex, restartedLastAppendIndex);
    }

    @Test
    void recoveredFromTheStartDatabaseWithCorrectAppendIndex() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        RelationshipType marker = RelationshipType.withName("marker");

        for (int i = 0; i < 10; i++) {
            createNodesWithRelationship(db, marker);
        }
        long lastAppendIndex = db.getDependencyResolver()
                .resolveDependency(MetadataProvider.class)
                .getLastAppendIndex();
        Path[] checkpointFiles = db.getDependencyResolver()
                .resolveDependency(LogFiles.class)
                .getCheckpointFile()
                .getDetachedCheckpointFiles();

        restartDbms();

        // all checkpoint log files are removed and recovery should replay the logs now
        for (Path checkpointFile : checkpointFiles) {
            fileSystem.deleteFile(checkpointFile);
        }

        var restartedDb = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        long restartedLastAppendIndex = restartedDb
                .getDependencyResolver()
                .resolveDependency(MetadataProvider.class)
                .getLastAppendIndex();

        assertEquals(lastAppendIndex, restartedLastAppendIndex);
    }

    @Test
    void recoveredAppendIndexFromTheDatabaseWithTransactionsAfterTheLastCheckpoint() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        RelationshipType marker = RelationshipType.withName("marker");

        for (int i = 0; i < 10; i++) {
            createNodesWithRelationship(db, marker);
        }

        db.getDependencyResolver()
                .resolveDependency(CheckPointer.class)
                .forceCheckPoint(new SimpleTriggerInfo("test checkpoint"));
        for (int i = 0; i < 10; i++) {
            createNodesWithRelationship(db, marker);
        }

        long lastAppendIndex = db.getDependencyResolver()
                .resolveDependency(MetadataProvider.class)
                .getLastAppendIndex();

        dbms.shutdown();
        // force recovery after existing checkpoint
        removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem);

        dbms = buildDbms();
        var restartedDb = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        long restartedLastAppendIndex = restartedDb
                .getDependencyResolver()
                .resolveDependency(MetadataProvider.class)
                .getLastAppendIndex();

        assertEquals(lastAppendIndex, restartedLastAppendIndex);
    }

    @Test
    void appliedIndexFromDatabaseWithMissingLogsFiles() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        RelationshipType marker = RelationshipType.withName("marker");

        for (int i = 0; i < 10; i++) {
            createNodesWithRelationship(db, marker);
        }
        dbms.shutdown();

        fileSystem.delete(databaseLayout.getTransactionLogsDirectory());

        restartDbms();

        var restartedDb = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        long restartedLastAppendIndex = restartedDb
                .getDependencyResolver()
                .resolveDependency(MetadataProvider.class)
                .getLastAppendIndex();

        // we do not create token indexes in this scenario
        assertEquals(1, restartedLastAppendIndex);
    }

    @Test
    void appliedIndexFromDatabaseWithMissingLogsFilesAndData() throws IOException {
        dbms = buildDbms();
        var db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        RelationshipType marker = RelationshipType.withName("marker");

        for (int i = 0; i < 10; i++) {
            createNodesWithRelationship(db, marker);
        }
        dbms.shutdown();

        fileSystem.delete(databaseLayout.databaseDirectory());
        fileSystem.delete(databaseLayout.getTransactionLogsDirectory());

        restartDbms();

        var restartedDb = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        long restartedLastAppendIndex = restartedDb
                .getDependencyResolver()
                .resolveDependency(MetadataProvider.class)
                .getLastAppendIndex();

        assertEquals(3, restartedLastAppendIndex);
    }

    private void restartDbms() {
        dbms.shutdown();
        dbms = buildDbms();
    }

    private static void createNodesWithRelationship(GraphDatabaseService db, RelationshipType marker) {
        try (Transaction transaction = db.beginTx()) {
            var start = transaction.createNode();
            var end = transaction.createNode();
            start.createRelationshipTo(end, marker);
            transaction.commit();
        }
    }

    DatabaseManagementService buildDbms() {
        return new TestDatabaseManagementServiceBuilder(neo4jLayout)
                .setConfig(fail_on_missing_files, false)
                .build();
    }
}
