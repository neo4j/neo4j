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

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
class RecoveryLogIT {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private DatabaseLayout databaseLayout;

    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @Test
    void transactionsRecoveryLogContainsTimeSpent() throws IOException {
        // Create database with forced recovery
        Path tmpLogDir = testDirectory.directory("logs");
        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);

        try (Transaction tx = db.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            node1.createRelationshipTo(node2, RelationshipType.withName("likes"));
            tx.commit();
        }

        Path[] txLogs = LogFilesBuilder.logFilesBasedOnlyBuilder(
                        databaseLayout.getTransactionLogsDirectory(), fileSystem)
                .build()
                .logFiles();
        for (Path file : txLogs) {
            fileSystem.copyToDirectory(file, tmpLogDir);
        }

        managementService.shutdown();

        for (Path txLog : fileSystem.listFiles(databaseLayout.getTransactionLogsDirectory())) {
            fileSystem.deleteFile(txLog);
        }

        for (Path file : LogFilesBuilder.logFilesBasedOnlyBuilder(tmpLogDir, fileSystem)
                .build()
                .logFiles()) {
            fileSystem.moveToDirectory(file, databaseLayout.getTransactionLogsDirectory());
        }

        AssertableLogProvider provider = new AssertableLogProvider();
        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setInternalLogProvider(provider)
                .build();
        managementService.database(DEFAULT_DATABASE_NAME);

        assertThat(provider).containsMessages("Recovery completed", "Time spent");
    }
}
