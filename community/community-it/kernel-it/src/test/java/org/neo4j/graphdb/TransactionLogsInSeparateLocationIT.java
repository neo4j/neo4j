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
package org.neo4j.graphdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class TransactionLogsInSeparateLocationIT {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void databaseWithTransactionLogsInSeparateAbsoluteLocation() throws IOException {
        Path txDirectory = testDirectory.directory("transaction-logs");
        Config config = Config.newBuilder()
                .set(neo4j_home, testDirectory.homePath())
                .set(transaction_logs_root_path, txDirectory.toAbsolutePath())
                .build();
        DatabaseLayout layout = DatabaseLayout.of(config);
        StorageEngineFactory storageEngineFactory =
                performTransactions(txDirectory.toAbsolutePath(), layout.databaseDirectory());
        verifyTransactionLogs(layout.getTransactionLogsDirectory(), layout.databaseDirectory(), storageEngineFactory);
    }

    private static StorageEngineFactory performTransactions(Path txPath, Path storeDir) {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(storeDir)
                .setConfig(transaction_logs_root_path, txPath)
                .build();
        GraphDatabaseService database = managementService.database(DEFAULT_DATABASE_NAME);
        for (int i = 0; i < 10; i++) {
            try (Transaction transaction = database.beginTx()) {
                Node node = transaction.createNode();
                node.setProperty("a", "b");
                node.setProperty("c", "d");
                transaction.commit();
            }
        }
        StorageEngineFactory storageEngineFactory =
                ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency(StorageEngineFactory.class);
        managementService.shutdown();
        return storageEngineFactory;
    }

    private void verifyTransactionLogs(Path txDirectory, Path storeDir, StorageEngineFactory storageEngineFactory)
            throws IOException {
        LogFiles storeDirLogs =
                LogFilesBuilder.logFilesBasedOnlyBuilder(storeDir, fileSystem).build();
        assertFalse(storeDirLogs.getLogFile().versionExists(0));

        LogFiles txDirectoryLogs = LogFilesBuilder.logFilesBasedOnlyBuilder(txDirectory, fileSystem)
                .build();
        assertTrue(txDirectoryLogs.getLogFile().versionExists(0));
        try (PhysicalLogVersionedStoreChannel physicalLogVersionedStoreChannel =
                txDirectoryLogs.getLogFile().openForVersion(0)) {
            assertThat(physicalLogVersionedStoreChannel.size()).isGreaterThan(0L);
        }
    }
}
