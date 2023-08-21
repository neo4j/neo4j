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
package org.neo4j.kernel.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

import java.io.IOException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.api.database.DatabaseSizeService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DatabaseSizeServiceIT {
    @Inject
    TestDirectory testDirectory;

    @Test
    void sizeOfDatabaseWithDifferentDataAndLogFolder() throws IOException {
        var managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).build();
        try {
            var database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            var databaseLayout = database.databaseLayout();
            assertNotEquals(databaseLayout.getTransactionLogsDirectory(), databaseLayout.databaseDirectory());

            verifyDatabaseSize(database);
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void sizeOfDatabaseWithSameDataAndLogFolder() throws IOException {
        var managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setConfig(transaction_logs_root_path, testDirectory.homePath())
                .setConfig(databases_root_path, testDirectory.homePath())
                .build();
        try {
            var database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            var databaseLayout = database.databaseLayout();
            assertEquals(databaseLayout.getTransactionLogsDirectory(), databaseLayout.databaseDirectory());

            verifyDatabaseSize(database);
        } finally {
            managementService.shutdown();
        }
    }

    private static void verifyDatabaseSize(GraphDatabaseAPI database) throws IOException {
        long propertySize = ByteUnit.mebiBytes(1);

        try (Transaction transaction = database.beginTx()) {
            Node node = transaction.createNode();
            node.setProperty("property", RandomStringUtils.randomAscii((int) propertySize));
            transaction.commit();
        }

        var databaseSizeService = database.getDependencyResolver().resolveDependency(DatabaseSizeService.class);

        assertThat(databaseSizeService.getDatabaseDataSize(database.databaseId()))
                .isGreaterThan(0);
        assertThat(databaseSizeService.getDatabaseTotalSize(database.databaseId()))
                .isGreaterThan(propertySize);
    }
}
