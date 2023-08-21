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
package org.neo4j.test;

import java.time.Duration;
import java.util.Objects;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;

public class AsyncDatabaseOperation {
    private AsyncDatabaseOperation() {}

    public static GraphDatabaseService findDatabaseEventually(
            DatabaseManagementService managementService, String databaseName) {
        return findDatabaseEventually(managementService, databaseName, Duration.ofSeconds(30));
    }

    public static GraphDatabaseService findDatabaseEventually(
            DatabaseManagementService managementService, String databaseName, Duration timeout) {
        try {
            return Awaitility.await()
                    .atMost(timeout)
                    .ignoreException(DatabaseNotFoundException.class)
                    .between(Duration.ofMillis(50), timeout)
                    .until(() -> findDatabase(managementService, databaseName), Objects::nonNull);
        } catch (ConditionTimeoutException e) {
            throw new DatabaseNotFoundException(databaseName);
        }
    }

    private static GraphDatabaseService findDatabase(DatabaseManagementService managementService, String databaseName) {
        var database = managementService.database(databaseName);
        return database.isAvailable() ? database : null;
    }
}
