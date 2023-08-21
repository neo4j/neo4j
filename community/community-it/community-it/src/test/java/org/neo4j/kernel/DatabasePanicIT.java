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
package org.neo4j.kernel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Panic;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class DatabasePanicIT {

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() {
        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).build();
    }

    @AfterEach
    void tearDown() {
        managementService.shutdown();
    }

    @Test
    void databasePanicNotification() throws DatabaseExistsException {
        var panicListener = new PanicDatabaseEventListener("neo4j");
        managementService.registerDatabaseEventListener(panicListener);

        assertFalse(panicListener.isPanic());

        getDatabaseHealth(managementService.database("neo4j"))
                .panic(new IllegalStateException("Whoops, something went wrong here."));
        assertTrue(panicListener.isPanic());

        assertShowDatabases();
    }

    private void assertShowDatabases() {
        assertEventually(
                () -> managementService
                        .database(SYSTEM_DATABASE_NAME)
                        .executeTransactionally("SHOW DATABASE neo4j", Map.of(), rawResult -> {
                            var resultRows = Iterators.asList(rawResult);
                            assertEquals(1, resultRows.size());
                            var resultRow = resultRows.get(0);
                            return new DatabaseStatus(
                                    (String) resultRow.get("currentStatus"), (String) resultRow.get("statusMessage"));
                        }),
                databaseStatus -> databaseStatus.currentStatus().equals("online")
                        && databaseStatus.statusMessage().equals("Whoops, something went wrong here."),
                30,
                TimeUnit.SECONDS);
    }

    private static Panic getDatabaseHealth(GraphDatabaseService service) {
        return ((GraphDatabaseAPI) service).getDependencyResolver().resolveDependency(DatabaseHealth.class);
    }

    private static class PanicDatabaseEventListener extends DatabaseEventListenerAdapter {
        private final String databaseName;
        private boolean panic;

        PanicDatabaseEventListener(String databaseName) {
            this.databaseName = databaseName;
        }

        @Override
        public void databasePanic(DatabaseEventContext eventContext) {
            if (databaseName.equals(eventContext.getDatabaseName())) {
                panic = true;
            }
        }

        boolean isPanic() {
            return panic;
        }
    }

    private record DatabaseStatus(String currentStatus, String statusMessage) {}
}
