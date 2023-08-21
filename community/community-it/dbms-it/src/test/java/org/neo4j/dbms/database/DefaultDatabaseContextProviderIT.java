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
package org.neo4j.dbms.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DefaultDatabaseContextProviderIT {
    private NamedDatabaseId defaultNamedDatabaseId;

    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService database;
    private DatabaseManagementService managementService;
    private DatabaseContextProvider<?> databaseContextProvider;

    @BeforeEach
    void setUp() {
        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, kibiBytes(128))
                .build();
        database = managementService.database(DEFAULT_DATABASE_NAME);
        databaseContextProvider =
                ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency(DatabaseContextProvider.class);
        defaultNamedDatabaseId = databaseContextProvider
                .databaseIdRepository()
                .getByName(DEFAULT_DATABASE_NAME)
                .orElseThrow();
    }

    @AfterEach
    void tearDown() {
        managementService.shutdown();
    }

    @Test
    void lookupExistingDatabase() {
        var defaultDatabaseContext = databaseContextProvider.getDatabaseContext(defaultNamedDatabaseId);
        var systemDatabaseContext = databaseContextProvider.getDatabaseContext(NAMED_SYSTEM_DATABASE_ID);

        assertTrue(defaultDatabaseContext.isPresent());
        assertTrue(systemDatabaseContext.isPresent());
    }

    @Test
    void listDatabases() {
        var databases = databaseContextProvider.registeredDatabases();
        assertEquals(2, databases.size());
        List<NamedDatabaseId> databaseNames = new ArrayList<>(databases.keySet());
        assertEquals(NAMED_SYSTEM_DATABASE_ID, databaseNames.get(0));
        assertEquals(defaultNamedDatabaseId, databaseNames.get(1));
    }
}
