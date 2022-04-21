/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.community.edition;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.util.HashSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseInfoService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class StandaloneDatabaseInfoServiceIT {
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService dbms;

    @BeforeEach
    void setUp() {
        dbms = new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).build();
    }

    @AfterEach
    void tearDown() {
        dbms.shutdown();
    }

    @Test
    void shouldBeAvailableInDependencyResolver() {
        var dependencyResolver = ((GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME)).getDependencyResolver();
        assertTrue(dependencyResolver.containsDependency(DatabaseInfoService.class));
    }

    @Test
    void shouldReturnInfoForAllExistingDatabases() {
        // given
        var dependencyResolver = ((GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME)).getDependencyResolver();
        var databaseInfoService = dependencyResolver.resolveDependency(DatabaseInfoService.class);
        var nonExistingDatabase = "foo";
        var existingDatabases = dbms.listDatabases();
        var allDatabases = new HashSet<>(existingDatabases);
        allDatabases.add(nonExistingDatabase);

        // when
        var results = databaseInfoService.lookupCachedInfo(allDatabases);
        var returnedDatabases = results.stream()
                .map(databaseInfo -> databaseInfo.namedDatabaseId().name())
                .collect(Collectors.toSet());

        // then
        assertThat(returnedDatabases.size()).isEqualTo(existingDatabases.size());
        assertTrue(returnedDatabases.containsAll(existingDatabases));
    }
}
