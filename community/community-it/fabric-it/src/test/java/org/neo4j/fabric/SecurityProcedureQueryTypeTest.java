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
package org.neo4j.fabric;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({DefaultFileSystemExtension.class, TestDirectorySupportExtension.class})
class SecurityProcedureQueryTypeTest {
    @Inject
    static TestDirectory testDirectory;

    private static DatabaseManagementService databaseManagementService;

    @BeforeAll
    static void beforeAll() {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setConfig(GraphDatabaseSettings.auth_enabled, true)
                .build();
    }

    @AfterAll
    static void afterAll() {
        databaseManagementService.shutdown();
    }

    @Test
    void showCurrentUserShouldBeReadQueryType() {
        assertThat(databaseManagementService
                        .database("system")
                        .beginTx()
                        .execute("CALL dbms.showCurrentUser()")
                        .getQueryExecutionType()
                        .canUpdateData())
                .isFalse();
    }
}
