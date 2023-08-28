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
package org.neo4j.community.edition;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY;

import java.util.HashSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseDetails;
import org.neo4j.dbms.database.TopologyInfoService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class DefaultTopologyInfoServiceIT {
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
        assertTrue(dependencyResolver.containsDependency(TopologyInfoService.class));
    }

    @Test
    void shouldReturnInfoForAllExistingDatabases() {
        // given
        var dependencyResolver = ((GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME)).getDependencyResolver();
        var topologyInfoService = dependencyResolver.resolveDependency(TopologyInfoService.class);
        var nonExistingDatabase = DatabaseIdFactory.from("unknown", UUID.randomUUID());
        var existingDatabases =
                dbms.listDatabases().stream().map(this::getIdForName).collect(Collectors.toSet());
        var allDatabases = new HashSet<>(existingDatabases);
        allDatabases.add(nonExistingDatabase);

        // when
        // DefaultTopologyInfoService does not use the transaction
        var results = topologyInfoService.databases(null, allDatabases, TopologyInfoService.RequestedExtras.NONE);
        var returnedDatabases =
                results.stream().collect(Collectors.toMap(DatabaseDetails::namedDatabaseId, Function.identity()));

        // then
        assertThat(returnedDatabases.size()).isEqualTo(allDatabases.size());
        assertThat(returnedDatabases.get(nonExistingDatabase).status()).isEqualTo("unknown");
        assertThat(returnedDatabases.get(existingDatabases.iterator().next()).status())
                .isEqualTo("online");
    }

    NamedDatabaseId getIdForName(String name) {
        try (Transaction tx = dbms.database(SYSTEM_DATABASE_NAME).beginTx()) {
            String temp = tx.findNodes(DATABASE_LABEL, DATABASE_NAME_PROPERTY, name)
                    .next()
                    .getProperty(DATABASE_UUID_PROPERTY)
                    .toString();
            return DatabaseIdFactory.from(name, UUID.fromString(temp));
        } catch (Exception e) {
            fail(e);
            return null;
        }
    }
}
