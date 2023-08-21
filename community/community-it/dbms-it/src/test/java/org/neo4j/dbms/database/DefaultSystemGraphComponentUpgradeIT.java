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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class DefaultSystemGraphComponentUpgradeIT {
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;

    private SystemGraphComponents systemGraphComponents;

    @BeforeEach
    void setUp() {
        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, kibiBytes(128))
                .build();
        GraphDatabaseService database = managementService.database(DEFAULT_DATABASE_NAME);
        systemGraphComponents =
                ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency(SystemGraphComponents.class);
    }

    @AfterEach
    void tearDown() {
        managementService.shutdown();
    }

    @Test
    public void upgradeWithClashingIndexInPlace() throws Exception {
        // Given
        var systemDb = managementService.database(SYSTEM_DATABASE_NAME);
        try (Transaction tx = systemDb.beginTx()) {
            Label dbname = TopologyGraphDbmsModel.DATABASE_LABEL;
            tx.schema().getConstraints(dbname).forEach(ConstraintDefinition::drop);
            tx.schema().indexFor(dbname).on("name").withName("rogue").create();
            tx.commit();
        }

        // When
        systemGraphComponents.upgradeToCurrent(systemDb);

        // Then
        try (Transaction tx = systemDb.beginTx()) {
            Label dbname = TopologyGraphDbmsModel.DATABASE_LABEL;
            assertEquals(1, Iterables.asList(tx.schema().getConstraints(dbname)).size());
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName("rogue"));
        }
    }

    @Test
    public void upgradeWithClashingMultiPropertyIndexInPlace() throws Exception {
        // Given
        var systemDb = managementService.database(SYSTEM_DATABASE_NAME);
        try (Transaction tx = systemDb.beginTx()) {
            Label dbname = TopologyGraphDbmsModel.DATABASE_NAME_LABEL;
            tx.schema().getConstraints(dbname).forEach(ConstraintDefinition::drop);
            tx.schema()
                    .indexFor(dbname)
                    .on(TopologyGraphDbmsModel.NAME_PROPERTY)
                    .on(TopologyGraphDbmsModel.NAMESPACE_PROPERTY)
                    .withName("rogue")
                    .create();
            tx.commit();
        }

        // When
        systemGraphComponents.upgradeToCurrent(systemDb);

        // Then
        try (Transaction tx = systemDb.beginTx()) {
            Label dbname = TopologyGraphDbmsModel.DATABASE_NAME_LABEL;
            assertEquals(1, Iterables.asList(tx.schema().getConstraints(dbname)).size());
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName("rogue"));
        }
    }
}
