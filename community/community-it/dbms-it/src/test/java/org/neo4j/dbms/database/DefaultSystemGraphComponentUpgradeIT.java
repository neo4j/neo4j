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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DISPLAY_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PRIMARY_PROPERTY;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.test.extension.SkipOnSpd.Note.irrelevant;

import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.exceptions.UpgradeException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@SkipOnSpd(notes = irrelevant, reason = "Enterprise system db upgrade path is different")
class DefaultSystemGraphComponentUpgradeIT {
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
    void upgradeWithClashingIndexInPlace() throws Exception {
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
            assertThat(Iterables.asList(tx.schema().getConstraints(dbname))).hasSize(1);
            assertThatThrownBy(() -> tx.schema().getIndexByName("rogue")).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void upgradeWithClashingMultiPropertyIndexInPlace() throws Exception {
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
            assertThat(Iterables.asList(tx.schema().getConstraints(dbname))).hasSize(2);
            assertThatThrownBy(() -> tx.schema().getIndexByName("rogue")).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldFailOnConflictingDisplayNamesAfterUpgrade() throws Exception {
        // Given - system with removed displayName constraint to simulate old version
        var systemDb = managementService.database(SYSTEM_DATABASE_NAME);
        dropDisplayNameConstraint(systemDb);

        // When
        systemGraphComponents.upgradeToCurrent(systemDb);

        // Then
        try (Transaction tx = systemDb.beginTx()) {
            // Create two databases with conflicting display names
            Node localAlias = tx.createNode(DATABASE_NAME_LABEL);
            localAlias.setProperty(NAME_PROPERTY, "composite.local");
            localAlias.setProperty(NAMESPACE_PROPERTY, DEFAULT_NAMESPACE);
            localAlias.setProperty(PRIMARY_PROPERTY, false);
            localAlias.setProperty(DISPLAY_NAME_PROPERTY, "composite.local");

            Node localConstituentAlias = tx.createNode(DATABASE_NAME_LABEL);
            localConstituentAlias.setProperty(NAME_PROPERTY, "composite.local");
            localConstituentAlias.setProperty(NAMESPACE_PROPERTY, "composite");
            localConstituentAlias.setProperty(PRIMARY_PROPERTY, false);

            assertThatThrownBy(() -> localConstituentAlias.setProperty(DISPLAY_NAME_PROPERTY, "composite.local"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining(
                            "already exists with label `DatabaseName` and property `displayName` = 'composite.local'");
        }
    }

    @Test
    void shouldFailUpgradeOnAlreadyExistingConflictingDisplayNames() throws Exception {
        // Given - system with removed displayName constraint to simulate old version
        var systemDb = managementService.database(SYSTEM_DATABASE_NAME);
        dropDisplayNameConstraint(systemDb);

        // When
        try (Transaction tx = systemDb.beginTx()) {
            // Create two databases with conflicting display names
            Node localAlias = tx.createNode(DATABASE_NAME_LABEL);
            localAlias.setProperty(NAME_PROPERTY, "foo.bar");
            localAlias.setProperty(NAMESPACE_PROPERTY, DEFAULT_NAMESPACE);
            localAlias.setProperty(DISPLAY_NAME_PROPERTY, "foo.bar");

            Node localConstituentAlias = tx.createNode(DATABASE_NAME_LABEL);
            localConstituentAlias.setProperty(NAME_PROPERTY, "bar");
            localConstituentAlias.setProperty(NAMESPACE_PROPERTY, "foo");
            localConstituentAlias.setProperty(DISPLAY_NAME_PROPERTY, "foo.bar");
            tx.commit();
        }

        // Then
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> systemGraphComponents.upgradeToCurrent(systemDb))
                .isInstanceOf(UpgradeException.class)
                .hasMessage(
                        "The upgrade to a new Neo4j version failed. Databases 'bar' and 'foo.bar' have ambiguous names."
                                + " Rename one of them before retrying the upgrade.")
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_51N76)
                .hasStatusDescription(
                        "error: system configuration or operation exception - upgrade failed. The upgrade to a new Neo4j version failed.")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_42N87)
                .hasStatusDescription(
                        "error: syntax error or access rule violation - database or alias with similar name exists. "
                                + "The database or alias name `bar` conflicts with the name `foo.bar` of an existing database or alias.");
    }

    private void dropDisplayNameConstraint(GraphDatabaseService systemDb) {
        try (var tx = systemDb.beginTx()) {
            var constraints = tx.schema().getConstraints(DATABASE_NAME_LABEL);
            for (ConstraintDefinition constraintDefinition : constraints) {
                var properties = StreamSupport.stream(
                                constraintDefinition.getPropertyKeys().spliterator(), false)
                        .toList();
                if (properties.contains(DISPLAY_NAME_PROPERTY)) {
                    constraintDefinition.drop();
                }
            }
            tx.commit();
        }
    }
}
