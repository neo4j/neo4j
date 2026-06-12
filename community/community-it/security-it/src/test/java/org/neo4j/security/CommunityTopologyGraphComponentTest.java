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
package org.neo4j.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.ComponentVersion.COMMUNITY_TOPOLOGY_GRAPH_COMPONENT;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_ACCESS_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_DEFAULT_LANGUAGE_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DISPLAY_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseAccess.READ_WRITE;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PRIMARY_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.QUOTED_DISPLAY_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.REMOTE_USERNAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS_RELATIONSHIP;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGET_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.URL_PROPERTY;

import java.time.Clock;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphComponent;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.Stringifier;

@TestDirectoryExtension
@TestInstance(PER_CLASS)
class CommunityTopologyGraphComponentTest {
    @Inject
    private static TestDirectory directory;

    private static DatabaseManagementService dbms;
    private static GraphDatabaseService system;

    @BeforeAll
    static void setup() {
        dbms = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .impermanent()
                .setConfig(Map.of(GraphDatabaseInternalSettings.trace_cursors, true))
                .noOpSystemGraphInitializer()
                .build();
        system = dbms.database(SYSTEM_DATABASE_NAME);
    }

    @BeforeEach
    void clear() throws Exception {
        inTx(tx -> Iterables.forEach(tx.getAllNodes(), n -> {
            Iterables.forEach(n.getRelationships(), Relationship::delete);
            n.delete();
        }));
    }

    @AfterAll
    static void tearDown() {
        dbms.shutdown();
    }

    @Test
    void shouldDetectUninitialized() throws Exception {
        initializeSystem();
        CommunityTopologyGraphComponent component =
                new CommunityTopologyGraphComponent(Config.defaults(), NullLogProvider.getInstance());
        inTx(tx -> {
            SystemGraphComponent.Status status = component.detect(tx);
            assertThat(status).isEqualTo(SystemGraphComponent.Status.UNINITIALIZED);
        });
    }

    @Test
    void shouldDetectStatusForLatest() throws Exception {
        // GIVEN
        initializeSystem();
        CommunityTopologyGraphComponent component =
                new CommunityTopologyGraphComponent(Config.defaults(), NullLogProvider.getInstance());
        component.initializeSystemGraph(system, true);

        // WHEN .. THEN
        inTx(tx -> {
            SystemGraphComponent.Status status = component.detect(tx);
            assertThat(status).isEqualTo(SystemGraphComponent.Status.CURRENT);
        });
    }

    @Test
    void shouldSetAccessToReadWriteOnInitialization() throws Exception {
        // GIVEN
        initializeSystem();
        CommunityTopologyGraphComponent component =
                new CommunityTopologyGraphComponent(Config.defaults(), NullLogProvider.getInstance());

        // WHEN
        component.initializeSystemGraph(system, true);

        // THEN
        inTx(tx -> {
            // Default db
            Node defaultDbNode = tx.findNode(DATABASE_LABEL, DATABASE_NAME_PROPERTY, DEFAULT_DATABASE_NAME);
            String defaultDbAccess =
                    defaultDbNode.getProperty(DATABASE_ACCESS_PROPERTY).toString();
            assertThat(defaultDbAccess).isEqualTo(READ_WRITE.toString());

            // System db
            Node systemDbNode = tx.findNode(DATABASE_LABEL, DATABASE_NAME_PROPERTY, SYSTEM_DATABASE_NAME);
            String systemDbAccess =
                    systemDbNode.getProperty(DATABASE_ACCESS_PROPERTY).toString();
            assertThat(systemDbAccess).isEqualTo(READ_WRITE.toString());
        });
    }

    @Test
    void shouldSetAccessToReadWriteOnUpgrade() throws Exception {
        // GIVEN
        initializeSystem();

        // Simulate that we are upgrading from an earlier version with more databases
        inTx(tx -> {
            Node dbNode = tx.createNode(DATABASE_LABEL);
            dbNode.setProperty(DATABASE_NAME_PROPERTY, "custom");
        });

        CommunityTopologyGraphComponent component =
                new CommunityTopologyGraphComponent(Config.defaults(), NullLogProvider.getInstance());

        // WHEN
        component.initializeSystemGraph(system, true);

        // THEN
        inTx(tx -> {
            // Custom db
            Node dbNode = tx.findNode(DATABASE_LABEL, DATABASE_NAME_PROPERTY, "custom");
            String dbAccess = dbNode.getProperty(DATABASE_ACCESS_PROPERTY).toString();
            assertThat(dbAccess).isEqualTo(READ_WRITE.toString());

            // Default db
            Node defaultDbNode = tx.findNode(DATABASE_LABEL, DATABASE_NAME_PROPERTY, DEFAULT_DATABASE_NAME);
            String defaultDbAccess =
                    defaultDbNode.getProperty(DATABASE_ACCESS_PROPERTY).toString();
            assertThat(defaultDbAccess).isEqualTo(READ_WRITE.toString());

            // System db
            Node systemDbNode = tx.findNode(DATABASE_LABEL, DATABASE_NAME_PROPERTY, SYSTEM_DATABASE_NAME);
            String systemDbAccess =
                    systemDbNode.getProperty(DATABASE_ACCESS_PROPERTY).toString();
            assertThat(systemDbAccess).isEqualTo(READ_WRITE.toString());
        });
    }

    @Test
    void shouldHavePrimaryAliasOnInitialization() throws Exception {
        // GIVEN
        initializeSystem();
        CommunityTopologyGraphComponent component =
                new CommunityTopologyGraphComponent(Config.defaults(), NullLogProvider.getInstance());

        // WHEN
        component.initializeSystemGraph(system, true);

        // THEN
        inTx(tx -> {
            // Default db
            shouldHavePrimaryAlias(DEFAULT_DATABASE_NAME, tx);

            // System db
            shouldHavePrimaryAlias(SYSTEM_DATABASE_NAME, tx);
        });
    }

    @Test
    void shouldHavePrimaryAliasOnUpgrade() throws Exception {
        // GIVEN
        initializeSystem();

        // Simulate that we are upgrading from an earlier version with more databases
        inTx(tx -> {
            Node dbNode = tx.createNode(DATABASE_LABEL);
            dbNode.setProperty(DATABASE_NAME_PROPERTY, "custom");
        });

        CommunityTopologyGraphComponent component =
                new CommunityTopologyGraphComponent(Config.defaults(), NullLogProvider.getInstance());

        // WHEN
        component.initializeSystemGraph(system, true);

        // THEN
        inTx(tx -> {
            // Custom db
            shouldHavePrimaryAlias("custom", tx);

            // Default db
            shouldHavePrimaryAlias(DEFAULT_DATABASE_NAME, tx);

            // System db
            shouldHavePrimaryAlias(SYSTEM_DATABASE_NAME, tx);
        });
    }

    @Test
    void shouldHavePrimaryAliasOnUpgradeFromV0ToV1() throws Exception {
        // GIVEN
        initializeSystem();
        CommunityTopologyGraphComponent component =
                new CommunityTopologyGraphComponent(Config.defaults(), NullLogProvider.getInstance());
        component.initializeSystemGraph(system, true);

        inTx(tx -> {
            // Remove all the :DatabaseName nodes from the created database to simulate pre-4.4 behavior
            var nameNode = tx.findNode(DATABASE_NAME_LABEL, DATABASE_NAME_PROPERTY, DEFAULT_DATABASE_NAME);
            nameNode.getRelationships().forEach(Relationship::delete);
            nameNode.delete();
        });
        setComponentVersionTo(0);

        // WHEN
        component.upgradeToCurrent(system);

        // THEN
        inTx(tx -> shouldHavePrimaryAlias(DEFAULT_DATABASE_NAME, tx));
    }

    @Test
    void shouldHaveNamespaceAndDisplayNameOnUpgradeToV1() throws Exception {
        // GIVEN
        initializeSystem();
        CommunityTopologyGraphComponent component =
                new CommunityTopologyGraphComponent(Config.defaults(), NullLogProvider.getInstance());
        component.initializeSystemGraph(system, true);

        inTx(tx -> {
            // Remove any namespaces / displaynames to get 4.4 behaviour
            try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_NAME_LABEL)) {
                nodes.forEachRemaining(node -> {
                    node.removeProperty(NAMESPACE_PROPERTY);
                    node.removeProperty(DISPLAY_NAME_PROPERTY);
                });
            }
        });
        setComponentVersionTo(0);

        // WHEN
        component.upgradeToCurrent(system);

        // THEN
        inTx(tx -> {
            try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_NAME_LABEL)) {
                nodes.forEachRemaining(node -> {
                    String name = (String) node.getProperty(NAME_PROPERTY);
                    assertThat(node.getProperty(DISPLAY_NAME_PROPERTY)).isEqualTo(name);
                    assertThat(node.getProperty(NAMESPACE_PROPERTY)).isEqualTo(DEFAULT_NAMESPACE);
                });
            }
        });
    }

    @Test
    void shouldHaveQuotedDisplayNameOnUpgradeToV3() throws Exception {
        // GIVEN
        initializeSystem();
        CommunityTopologyGraphComponent component =
                new CommunityTopologyGraphComponent(Config.defaults(), NullLogProvider.getInstance());
        component.initializeSystemGraph(system, true);

        inTx(tx -> {
            // Remove any quotedDisplayNames to get old behaviour
            try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_NAME_LABEL)) {
                nodes.forEachRemaining(node -> node.removeProperty(QUOTED_DISPLAY_NAME_PROPERTY));
            }
        });
        setComponentVersionTo(0);

        // WHEN
        component.upgradeToCurrent(system);

        // THEN
        inTx(tx -> {
            try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_NAME_LABEL)) {
                nodes.forEachRemaining(node -> {
                    String name = (String) node.getProperty(NAME_PROPERTY);
                    assertThat(node.getProperty(QUOTED_DISPLAY_NAME_PROPERTY)).isEqualTo(Stringifier.backtick(name));
                });
            }
        });
    }

    @Test
    void shouldHaveDefaultLanguageOnDatabasesOnUpgradeToV3() throws Exception {
        // GIVEN
        initializeSystem();
        CommunityTopologyGraphComponent component = new CommunityTopologyGraphComponent(
                Config.defaults(Map.of(
                        // to show we don't pick up on the config value when upgrading
                        GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher25
                        // Might need to be enabled when the next experimental version appear:
                        // GraphDatabaseInternalSettings.enable_experimental_cypher_versions,Boolean.TRUE
                        )),
                NullLogProvider.getInstance());
        component.initializeSystemGraph(system, true);

        inTx(tx -> {
            // Remove any defaultLanguage to get old behaviour
            try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_LABEL)) {
                nodes.forEachRemaining(node -> node.removeProperty(DATABASE_DEFAULT_LANGUAGE_PROPERTY));
            }
        });
        setComponentVersionTo(0);

        // WHEN
        component.upgradeToCurrent(system);

        // THEN
        inTx(tx -> {
            try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_LABEL)) {
                nodes.forEachRemaining(node -> assertThat(node.getProperty(DATABASE_DEFAULT_LANGUAGE_PROPERTY))
                        .isEqualTo(CypherVersion.Cypher5.persistedValue));
            }
        });
    }

    @Test
    void shouldHaveDefaultLanguageOnAliasesOnUpgradeToV4() throws Exception {
        // GIVEN
        initializeSystem();
        CommunityTopologyGraphComponent component = new CommunityTopologyGraphComponent(
                Config.defaults(Map.of(
                        // to show we don't pick up on the config value when upgrading
                        GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher25
                        // Might need to be enabled when the next experimental version appear:
                        // GraphDatabaseInternalSettings.enable_experimental_cypher_versions, Boolean.TRUE
                        )),
                NullLogProvider.getInstance());
        component.initializeSystemGraph(system, true);

        inTx(tx -> {
            // Add aliases to check (missing the relationships to other things and some properties on remote alias)
            // cannot use `tx.execute(...)` since we're in community and aliases (and composite database) are enterprise
            Node localAlias = tx.createNode(DATABASE_NAME_LABEL);
            localAlias.setProperty(NAME_PROPERTY, "local");
            localAlias.setProperty(NAMESPACE_PROPERTY, DEFAULT_NAMESPACE);
            localAlias.setProperty(PRIMARY_PROPERTY, false);
            localAlias.setProperty(DISPLAY_NAME_PROPERTY, "local");
            localAlias.setProperty(QUOTED_DISPLAY_NAME_PROPERTY, "local");

            Node localConstituentAlias = tx.createNode(DATABASE_NAME_LABEL);
            localConstituentAlias.setProperty(NAME_PROPERTY, "composite.local");
            localConstituentAlias.setProperty(NAMESPACE_PROPERTY, "composite");
            localConstituentAlias.setProperty(PRIMARY_PROPERTY, false);
            localConstituentAlias.setProperty(DISPLAY_NAME_PROPERTY, "composite.local");
            localConstituentAlias.setProperty(QUOTED_DISPLAY_NAME_PROPERTY, "composite.local");

            Node remoteAlias = tx.createNode(DATABASE_NAME_LABEL);
            remoteAlias.setProperty(NAME_PROPERTY, "remote");
            remoteAlias.setProperty(NAMESPACE_PROPERTY, DEFAULT_NAMESPACE);
            remoteAlias.setProperty(PRIMARY_PROPERTY, false);
            remoteAlias.setProperty(DISPLAY_NAME_PROPERTY, "remote");
            remoteAlias.setProperty(QUOTED_DISPLAY_NAME_PROPERTY, "remote");
            remoteAlias.setProperty(TARGET_NAME_PROPERTY, "target");
            remoteAlias.setProperty(URL_PROPERTY, "neo4j+s://remote-location");
            remoteAlias.setProperty(REMOTE_USERNAME_PROPERTY, "remoteUser");

            Node remoteConstituentAlias = tx.createNode(DATABASE_NAME_LABEL);
            remoteConstituentAlias.setProperty(NAME_PROPERTY, "composite.remote");
            remoteConstituentAlias.setProperty(NAMESPACE_PROPERTY, "composite");
            remoteConstituentAlias.setProperty(PRIMARY_PROPERTY, false);
            remoteConstituentAlias.setProperty(DISPLAY_NAME_PROPERTY, "composite.remote");
            remoteConstituentAlias.setProperty(QUOTED_DISPLAY_NAME_PROPERTY, "composite.remote");
            remoteAlias.setProperty(TARGET_NAME_PROPERTY, "target");
            remoteAlias.setProperty(URL_PROPERTY, "neo4j+s://remote-location");
            remoteAlias.setProperty(REMOTE_USERNAME_PROPERTY, "remoteUser");

            // Remove any defaultLanguage to get old behaviour
            try (ResourceIterator<Node> nodes =
                    tx.findNodes(REMOTE_DATABASE_LABEL, NAMESPACE_PROPERTY, DEFAULT_NAMESPACE)) {
                nodes.forEachRemaining(node -> node.removeProperty(DATABASE_DEFAULT_LANGUAGE_PROPERTY));
            }
        });
        setComponentVersionTo(0);

        // WHEN
        component.upgradeToCurrent(system);

        // THEN
        inTx(tx -> {
            try (ResourceIterator<Node> nodes = tx.findNodes(DATABASE_NAME_LABEL)) {
                nodes.forEachRemaining(node -> {
                    if (node.hasLabel(REMOTE_DATABASE_LABEL)
                            && node.getProperty(NAMESPACE_PROPERTY, DEFAULT_NAMESPACE)
                                    .equals(DEFAULT_NAMESPACE)) {
                        assertThat(node.getProperty(DATABASE_DEFAULT_LANGUAGE_PROPERTY, null))
                                .isEqualTo(CypherVersion.Cypher5.persistedValue);
                    } else {
                        assertThat(node.getProperty(DATABASE_DEFAULT_LANGUAGE_PROPERTY, null))
                                .isNull();
                    }
                });
            }
        });
    }

    private static void shouldHavePrimaryAlias(String dbName, Transaction tx) {
        Node dbAlias = tx.findNode(DATABASE_NAME_LABEL, DATABASE_NAME_PROPERTY, dbName);
        assertThat(dbAlias)
                .describedAs("No aliases found for database: " + dbName)
                .isNotNull();
        assertThat(dbAlias.getProperty(PRIMARY_PROPERTY)).isEqualTo(true);
        Iterables.forEach(
                dbAlias.getRelationships(TARGETS_RELATIONSHIP),
                target ->
                        assertThat(target.getEndNode().hasLabel(DATABASE_LABEL)).isTrue());
    }

    private static void setComponentVersionTo(int n) throws Exception {
        // Set the component version back to a previous version to re-trigger upgrade from this point
        inTx(tx -> Iterators.first(tx.findNodes(Label.label("Version")))
                .setProperty(COMMUNITY_TOPOLOGY_GRAPH_COMPONENT.name(), n));
    }

    private static void initializeSystem() throws Exception {
        var systemGraphComponent = new DefaultSystemGraphComponent(Config.defaults(), Clock.systemUTC());
        systemGraphComponent.initializeSystemGraph(system, true);
    }

    private static void inTx(ThrowingConsumer<Transaction, Exception> consumer) throws Exception {
        try (Transaction tx = system.beginTx()) {
            consumer.accept(tx);
            tx.commit();
        }
    }
}
