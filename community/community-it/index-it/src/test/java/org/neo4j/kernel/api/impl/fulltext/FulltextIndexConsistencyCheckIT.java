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
package org.neo4j.kernel.api.impl.fulltext;

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.array;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.FULLTEXT_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asNodeLabelStr;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asPropertiesStrList;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asRelationshipTypeStr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class FulltextIndexConsistencyCheckIT {
    @Inject
    private TestDirectory testDirectory;

    private DatabaseLayout databaseLayout;

    @Inject
    private RandomSupport random;

    private TestDatabaseManagementServiceBuilder builder;
    private GraphDatabaseAPI database;
    private DatabaseManagementService managementService;

    @BeforeEach
    void before() {
        builder = new TestDatabaseManagementServiceBuilder(testDirectory.homePath());
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            managementService.shutdown();
        }
    }

    @Test
    void mustBeAbleToConsistencyCheckEmptyDatabaseWithWithEmptyFulltextIndexes() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("Label"), asPropertiesStrList("prop")))
                    .close();
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("R1"), asPropertiesStrList("p1")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeIndexWithOneLabelAndOneProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("Label"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.createNode(Label.label("Label")).setProperty("prop", "value");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeIndexWithOneLabelAndMultipleProperties() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("Label"), asPropertiesStrList("p1", "p2")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode(Label.label("Label"));
            node.setProperty("p1", "value");
            node.setProperty("p2", "value");
            tx.createNode(Label.label("Label")).setProperty("p1", "value");
            tx.createNode(Label.label("Label")).setProperty("p2", "value");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeIndexWithMultipleLabelsAndOneProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("L1", "L2"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.createNode(Label.label("L1"), Label.label("L2")).setProperty("prop", "value");
            tx.createNode(Label.label("L2")).setProperty("prop", "value");
            tx.createNode(Label.label("L1")).setProperty("prop", "value");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeIndexWithManyLabelsAndOneProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        // Enough labels to prevent inlining them into the node record, and instead require a dynamic label record to be
        // allocated.
        String[] labels = {
            "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10", "L11", "L12", "L13", "L14", "L15", "L16"
        };
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr(labels), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.createNode(Stream.of(labels).map(Label::label).toArray(Label[]::new))
                    .setProperty("prop", "value");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeIndexWithMultipleLabelsAndMultipleProperties() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("L1", "L2"), asPropertiesStrList("p1", "p2")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node n1 = tx.createNode(Label.label("L1"), Label.label("L2"));
            n1.setProperty("p1", "value");
            n1.setProperty("p2", "value");
            Node n2 = tx.createNode(Label.label("L1"), Label.label("L2"));
            n2.setProperty("p1", "value");
            Node n3 = tx.createNode(Label.label("L1"), Label.label("L2"));
            n3.setProperty("p2", "value");
            Node n4 = tx.createNode(Label.label("L1"));
            n4.setProperty("p1", "value");
            n4.setProperty("p2", "value");
            Node n5 = tx.createNode(Label.label("L1"));
            n5.setProperty("p1", "value");
            Node n6 = tx.createNode(Label.label("L1"));
            n6.setProperty("p2", "value");
            Node n7 = tx.createNode(Label.label("L2"));
            n7.setProperty("p1", "value");
            n7.setProperty("p2", "value");
            Node n8 = tx.createNode(Label.label("L2"));
            n8.setProperty("p1", "value");
            Node n9 = tx.createNode(Label.label("L2"));
            n9.setProperty("p2", "value");
            tx.createNode(Label.label("L2")).setProperty("p1", "value");
            tx.createNode(Label.label("L2")).setProperty("p2", "value");
            tx.createNode(Label.label("L1")).setProperty("p1", "value");
            tx.createNode(Label.label("L1")).setProperty("p2", "value");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeIndexWithTextArrayProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("Label"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.createNode(Label.label("Label")).setProperty("prop", array("value1", "value2"));
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeIndexWithEmptyTextArrayProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("Label"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.createNode(Label.label("Label")).setProperty("prop", EMPTY_STRING_ARRAY);
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeIndexWithEmptyTextProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("Label"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.createNode(Label.label("Label")).setProperty("prop", "");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeIndexWithMixOfTextAndTextArrayProperties() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("Label"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.createNode(Label.label("Label")).setProperty("prop", "plainValue");
            tx.createNode(Label.label("Label")).setProperty("prop", array("value1", "value2"));
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckRelationshipIndexWithOneRelationshipTypeAndOneProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        RelationshipType relationshipType = RelationshipType.withName("R1");
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("R1"), asPropertiesStrList("p1")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode();
            node.createRelationshipTo(node, relationshipType).setProperty("p1", "value");
            node.createRelationshipTo(node, relationshipType)
                    .setProperty("p1", "value"); // This relationship will have a different id value than the node.
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckRelationshipIndexWithOneRelationshipTypeAndMultipleProperties() throws Exception {
        GraphDatabaseService db = createDatabase();
        RelationshipType relationshipType = RelationshipType.withName("R1");
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("R1"), asPropertiesStrList("p1", "p2")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode();
            Relationship r1 = node.createRelationshipTo(node, relationshipType);
            r1.setProperty("p1", "value");
            r1.setProperty("p2", "value");
            Relationship r2 = node.createRelationshipTo(
                    node, relationshipType); // This relationship will have a different id value than the node.
            r2.setProperty("p1", "value");
            r2.setProperty("p2", "value");
            node.createRelationshipTo(node, relationshipType).setProperty("p1", "value");
            node.createRelationshipTo(node, relationshipType).setProperty("p2", "value");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckRelationshipIndexWithMultipleRelationshipTypesAndOneProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        RelationshipType relType1 = RelationshipType.withName("R1");
        RelationshipType relType2 = RelationshipType.withName("R2");
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("R1", "R2"), asPropertiesStrList("p1")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node n1 = tx.createNode();
            Node n2 = tx.createNode();
            n1.createRelationshipTo(n1, relType1).setProperty("p1", "value");
            n1.createRelationshipTo(n1, relType2).setProperty("p1", "value");
            n2.createRelationshipTo(n2, relType1).setProperty("p1", "value");
            n2.createRelationshipTo(n2, relType2).setProperty("p1", "value");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckRelationshipIndexWithMultipleRelationshipTypesAndMultipleProperties()
            throws Exception {
        GraphDatabaseService db = createDatabase();
        RelationshipType relType1 = RelationshipType.withName("R1");
        RelationshipType relType2 = RelationshipType.withName("R2");
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(
                            FULLTEXT_CREATE,
                            "rels",
                            asRelationshipTypeStr("R1", "R2"),
                            asPropertiesStrList("p1", "p2")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node n1 = tx.createNode();
            Node n2 = tx.createNode();
            Relationship r1 = n1.createRelationshipTo(n1, relType1);
            r1.setProperty("p1", "value");
            r1.setProperty("p2", "value");
            Relationship r2 = n1.createRelationshipTo(n1, relType2);
            r2.setProperty("p1", "value");
            r2.setProperty("p2", "value");
            Relationship r3 = n2.createRelationshipTo(n2, relType1);
            r3.setProperty("p1", "value");
            r3.setProperty("p2", "value");
            Relationship r4 = n2.createRelationshipTo(n2, relType2);
            r4.setProperty("p1", "value");
            r4.setProperty("p2", "value");
            n1.createRelationshipTo(n2, relType1).setProperty("p1", "value");
            n1.createRelationshipTo(n2, relType2).setProperty("p1", "value");
            n1.createRelationshipTo(n2, relType1).setProperty("p2", "value");
            n1.createRelationshipTo(n2, relType2).setProperty("p2", "value");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckRelationshipIndexWithTextArrayProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        RelationshipType relationshipType = RelationshipType.withName("R1");
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("R1"), asPropertiesStrList("p1")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode();
            node.createRelationshipTo(node, relationshipType).setProperty("p1", array("value1", "value2"));
            node.createRelationshipTo(node, relationshipType).setProperty("p1", array("value1", "value2"));
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckRelationshipIndexWithEmptyTextArrayProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        RelationshipType relationshipType = RelationshipType.withName("R1");
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("R1"), asPropertiesStrList("p1")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode();
            node.createRelationshipTo(node, relationshipType).setProperty("p1", EMPTY_STRING_ARRAY);
            node.createRelationshipTo(node, relationshipType).setProperty("p1", EMPTY_STRING_ARRAY);
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckRelationshipIndexWithEmptyTextProperty() throws Exception {
        GraphDatabaseService db = createDatabase();
        RelationshipType relationshipType = RelationshipType.withName("R1");
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("R1"), asPropertiesStrList("p1")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode();
            node.createRelationshipTo(node, relationshipType).setProperty("p1", "");
            node.createRelationshipTo(node, relationshipType).setProperty("p1", "");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckRelationshipIndexWithMixOfTextAndTextArrayProperties() throws Exception {
        GraphDatabaseService db = createDatabase();
        RelationshipType relationshipType = RelationshipType.withName("R1");
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("R1"), asPropertiesStrList("p1")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode();
            node.createRelationshipTo(node, relationshipType).setProperty("p1", "plainValue");
            node.createRelationshipTo(node, relationshipType).setProperty("p1", array("value1", "value2"));
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeAndRelationshipIndexesAtTheSameTime() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(
                            FULLTEXT_CREATE,
                            "nodes",
                            asNodeLabelStr("L1", "L2", "L3"),
                            asPropertiesStrList("p1", "p2")))
                    .close();
            tx.execute(format(
                            FULLTEXT_CREATE,
                            "rels",
                            asRelationshipTypeStr("R1", "R2"),
                            asPropertiesStrList("p1", "p2")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node n1 = tx.createNode(Label.label("L1"), Label.label("L3"));
            n1.setProperty("p1", "value");
            n1.setProperty("p2", "value");
            n1.createRelationshipTo(n1, RelationshipType.withName("R2")).setProperty("p1", "value");
            Node n2 = tx.createNode(Label.label("L2"));
            n2.setProperty("p2", "value");
            Relationship r1 = n2.createRelationshipTo(n2, RelationshipType.withName("R1"));
            r1.setProperty("p1", "value");
            r1.setProperty("p2", "value");
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustBeAbleToConsistencyCheckNodeIndexThatIsMissingNodesBecauseTheirPropertyValuesAreNotStrings()
            throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("L1"), asPropertiesStrList("p1")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.createNode(Label.label("L1")).setProperty("p1", 1);
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void
            mustBeAbleToConsistencyCheckRelationshipIndexThatIsMissingRelationshipsBecauseTheirPropertyValuesAreNotStrings()
                    throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("R1"), asPropertiesStrList("p1")))
                    .close();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode();
            node.createRelationshipTo(node, RelationshipType.withName("R1")).setProperty("p1", 1);
            tx.commit();
        }
        managementService.shutdown();
        assertIsConsistent(checkConsistency());
    }

    @Test
    void consistencyCheckerMustBeAbleToRunOnStoreWithFulltextIndexes() throws Exception {
        GraphDatabaseService db = createDatabase();
        Label[] labels =
                IntStream.range(1, 7).mapToObj(i -> Label.label("LABEL" + i)).toArray(Label[]::new);
        RelationshipType[] relTypes = IntStream.range(1, 5)
                .mapToObj(i -> RelationshipType.withName("REL" + i))
                .toArray(RelationshipType[]::new);
        String[] propertyKeys = IntStream.range(1, 7).mapToObj(i -> "PROP" + i).toArray(String[]::new);
        RandomValues randomValues = random.randomValues();

        try (Transaction tx = db.beginTx()) {
            int nodeCount = 1000;
            List<Node> nodes = new ArrayList<>(nodeCount);
            for (int i = 0; i < nodeCount; i++) {
                Label[] nodeLabels = random.ints(random.nextInt(labels.length), 0, labels.length)
                        .distinct()
                        .mapToObj(x -> labels[x])
                        .toArray(Label[]::new);
                Node node = tx.createNode(nodeLabels);
                Stream.of(propertyKeys)
                        .forEach(p -> node.setProperty(
                                p,
                                random.nextBoolean()
                                        ? p
                                        : randomValues.nextValue().asObject()));
                nodes.add(node);
                int localRelCount = Math.min(nodes.size(), 5);
                random.ints(localRelCount, 0, localRelCount)
                        .distinct()
                        .mapToObj(
                                x -> node.createRelationshipTo(nodes.get(x), relTypes[random.nextInt(relTypes.length)]))
                        .forEach(r -> Stream.of(propertyKeys)
                                .forEach(p -> r.setProperty(
                                        p,
                                        random.nextBoolean()
                                                ? p
                                                : randomValues.nextValue().asObject())));
            }
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            for (int i = 1; i < labels.length; i++) {
                tx.execute(format(
                                FULLTEXT_CREATE,
                                "nodes" + i,
                                asNodeLabelStr(Arrays.stream(labels)
                                        .limit(i)
                                        .map(Label::name)
                                        .toArray(String[]::new)),
                                asPropertiesStrList(Arrays.copyOf(propertyKeys, i))))
                        .close();
            }
            for (int i = 1; i < relTypes.length; i++) {
                tx.execute(format(
                                FULLTEXT_CREATE,
                                "rels" + i,
                                asRelationshipTypeStr(Arrays.stream(relTypes)
                                        .limit(i)
                                        .map(RelationshipType::name)
                                        .toArray(String[]::new)),
                                asPropertiesStrList(Arrays.copyOf(propertyKeys, i))))
                        .close();
            }
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }

        managementService.shutdown();

        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustDiscoverNodeInStoreMissingFromIndex() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("Label"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        IndexDescriptor indexDescriptor;
        long nodeId;
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            indexDescriptor = getFulltextIndexDescriptor(tx.schema().getIndexes());
            Node node = tx.createNode(Label.label("Label"));
            node.setProperty("prop", "value");
            nodeId = getNodeId(tx, node);
            tx.commit();
        }
        IndexingService indexes = getIndexingService(db);
        IndexProxy indexProxy = indexes.getIndexProxy(indexDescriptor);
        try (IndexUpdater updater = indexProxy.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            updater.process(IndexEntryUpdate.remove(nodeId, indexDescriptor, Values.stringValue("value")));
        }

        managementService.shutdown();

        ConsistencyCheckService.Result result = checkConsistency();
        assertFalse(result.isSuccessful());
    }

    @Test
    void mustDiscoverNodeWithTextArrayInStoreMissingFromIndex() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("Label"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        IndexDescriptor indexDescriptor;
        long nodeId;
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            indexDescriptor = getFulltextIndexDescriptor(tx.schema().getIndexes());
            Node node = tx.createNode(Label.label("Label"));
            node.setProperty("prop", array("value1", "value2"));
            nodeId = getNodeId(tx, node);
            tx.commit();
        }
        IndexingService indexes = getIndexingService(db);
        IndexProxy indexProxy = indexes.getIndexProxy(indexDescriptor);
        try (IndexUpdater updater = indexProxy.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            updater.process(IndexEntryUpdate.remove(nodeId, indexDescriptor, Values.stringArray("value1", "value2")));
        }

        managementService.shutdown();

        ConsistencyCheckService.Result result = checkConsistency();
        assertFalse(result.isSuccessful());
    }

    @Test
    public void shouldNotReportNodesWithoutAllPropertiesInIndex() throws Exception {
        // Given
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("L1", "L2"), asPropertiesStrList("p1", "p2")))
                    .close();
            tx.commit();
        }
        // when
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode(Label.label("L1"), Label.label("L2"));
            node.setProperty("p1", "value");
            // skip property p2
            tx.commit();
        }

        managementService.shutdown();
        // then
        assertIsConsistent(checkConsistency());
    }

    @Test
    public void shouldNotReportNodesWithMorePropertiesThanInIndex() throws Exception {
        // Given
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("L1", "L2"), asPropertiesStrList("p1")))
                    .close();
            tx.commit();
        }
        // when
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode(Label.label("L1"), Label.label("L2"));
            node.setProperty("p1", "value");
            node.setProperty("p2", "value"); // p2 does not exist in index
            tx.commit();
        }

        managementService.shutdown();
        // then
        assertIsConsistent(checkConsistency());
    }

    @Test
    void mustDiscoverRelationshipInStoreMissingFromIndex() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("REL"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        IndexDescriptor indexDescriptor;
        long relId;
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            indexDescriptor = getFulltextIndexDescriptor(tx.schema().getIndexes());
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo(node, RelationshipType.withName("REL"));
            rel.setProperty("prop", "value");
            relId = getId(tx, rel);
            tx.commit();
        }
        IndexingService indexes = getIndexingService(db);
        IndexProxy indexProxy = indexes.getIndexProxy(indexDescriptor);
        try (IndexUpdater updater = indexProxy.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            updater.process(IndexEntryUpdate.remove(relId, indexDescriptor, Values.stringValue("value")));
        }

        managementService.shutdown();

        ConsistencyCheckService.Result result = checkConsistency();
        assertFalse(result.isSuccessful());
    }

    @Test
    void mustDiscoverRelationshipWithTextArrayInStoreMissingFromIndex() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("REL"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        IndexDescriptor indexDescriptor;
        long relId;
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            indexDescriptor = getFulltextIndexDescriptor(tx.schema().getIndexes());
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo(node, RelationshipType.withName("REL"));
            rel.setProperty("prop", array("value1", "value2"));
            relId = getId(tx, rel);
            tx.commit();
        }
        IndexingService indexes = getIndexingService(db);
        IndexProxy indexProxy = indexes.getIndexProxy(indexDescriptor);
        try (IndexUpdater updater = indexProxy.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            updater.process(IndexEntryUpdate.remove(relId, indexDescriptor, Values.stringArray("value1", "value2")));
        }

        managementService.shutdown();

        ConsistencyCheckService.Result result = checkConsistency();
        assertFalse(result.isSuccessful());
    }

    @Test
    void mustDiscoverNodeInIndexWithMissingPropertyInStore() throws Exception {
        GraphDatabaseService db = createDatabase();
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "nodes", asNodeLabelStr("Label"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }
        String nodeId;
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.createNode(Label.label("Label"));
            nodeId = node.getElementId();
            tx.commit();
        }

        managementService.shutdown();
        Path copyRoot = testDirectory.directory("cpy");
        copyStoreFiles(copyRoot);
        db = createDatabase();

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            Node node = tx.getNodeByElementId(nodeId);
            node.setProperty("prop", "value");
            tx.commit();
        }
        managementService.shutdown();

        restoreStoreFiles(copyRoot);

        ConsistencyCheckService.Result result = checkConsistency();
        assertFalse(result.isSuccessful());
        assertThat(result.summary().getInconsistencyCountForRecordType("INDEX")).isEqualTo(1);
    }

    @Test
    void mustDiscoverRelationshipInIndexWithMissingPropertyInStore() throws Exception {
        var db = createDatabase();
        try (var tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("REL"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }

        String relId;
        try (var tx = db.beginTx()) {
            final var node = tx.createNode();
            final var rel = node.createRelationshipTo(node, RelationshipType.withName("REL"));
            relId = rel.getElementId();
            tx.commit();
        }

        managementService.shutdown();
        final var copyRoot = testDirectory.directory("cpy");
        copyStoreFiles(copyRoot);

        db = createDatabase();
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);

            tx.getRelationshipByElementId(relId).setProperty("prop", "value");
            tx.commit();
        }
        managementService.shutdown();

        restoreStoreFiles(copyRoot);

        final var result = checkConsistency();
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.summary().getInconsistencyCountForRecordType("INDEX")).isEqualTo(1);
    }

    @Test
    void mustDiscoverRelationshipInIndexWithMissingRelationshipInStore() throws Exception {
        var db = createDatabase();
        try (var tx = db.beginTx()) {
            tx.execute(format(FULLTEXT_CREATE, "rels", asRelationshipTypeStr("REL"), asPropertiesStrList("prop")))
                    .close();
            tx.commit();
        }

        String nodeId;
        try (var tx = db.beginTx()) {
            final var node = tx.createNode();
            nodeId = node.getElementId();
            tx.commit();
        }

        managementService.shutdown();
        final var copyRoot = testDirectory.directory("cpy");
        copyStoreFiles(copyRoot);

        db = createDatabase();
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);

            final var node = tx.getNodeByElementId(nodeId);
            final var rel = node.createRelationshipTo(node, RelationshipType.withName("REL"));
            rel.setProperty("prop", "value");
            tx.commit();
        }
        managementService.shutdown();

        restoreStoreFiles(copyRoot);

        final var result = checkConsistency();
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.summary().getInconsistencyCountForRecordType("INDEX")).isEqualTo(1);
    }

    private static long getNodeId(Transaction tx, Node node) {
        return ((TransactionImpl) tx).elementIdMapper().nodeId(node.getElementId());
    }

    private static long getId(Transaction tx, Relationship rel) {
        return ((TransactionImpl) tx).elementIdMapper().relationshipId(rel.getElementId());
    }

    private void copyStoreFiles(Path into) throws IOException {
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        for (Path storeFile : databaseLayout.mandatoryStoreFiles()) {
            fileSystem.copyFile(storeFile, into.resolve(storeFile.getFileName()));
        }
    }

    private void restoreStoreFiles(Path from) throws IOException {
        testDirectory.getFileSystem().copyRecursively(from, databaseLayout.databaseDirectory());
    }

    private GraphDatabaseAPI createDatabase() {
        managementService = builder.build();
        database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        databaseLayout = database.databaseLayout();
        return database;
    }

    private ConsistencyCheckService.Result checkConsistency() throws ConsistencyCheckIncompleteException {
        Config config = Config.defaults(GraphDatabaseSettings.logs_directory, databaseLayout.databaseDirectory());
        return new ConsistencyCheckService(databaseLayout).with(config).runFullConsistencyCheck();
    }

    private static IndexDescriptor getFulltextIndexDescriptor(Iterable<IndexDefinition> indexes) {
        for (IndexDefinition index : indexes) {
            if (index.getIndexType() == IndexType.FULLTEXT) {
                return getIndexDescriptor(index);
            }
        }
        return IndexDescriptor.NO_INDEX;
    }

    private static IndexDescriptor getIndexDescriptor(IndexDefinition definition) {
        return ((IndexDefinitionImpl) definition).getIndexReference();
    }

    private static IndexingService getIndexingService(GraphDatabaseService db) {
        return getDependencyResolver(db).resolveDependency(IndexingService.class);
    }

    private static DependencyResolver getDependencyResolver(GraphDatabaseService db) {
        return ((GraphDatabaseAPI) db).getDependencyResolver();
    }

    private static void assertIsConsistent(ConsistencyCheckService.Result result) throws IOException {
        if (!result.isSuccessful()) {
            printReport(result);
            fail("Expected consistency check to be successful.");
        }
    }

    private static void printReport(ConsistencyCheckService.Result result) throws IOException {
        Files.readAllLines(result.reportFile()).forEach(System.err::println);
    }
}
