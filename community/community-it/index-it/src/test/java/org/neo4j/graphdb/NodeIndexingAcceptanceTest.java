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
package org.neo4j.graphdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Iterators;

public class NodeIndexingAcceptanceTest {
    abstract static class NodeIndexingAcceptanceTestBase extends IndexingAcceptanceTestBase<Label, Node> {
        @Override
        protected Label createToken(String name) {
            return Label.label(name);
        }

        @Override
        protected Node createEntity(GraphDatabaseService db, Map<String, Object> properties, Label label) {
            return createNode(db, properties, label);
        }

        private static Node createNode(GraphDatabaseService db, Map<String, Object> properties, Label... labels) {
            try (Transaction tx = db.beginTx()) {
                Node node = tx.createNode(labels);
                properties.forEach(node::setProperty);
                tx.commit();
                return node;
            }
        }

        @Override
        protected Node createEntity(Transaction tx, Label label) {
            return tx.createNode(label);
        }

        @Override
        protected void deleteEntity(Transaction tx, long id) {
            tx.getNodeById(id).delete();
        }

        @Override
        protected Node getEntity(Transaction tx, long id) {
            return tx.getNodeById(id);
        }

        @Override
        protected IndexDefinition createIndex(
                GraphDatabaseService db, IndexType indexType, Label token, String... properties) {
            return SchemaAcceptanceTest.createIndex(db, indexType, token, properties);
        }

        @Override
        protected List<Node> findEntitiesByTokenAndProperty(
                Transaction tx, Label label, String propertyName, Object value) {
            return Iterators.asList(tx.findNodes(label, propertyName, value));
        }

        @Override
        protected ResourceIterator<Node> findEntities(Transaction tx, Label label, String key, Object value) {
            return tx.findNodes(label, key, value);
        }

        @Override
        protected Node findEntity(Transaction tx, Label label, String key, Object value) {
            return tx.findNode(label, key, value);
        }

        @Test
        void shouldUseDynamicPropertiesToIndexANodeWhenAddedAlongsideExistingPropertiesInASeparateTransaction() {
            // When
            long id = createNode(db, map("key0", true, "key1", true)).getId();

            createIndex(db, indexType(), TOKEN1, "key2");
            Node myNode;
            try (Transaction tx = db.beginTx()) {
                myNode = tx.getNodeById(id);
                myNode.addLabel(TOKEN1);
                myNode.setProperty("key2", LONG_STRING);
                myNode.setProperty("key3", LONG_STRING);

                tx.commit();
            }

            try (Transaction transaction = db.beginTx()) {
                myNode = transaction.getNodeById(myNode.getId());
                // Then
                assertEquals(LONG_STRING, myNode.getProperty("key2"));
                assertEquals(LONG_STRING, myNode.getProperty("key3"));
                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "key2", LONG_STRING))
                        .containsOnly(myNode);
            }
        }

        @Test
        void shouldCorrectlyUpdateIndexesWhenChangingLabelsAndPropertyAtTheSameTime() {
            // Given
            var myNode = createNode(db, map("name", "Hawking"), TOKEN1, TOKEN2);
            createIndex(db, indexType(), TOKEN1, "name");
            createIndex(db, indexType(), TOKEN2, "name");
            createIndex(db, indexType(), TOKEN3, "name");

            // When
            try (Transaction tx = db.beginTx()) {
                myNode = tx.getNodeById(myNode.getId());
                myNode.removeLabel(TOKEN1);
                myNode.addLabel(TOKEN3);
                myNode.setProperty("name", "Einstein");
                tx.commit();
            }

            try (Transaction transaction = db.beginTx()) {
                myNode = transaction.getNodeById(myNode.getId());
                // Then
                assertEquals("Einstein", myNode.getProperty("name"));
                assertThat(myNode.getLabels()).containsOnly(TOKEN2, TOKEN3);

                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "name", "Einstein"))
                        .isEmpty();

                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN2, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN2, "name", "Einstein"))
                        .containsOnly(myNode);

                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN3, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN3, "name", "Einstein"))
                        .containsOnly(myNode);
                transaction.commit();
            }
        }

        @Test
        void shouldCorrectlyUpdateIndexesWhenChangingLabelsAndPropertyMultipleTimesAllAtOnce() {
            // Given
            Node myNode = createNode(db, map("name", "Hawking"), TOKEN1, TOKEN2);
            createIndex(db, indexType(), TOKEN1, "name");
            createIndex(db, indexType(), TOKEN2, "name");
            createIndex(db, indexType(), TOKEN3, "name");

            // When
            try (Transaction tx = db.beginTx()) {
                myNode = tx.getNodeById(myNode.getId());
                myNode.addLabel(TOKEN3);
                myNode.setProperty("name", "Einstein");
                myNode.removeLabel(TOKEN1);
                myNode.setProperty("name", "Feynman");
                tx.commit();
            }

            try (Transaction transaction = db.beginTx()) {
                myNode = transaction.getNodeById(myNode.getId());
                // Then
                assertEquals("Feynman", myNode.getProperty("name"));
                assertThat(myNode.getLabels()).containsOnly(TOKEN2, TOKEN3);

                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "name", "Einstein"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "name", "Feynman"))
                        .isEmpty();

                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN2, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN2, "name", "Einstein"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN2, "name", "Feynman"))
                        .containsOnly(myNode);

                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN3, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN3, "name", "Einstein"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN3, "name", "Feynman"))
                        .containsOnly(myNode);
                transaction.commit();
            }
        }

        @Test
        void shouldAddIndexedPropertyToNodeWithDynamicLabels() {
            // Given
            int indexesCount = 20;
            String labelPrefix = "foo";
            String propertyKeyPrefix = "bar";
            String propertyValuePrefix = "baz";

            for (int i = 0; i < indexesCount; i++) {
                createIndex(db, indexType(), createToken(labelPrefix + i), propertyKeyPrefix + i);
            }

            // When
            long nodeId;
            try (Transaction tx = db.beginTx()) {
                nodeId = tx.createNode().getId();
                tx.commit();
            }

            try (Transaction tx = db.beginTx()) {
                Node node = tx.getNodeById(nodeId);
                for (int i = 0; i < indexesCount; i++) {
                    node.addLabel(Label.label(labelPrefix + i));
                    node.setProperty(propertyKeyPrefix + i, propertyValuePrefix + i);
                }
                tx.commit();
            }

            // Then
            try (Transaction tx = db.beginTx()) {
                for (int i = 0; i < indexesCount; i++) {
                    Label label = Label.label(labelPrefix + i);
                    String key = propertyKeyPrefix + i;
                    String value = propertyValuePrefix + i;

                    ResourceIterator<Node> nodes = findEntities(tx, label, key, value);
                    assertEquals(1, Iterators.count(nodes));
                }
                tx.commit();
            }
        }

        @Override
        protected String getMultipleEntitiesMessageTemplate() {
            return "Found multiple nodes with label: '%s', property name: 'name' "
                    + "and property value: 'Stefan' while only one was expected.";
        }
    }

    @Nested
    class RangeIndexTest extends NodeIndexingAcceptanceTestBase {
        @Override
        protected IndexType indexType() {
            return IndexType.RANGE;
        }
    }
}
