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
        protected String createEntity(GraphDatabaseService db, Map<String, Object> properties, Label label) {
            return createNode(db, properties, label);
        }

        private static String createNode(GraphDatabaseService db, Map<String, Object> properties, Label... labels) {
            try (Transaction tx = db.beginTx()) {
                Node node = tx.createNode(labels);
                String nodeId = node.getElementId();
                properties.forEach(node::setProperty);
                tx.commit();
                return nodeId;
            }
        }

        @Override
        protected Node createEntity(Transaction tx, Label label) {
            return tx.createNode(label);
        }

        @Override
        protected void deleteEntity(Transaction tx, String id) {
            tx.getNodeByElementId(id).delete();
        }

        @Override
        protected Node getEntity(Transaction tx, String id) {
            return tx.getNodeByElementId(id);
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
            String id = createNode(db, map("key0", true, "key1", true));

            createIndex(db, indexType(), token1, "key2");
            Node myNode;
            try (Transaction tx = db.beginTx()) {
                myNode = tx.getNodeByElementId(id);
                myNode.addLabel(token1);
                myNode.setProperty("key2", LONG_STRING);
                myNode.setProperty("key3", LONG_STRING);

                tx.commit();
            }

            try (Transaction transaction = db.beginTx()) {
                myNode = transaction.getNodeByElementId(id);
                // Then
                assertEquals(LONG_STRING, myNode.getProperty("key2"));
                assertEquals(LONG_STRING, myNode.getProperty("key3"));
                assertThat(findEntitiesByTokenAndProperty(transaction, token1, "key2", LONG_STRING))
                        .containsOnly(myNode);
            }
        }

        @Test
        void shouldCorrectlyUpdateIndexesWhenChangingLabelsAndPropertyAtTheSameTime() {
            // Given
            String nodeId = createNode(db, map("name", "Hawking"), token1, token2);
            createIndex(db, indexType(), token1, "name");
            createIndex(db, indexType(), token2, "name");
            createIndex(db, indexType(), token3, "name");

            // When
            try (Transaction tx = db.beginTx()) {
                Node myNode = tx.getNodeByElementId(nodeId);
                myNode.removeLabel(token1);
                myNode.addLabel(token3);
                myNode.setProperty("name", "Einstein");
                tx.commit();
            }

            try (Transaction transaction = db.beginTx()) {
                Node myNode = transaction.getNodeByElementId(nodeId);
                // Then
                assertEquals("Einstein", myNode.getProperty("name"));
                assertThat(myNode.getLabels()).containsOnly(token2, token3);

                assertThat(findEntitiesByTokenAndProperty(transaction, token1, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, token1, "name", "Einstein"))
                        .isEmpty();

                assertThat(findEntitiesByTokenAndProperty(transaction, token2, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, token2, "name", "Einstein"))
                        .containsOnly(myNode);

                assertThat(findEntitiesByTokenAndProperty(transaction, token3, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, token3, "name", "Einstein"))
                        .containsOnly(myNode);
                transaction.commit();
            }
        }

        @Test
        void shouldCorrectlyUpdateIndexesWhenChangingLabelsAndPropertyMultipleTimesAllAtOnce() {
            // Given
            String nodeId = createNode(db, map("name", "Hawking"), token1, token2);
            createIndex(db, indexType(), token1, "name");
            createIndex(db, indexType(), token2, "name");
            createIndex(db, indexType(), token3, "name");

            // When
            try (Transaction tx = db.beginTx()) {
                Node myNode = tx.getNodeByElementId(nodeId);
                myNode.addLabel(token3);
                myNode.setProperty("name", "Einstein");
                myNode.removeLabel(token1);
                myNode.setProperty("name", "Feynman");
                tx.commit();
            }

            try (Transaction transaction = db.beginTx()) {
                Node myNode = transaction.getNodeByElementId(nodeId);
                // Then
                assertEquals("Feynman", myNode.getProperty("name"));
                assertThat(myNode.getLabels()).containsOnly(token2, token3);

                assertThat(findEntitiesByTokenAndProperty(transaction, token1, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, token1, "name", "Einstein"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, token1, "name", "Feynman"))
                        .isEmpty();

                assertThat(findEntitiesByTokenAndProperty(transaction, token2, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, token2, "name", "Einstein"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, token2, "name", "Feynman"))
                        .containsOnly(myNode);

                assertThat(findEntitiesByTokenAndProperty(transaction, token3, "name", "Hawking"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, token3, "name", "Einstein"))
                        .isEmpty();
                assertThat(findEntitiesByTokenAndProperty(transaction, token3, "name", "Feynman"))
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
            String nodeId;
            try (Transaction tx = db.beginTx()) {
                nodeId = tx.createNode().getElementId();
                tx.commit();
            }

            try (Transaction tx = db.beginTx()) {
                Node node = tx.getNodeByElementId(nodeId);
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
