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
package org.neo4j.kernel.impl.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
abstract class RelationshipReadOperationsTest {

    private final String PROPERTY_KEY = "PROPERTY_KEY";

    @Inject
    static GraphDatabaseAPI db;

    @Test
    void testElementId() {
        var type = RelationshipType.withName("type-1");

        String relId;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            relId = node.createRelationshipTo(node, type).getElementId();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Relationship rel = lookupRelationship(tx, relId);
            assertThat(rel.getElementId()).isEqualTo(relId);
        }
    }

    @Test
    void testTypeLookup() {
        var type = RelationshipType.withName("type-1");

        String relId;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            relId = node.createRelationshipTo(node, type).getElementId();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Relationship rel = lookupRelationship(tx, relId);
            assertThat(rel.getType()).isEqualTo(type);
            assertThat(rel.isType(type)).isTrue();
            assertThat(rel.isType(RelationshipType.withName("another"))).isFalse();
        }
    }

    @Test
    void testNodeLookup() {
        String relId;
        String node1Id;
        String node2Id;
        try (Transaction tx = db.beginTx()) {
            Node node1 = tx.createNode();
            node1Id = node1.getElementId();
            Node node2 = tx.createNode();
            node2Id = node2.getElementId();
            relId = node1.createRelationshipTo(node2, RelationshipType.withName("type-1"))
                    .getElementId();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Relationship rel = lookupRelationship(tx, relId);
            assertThat(rel.getStartNode().getElementId()).isEqualTo(node1Id);
            assertThat(rel.getEndNode().getElementId()).isEqualTo(node2Id);
            assertThat(rel.getNodes()).containsExactlyInAnyOrder(rel.getStartNode(), rel.getEndNode());
            assertThat(rel.getOtherNode(rel.getStartNode()).getElementId()).isEqualTo(node2Id);
            assertThat(rel.getOtherNode(rel.getEndNode()).getElementId()).isEqualTo(node1Id);
        }
    }

    @Test
    void testPropertyLookup() {
        var prop1 = "prop1";
        var value1 = "value1";
        var prop2 = "prop2";
        var value2 = "value2";
        var prop3 = "prop3";
        var value3 = "value3";

        String relId;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo(node, RelationshipType.withName("type-1"));
            relId = rel.getElementId();
            rel.setProperty(prop1, value1);
            rel.setProperty(prop2, value2);
            rel.setProperty(prop3, value3);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Relationship rel = lookupRelationship(tx, relId);
            assertThat(rel.getAllProperties())
                    .containsExactlyInAnyOrderEntriesOf(Map.of(prop1, value1, prop2, value2, prop3, value3));
            assertThat(rel.getProperties(prop1, prop3))
                    .containsExactlyInAnyOrderEntriesOf(Map.of(prop1, value1, prop3, value3));
            assertThat(rel.getProperties(prop1, "another", prop3))
                    .containsExactlyInAnyOrderEntriesOf(Map.of(prop1, value1, prop3, value3));
            assertThat(rel.getProperty(prop1)).isEqualTo(value1);
            assertThat(rel.getProperty(prop1, "default value")).isEqualTo(value1);
            assertThat(rel.getProperty("another", "default value")).isEqualTo("default value");
            assertThat(rel.hasProperty(prop1)).isTrue();
            assertThat(rel.hasProperty("another")).isFalse();
            assertThat(rel.getPropertyKeys()).containsExactlyInAnyOrder(prop1, prop2, prop3);
        }
    }

    @Test
    void shouldThrowHumaneExceptionsWhenPropertyDoesNotExist() {
        // Given a database with PROPERTY_KEY in it
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo(node, RelationshipType.withName("type-1"));
            rel.setProperty(PROPERTY_KEY, 1);
            tx.commit();
        }

        String relId;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            relId = node.createRelationshipTo(node, RelationshipType.withName("type-1"))
                    .getElementId();
            tx.commit();
        }

        // When trying to get property from relationship without it
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        Relationship rel = lookupRelationship(tx, relId);
                        rel.getProperty(PROPERTY_KEY);
                    }
                })
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining("No such property, '" + PROPERTY_KEY);
    }

    @Test
    void shouldThrowHumaneExceptionsWhenPropertyKeyDoesNotExist() {
        // Given a database without PROPERTY_KEY in it
        String relId;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            relId = node.createRelationshipTo(node, RelationshipType.withName("type-1"))
                    .getElementId();
            tx.commit();
        }

        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        Relationship rel = lookupRelationship(tx, relId);
                        rel.getProperty(PROPERTY_KEY);
                    }
                })
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining("No such property, '" + PROPERTY_KEY);
    }

    protected abstract Relationship lookupRelationship(Transaction transaction, String id);
}
