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
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.test.PageCacheTracerAssertions.assertThatTracing;
import static org.neo4j.test.PageCacheTracerAssertions.pins;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
abstract class NodeReadOperationsTest {

    private final String PROPERTY_KEY = "PROPERTY_KEY";

    @Inject
    private GraphDatabaseAPI db;

    @Test
    void testNodeRelationshipsLookup() {
        var outgoing1 = RelationshipType.withName("outgoing-1");
        var outgoing2 = RelationshipType.withName("outgoing-2");
        var incoming1 = RelationshipType.withName("incoming-1");

        String node1Id;
        String rel1Id;
        String rel2Id;
        String rel3Id;
        String rel4Id;
        try (Transaction rx = db.beginTx()) {
            Node node1 = rx.createNode();
            Node node2 = rx.createNode();
            Node node3 = rx.createNode();
            node1Id = node1.getElementId();

            rel1Id = node1.createRelationshipTo(node2, outgoing1).getElementId();
            rel2Id = node1.createRelationshipTo(node2, outgoing2).getElementId();
            rel3Id = node2.createRelationshipTo(node1, incoming1).getElementId();
            rel4Id = node1.createRelationshipTo(node3, outgoing1).getElementId();

            rx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node node1 = lookupNode(tx, node1Id);
            assertThat(toIds(node1.getRelationships())).containsExactlyInAnyOrder(rel1Id, rel2Id, rel3Id, rel4Id);
            assertThat(toIds(node1.getRelationships(outgoing1))).containsExactlyInAnyOrder(rel1Id, rel4Id);
            assertThat(toIds(node1.getRelationships(outgoing2))).containsExactlyInAnyOrder(rel2Id);
            assertThat(toIds(node1.getRelationships(incoming1))).containsExactlyInAnyOrder(rel3Id);

            assertThat(toIds(node1.getRelationships(Direction.OUTGOING)))
                    .containsExactlyInAnyOrder(rel1Id, rel2Id, rel4Id);
            assertThat(toIds(node1.getRelationships(Direction.INCOMING))).containsExactlyInAnyOrder(rel3Id);
            assertThat(toIds(node1.getRelationships(Direction.BOTH)))
                    .containsExactlyInAnyOrder(rel1Id, rel2Id, rel3Id, rel4Id);

            assertThat(toIds(node1.getRelationships(Direction.OUTGOING, outgoing1)))
                    .containsExactlyInAnyOrder(rel1Id, rel4Id);
            assertThat(toIds(node1.getRelationships(Direction.INCOMING, outgoing1)))
                    .isEmpty();

            assertThat(node1.hasRelationship()).isTrue();
            assertThat(node1.hasRelationship(Direction.OUTGOING, outgoing1)).isTrue();
            assertThat(node1.hasRelationship(Direction.INCOMING, outgoing1)).isFalse();

            assertThat(node1.getDegree()).isEqualTo(4);
            assertThat(node1.getDegree(outgoing1)).isEqualTo(2);
            assertThat(node1.getDegree(outgoing2)).isEqualTo(1);
            assertThat(node1.getDegree(incoming1)).isEqualTo(1);

            assertThat(node1.getDegree(Direction.OUTGOING)).isEqualTo(3);
            assertThat(node1.getDegree(Direction.INCOMING)).isEqualTo(1);
            assertThat(node1.getDegree(Direction.BOTH)).isEqualTo(4);

            assertThat(node1.getDegree(outgoing1, Direction.OUTGOING)).isEqualTo(2);
            assertThat(node1.getDegree(outgoing1, Direction.INCOMING)).isEqualTo(0);

            assertThat(node1.getSingleRelationship(outgoing2, Direction.OUTGOING)
                            .getElementId())
                    .isEqualTo(rel2Id);
            assertThat(node1.getSingleRelationship(incoming1, Direction.OUTGOING))
                    .isNull();
            assertThatThrownBy(() -> node1.getSingleRelationship(outgoing1, Direction.OUTGOING))
                    .isInstanceOf(NotFoundException.class)
                    .hasMessageContaining("More than one relationship[outgoing-1, OUTGOING] found for Node");

            assertThat(node1.getRelationshipTypes()).containsExactlyInAnyOrder(outgoing1, outgoing2, incoming1);
        }
    }

    @Test
    void testLabelLookup() {
        var label1 = Label.label("label-1");
        var label2 = Label.label("label-2");

        String nodeId;
        try (Transaction rx = db.beginTx()) {
            Node node = rx.createNode();
            nodeId = node.getElementId();
            node.addLabel(label1);
            node.addLabel(label2);

            rx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node node = lookupNode(tx, nodeId);
            assertThat(node.hasLabel(label1)).isTrue();
            assertThat(node.hasLabel(label2)).isTrue();
            assertThat(node.hasLabel(Label.label("another"))).isFalse();

            assertThat(node.getLabels()).containsExactlyInAnyOrder(label1, label2);
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

        String nodeId;
        try (Transaction rx = db.beginTx()) {
            Node node = rx.createNode();
            nodeId = node.getElementId();
            node.setProperty(prop1, value1);
            node.setProperty(prop2, value2);
            node.setProperty(prop3, value3);

            rx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node node = lookupNode(tx, nodeId);
            assertThat(node.getAllProperties())
                    .containsExactlyInAnyOrderEntriesOf(Map.of(prop1, value1, prop2, value2, prop3, value3));
            assertThat(node.getProperties(prop1, prop3))
                    .containsExactlyInAnyOrderEntriesOf(Map.of(prop1, value1, prop3, value3));
            assertThat(node.getProperties(prop1, "another", prop3))
                    .containsExactlyInAnyOrderEntriesOf(Map.of(prop1, value1, prop3, value3));
            assertThat(node.getProperty(prop1)).isEqualTo(value1);
            assertThat(node.getProperty(prop1, "default value")).isEqualTo(value1);
            assertThat(node.getProperty("another", "default value")).isEqualTo("default value");
            assertThat(node.hasProperty(prop1)).isTrue();
            assertThat(node.hasProperty("another")).isFalse();
            assertThat(node.getPropertyKeys()).containsExactlyInAnyOrder(prop1, prop2, prop3);
        }
    }

    @Test
    void shouldThrowHumaneExceptionsWhenPropertyDoesNotExistOnNode() {
        // Given a database with PROPERTY_KEY in it
        createNodeWith(PROPERTY_KEY);

        String nodeId;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            nodeId = node.getElementId();
            tx.commit();
        }

        // When trying to get property from node without it
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        Node node = lookupNode(tx, nodeId);
                        node.getProperty(PROPERTY_KEY);
                    }
                })
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining("No such property, '" + PROPERTY_KEY);
    }

    @Test
    void shouldThrowHumaneExceptionsWhenPropertyKeyDoesNotExist() {
        // Given a database without PROPERTY_KEY in it

        // When
        String nodeId;
        try (Transaction tx = db.beginTx()) {
            nodeId = tx.createNode().getElementId();
            tx.commit();
        }

        // Then
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        Node node = lookupNode(tx, nodeId);
                        node.getProperty(PROPERTY_KEY);
                    }
                })
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining("No such property, '" + PROPERTY_KEY);
    }

    @Test
    void traceNodePageCacheAccessOnDegreeCount() {
        String sourceId;
        try (Transaction tx = db.beginTx()) {
            var source = tx.createNode();
            var relationshipType = RelationshipType.withName("connection");
            createDenseNodeWithShortIncomingChain(tx, source, relationshipType);
            sourceId = source.getElementId();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            var source = lookupNode(tx, sourceId);
            var cursorTracer = reportCursorEventsAndGetTracer(tx);

            source.getDegree(Direction.INCOMING);

            assertThatTracing(db)
                    .record(pins(2).atMost(3).noFaults().skipUnpins())
                    .block(pins(1).atMost(2).noFaults().skipUnpins())
                    .matches(cursorTracer);
        }
    }

    @Test
    void traceNodePageCacheAccessOnRelationshipTypeAndDegreeCount() {
        String sourceId;
        var relationshipType = RelationshipType.withName("connection");
        try (Transaction tx = db.beginTx()) {
            var source = tx.createNode();
            createDenseNodeWithShortIncomingChain(tx, source, relationshipType);
            sourceId = source.getElementId();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            var source = lookupNode(tx, sourceId);
            var cursorTracer = reportCursorEventsAndGetTracer(tx);

            source.getDegree(relationshipType, Direction.INCOMING);

            assertThatTracing(db)
                    .record(pins(2).atMost(3).noFaults().skipUnpins())
                    .block(pins(1).atMost(2).noFaults().skipUnpins())
                    .matches(cursorTracer);
        }
    }

    @Test
    void traceNodePageCacheAccessOnRelationshipsAccess() {
        String targetId;
        var relationshipType = RelationshipType.withName("connection");
        try (Transaction tx = db.beginTx()) {
            var target = tx.createNode();
            for (int i = 0; i < 100; i++) {
                tx.createNode().createRelationshipTo(target, relationshipType);
            }
            targetId = target.getElementId();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            var source = lookupNode(tx, targetId);
            var cursorTracer = reportCursorEventsAndGetTracer(tx);

            assertThat(count(source.getRelationships(Direction.INCOMING, relationshipType)))
                    .isGreaterThan(0);

            assertThatTracing(db)
                    .record(pins(2).atMost(3).noFaults().skipUnpins())
                    .block(pins(1).atMost(2).noFaults().skipUnpins())
                    .matches(cursorTracer);
        }
    }

    private List<String> toIds(ResourceIterable<Relationship> relationships) {
        try (relationships) {
            return relationships.stream().map(Entity::getElementId).collect(Collectors.toList());
        }
    }

    private void createNodeWith(String key) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            node.setProperty(key, 1);
            tx.commit();
        }
    }

    static void assertZeroTracer(CursorContext cursorContext) {
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.hits()).isZero();
        assertThat(cursorTracer.unpins()).isZero();
        assertThat(cursorTracer.pins()).isZero();
    }

    private static void createDenseNodeWithShortIncomingChain(
            Transaction tx, Node source, RelationshipType relationshipType) {
        // This test measures page cache access very specifically when accessing degree for dense node.
        // For dense nodes chain degrees gets "upgraded" to live in a separate degrees store on a certain chain length
        // threshold
        // which is why we create an additional short chain where this still is the case
        for (int i = 0; i < 300; i++) {
            source.createRelationshipTo(tx.createNode(), relationshipType);
        }
        tx.createNode().createRelationshipTo(source, relationshipType);
    }

    abstract PageCursorTracer reportCursorEventsAndGetTracer(Transaction tx);

    protected abstract Node lookupNode(Transaction transaction, String id);
}
