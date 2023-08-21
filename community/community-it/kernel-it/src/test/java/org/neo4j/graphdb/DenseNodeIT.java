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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterables.single;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class DenseNodeIT {
    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private Config config;

    @Test
    void testBringingNodeOverDenseThresholdIsConsistent() {
        // GIVEN
        Node root;
        try (Transaction tx = db.beginTx()) {
            root = tx.createNode();
            createRelationshipsOnNode(tx, root, 40);
            tx.commit();
        }

        // WHEN
        try (Transaction tx = db.beginTx()) {
            root = tx.getNodeById(root.getId());
            createRelationshipsOnNode(tx, root, 60);

            // THEN
            assertEquals(100, root.getDegree());
            assertEquals(100, root.getDegree(Direction.OUTGOING));
            assertEquals(0, root.getDegree(Direction.INCOMING));
            assertEquals(25, root.getDegree(RelationshipType.withName("Type0")));
            assertEquals(25, root.getDegree(RelationshipType.withName("Type1")));
            assertEquals(25, root.getDegree(RelationshipType.withName("Type2")));
            assertEquals(25, root.getDegree(RelationshipType.withName("Type3")));
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            root = tx.getNodeById(root.getId());

            assertEquals(100, root.getDegree());
            assertEquals(100, root.getDegree(Direction.OUTGOING));
            assertEquals(0, root.getDegree(Direction.INCOMING));
            assertEquals(25, root.getDegree(RelationshipType.withName("Type0")));
            assertEquals(25, root.getDegree(RelationshipType.withName("Type1")));
            assertEquals(25, root.getDegree(RelationshipType.withName("Type2")));
            assertEquals(25, root.getDegree(RelationshipType.withName("Type3")));
            tx.commit();
        }
    }

    @Test
    void deletingRelationshipsFromDenseNodeIsConsistent() {
        // GIVEN
        Node root;
        try (Transaction tx = db.beginTx()) {
            root = tx.createNode();
            createRelationshipsOnNode(tx, root, 100);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            root = tx.getNodeById(root.getId());
            deleteRelationshipsFromNode(root, 80);

            assertEquals(20, root.getDegree());
            assertEquals(20, root.getDegree(Direction.OUTGOING));
            assertEquals(0, root.getDegree(Direction.INCOMING));
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            root = tx.getNodeById(root.getId());
            assertEquals(20, root.getDegree());
            assertEquals(20, root.getDegree(Direction.OUTGOING));
            assertEquals(0, root.getDegree(Direction.INCOMING));
            tx.commit();
        }
    }

    @Test
    void movingBilaterallyOfTheDenseNodeThresholdIsConsistent() {
        // GIVEN
        Node root;
        // WHEN
        try (Transaction tx = db.beginTx()) {
            root = tx.createNode();
            createRelationshipsOnNode(tx, root, 100);
            deleteRelationshipsFromNode(root, 80);

            assertEquals(20, root.getDegree());
            assertEquals(20, root.getDegree(Direction.OUTGOING));
            assertEquals(0, root.getDegree(Direction.INCOMING));

            tx.commit();
        }

        // THEN
        try (Transaction tx = db.beginTx()) {
            root = tx.getNodeById(root.getId());
            assertEquals(20, root.getDegree());
            assertEquals(20, root.getDegree(Direction.OUTGOING));
            assertEquals(0, root.getDegree(Direction.INCOMING));
            tx.commit();
        }
    }

    @Test
    void testBringingTwoConnectedNodesOverDenseThresholdIsConsistent() {
        // GIVEN
        Node source;
        Node sink;
        try (Transaction tx = db.beginTx()) {
            source = tx.createNode();
            sink = tx.createNode();
            createRelationshipsBetweenNodes(source, sink, 40);
            tx.commit();
        }

        // WHEN
        try (Transaction tx = db.beginTx()) {
            source = tx.getNodeById(source.getId());
            sink = tx.getNodeById(sink.getId());
            createRelationshipsBetweenNodes(source, sink, 60);

            // THEN
            assertEquals(100, source.getDegree());
            assertEquals(100, source.getDegree(Direction.OUTGOING));
            assertEquals(0, source.getDegree(Direction.INCOMING));
            assertEquals(25, source.getDegree(RelationshipType.withName("Type0")));
            assertEquals(25, source.getDegree(RelationshipType.withName("Type1")));
            assertEquals(25, source.getDegree(RelationshipType.withName("Type2")));
            assertEquals(25, source.getDegree(RelationshipType.withName("Type3")));

            assertEquals(100, sink.getDegree());
            assertEquals(0, sink.getDegree(Direction.OUTGOING));
            assertEquals(100, sink.getDegree(Direction.INCOMING));
            assertEquals(25, sink.getDegree(RelationshipType.withName("Type0")));
            assertEquals(25, sink.getDegree(RelationshipType.withName("Type1")));
            assertEquals(25, sink.getDegree(RelationshipType.withName("Type2")));
            assertEquals(25, sink.getDegree(RelationshipType.withName("Type3")));
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            source = tx.getNodeById(source.getId());
            sink = tx.getNodeById(sink.getId());

            assertEquals(100, source.getDegree());
            assertEquals(100, source.getDegree(Direction.OUTGOING));
            assertEquals(0, source.getDegree(Direction.INCOMING));
            assertEquals(25, source.getDegree(RelationshipType.withName("Type0")));
            assertEquals(25, source.getDegree(RelationshipType.withName("Type1")));
            assertEquals(25, source.getDegree(RelationshipType.withName("Type2")));
            assertEquals(25, source.getDegree(RelationshipType.withName("Type3")));

            assertEquals(100, sink.getDegree());
            assertEquals(0, sink.getDegree(Direction.OUTGOING));
            assertEquals(100, sink.getDegree(Direction.INCOMING));
            assertEquals(25, sink.getDegree(RelationshipType.withName("Type0")));
            assertEquals(25, sink.getDegree(RelationshipType.withName("Type1")));
            assertEquals(25, sink.getDegree(RelationshipType.withName("Type2")));
            assertEquals(25, sink.getDegree(RelationshipType.withName("Type3")));
            tx.commit();
        }
    }

    @Test
    void shouldBeAbleToCreateRelationshipsInEmptyDenseNode() {
        // GIVEN
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            createRelationshipsBetweenNodes(node, tx.createNode(), denseNodeThreshold(db) + 1);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            long nodeId = node.getId();
            Iterables.forEach(tx.getNodeById(nodeId).getRelationships(), Relationship::delete);
            tx.commit();
        }

        // WHEN
        Relationship rel;
        try (Transaction tx = db.beginTx()) {
            rel = tx.getNodeById(node.getId()).createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            // THEN
            node = tx.getNodeById(node.getId());
            rel = tx.getRelationshipById(rel.getId());
            assertEquals(rel, single(node.getRelationships()));
            tx.commit();
        }
    }

    private int denseNodeThreshold(GraphDatabaseAPI db) {
        return config.get(GraphDatabaseSettings.dense_node_threshold);
    }

    private static void deleteRelationshipsFromNode(Node root, int numberOfRelationships) {
        int deleted = 0;
        try (ResourceIterable<Relationship> relationships = root.getRelationships()) {
            for (final var relationship : relationships) {
                relationship.delete();
                deleted++;
                if (deleted == numberOfRelationships) {
                    break;
                }
            }
        }
    }

    private static void createRelationshipsOnNode(Transaction tx, Node root, int numberOfRelationships) {
        for (int i = 0; i < numberOfRelationships; i++) {
            root.createRelationshipTo(tx.createNode(), RelationshipType.withName("Type" + (i % 4)))
                    .setProperty("" + i, i);
        }
    }

    private static void createRelationshipsBetweenNodes(Node source, Node sink, int numberOfRelationships) {
        for (int i = 0; i < numberOfRelationships; i++) {
            source.createRelationshipTo(sink, RelationshipType.withName("Type" + (i % 4)))
                    .setProperty("" + i, i);
        }
    }
}
