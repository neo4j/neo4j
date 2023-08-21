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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class TestConcurrentIteratorModification {
    @Inject
    private GraphDatabaseService db;

    @Test
    void shouldNotThrowConcurrentModificationExceptionWhenUpdatingWhileIteratingNodes() {
        // given
        Label label = Label.label("Bird");

        Node node1;
        Node node2;
        Node node3;
        try (Transaction tx = db.beginTx()) {
            node1 = tx.createNode(label);
            node2 = tx.createNode(label);
            tx.commit();
        }

        // when
        Set<Node> result = new HashSet<>();
        try (Transaction tx = db.beginTx()) {
            node3 = tx.createNode(label);
            try (ResourceIterator<Node> iterator = tx.findNodes(label)) {
                node3.removeLabel(label);
                tx.createNode(label);
                while (iterator.hasNext()) {
                    result.add(iterator.next());
                }
            }
            tx.commit();
        }

        // then does not throw and retains view from iterator creation time
        assertEquals(asSet(node1, node2, node3), result);
    }

    @Test
    void shouldNotThrowConcurrentModificationExceptionWhenUpdatingWhileIteratingRelationships() {
        // given
        RelationshipType type = RelationshipType.withName("type");

        Relationship rel1;
        Relationship rel2;
        Relationship rel3;
        try (Transaction tx = db.beginTx()) {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            rel1 = node1.createRelationshipTo(node2, type);
            rel2 = node2.createRelationshipTo(node1, type);
            tx.commit();
        }

        // when
        Set<Relationship> result = new HashSet<>();
        try (Transaction tx = db.beginTx()) {
            rel3 = tx.createNode().createRelationshipTo(tx.createNode(), type);
            try (ResourceIterator<Relationship> iterator = tx.findRelationships(type)) {
                rel3.delete();
                tx.createNode().createRelationshipTo(tx.createNode(), type);
                while (iterator.hasNext()) {
                    result.add(iterator.next());
                }
            }
            tx.commit();
        }

        // then does not throw and retains view from iterator creation time
        assertThat(result).containsExactlyInAnyOrder(rel1, rel2, rel3);
    }
}
