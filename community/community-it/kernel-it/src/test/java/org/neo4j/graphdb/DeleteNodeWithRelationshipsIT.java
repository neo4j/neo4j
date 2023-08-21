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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
@ImpermanentDbmsExtension
class DeleteNodeWithRelationshipsIT {
    @Inject
    private GraphDatabaseService db;

    @Test
    void shouldGiveHelpfulExceptionWhenDeletingNodeWithRelationships() {
        // Given
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            node.createRelationshipTo(tx.createNode(), RelationshipType.withName("MAYOR_OF"));
            tx.commit();
        }

        // And given a transaction deleting just the node
        Transaction tx = db.beginTx();
        tx.getNodeById(node.getId()).delete();

        ConstraintViolationException ex = assertThrows(ConstraintViolationException.class, tx::commit);
        assertEquals(
                "Cannot delete node<" + node.getId() + ">, because it still has relationships. "
                        + "To delete this node, you must first delete its relationships.",
                ex.getMessage());
    }

    @Test
    void shouldDeleteDenseNodeEvenWithTemporarilyCreatedRelationshipsBeforeDeletion() {
        // Given
        long nodeId;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            nodeId = node.getId();
            for (int i = 0; i < 200; i++) {
                node.createRelationshipTo(tx.createNode(), RelationshipType.withName("TYPE_" + i % 3));
            }
            tx.commit();
        }

        // When
        try (Transaction tx = db.beginTx()) {
            Node node = tx.getNodeById(nodeId);
            // Create temporary relationships of new types, which will be deleted right afterwards
            node.createRelationshipTo(tx.createNode(), RelationshipType.withName("OTHER_TYPE_1"));
            node.createRelationshipTo(tx.createNode(), RelationshipType.withName("OTHER_TYPE_2"));
            Iterables.forEach(node.getRelationships(), Relationship::delete);
            node.delete();
            tx.commit();
        }

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThrows(NotFoundException.class, () -> tx.getNodeById(nodeId));
            tx.commit();
        }
    }
}
