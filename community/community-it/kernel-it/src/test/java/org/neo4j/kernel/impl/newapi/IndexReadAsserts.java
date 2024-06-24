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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.neo4j.internal.kernel.api.NodeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.storageengine.api.LongReference;

class IndexReadAsserts {
    static void assertNodes(NodeIndexCursor node, MutableLongSet uniqueIds, long... expected) {
        uniqueIds.clear();
        var expectedIdCount = expected.length;
        while (expectedIdCount-- > 0) {
            assertTrue(node.next(), "at least " + expected.length + " nodes");
            assertTrue(uniqueIds.add(node.nodeReference()));
        }
        assertFalse(node.next(), "no more than " + expected.length + " nodes");
        assertEquals(expected.length, uniqueIds.size(), "all nodes are unique");
        for (long expectedNode : expected) {
            assertTrue(uniqueIds.contains(expectedNode), "expected node " + expectedNode);
        }
    }

    static void assertNodeCount(NodeIndexCursor node, int expectedCount, MutableLongSet uniqueIds) {
        uniqueIds.clear();
        for (int i = 0; i < expectedCount; i++) {
            assertTrue(node.next(), "at least " + expectedCount + " nodes");
            assertTrue(uniqueIds.add(node.nodeReference()));
        }
        assertFalse(node.next(), "no more than " + expectedCount + " nodes");
    }

    static void assertRelationships(RelationshipIndexCursor edge, MutableLongSet uniqueIds, long... expected) {
        uniqueIds.clear();
        var expectedIdCount = expected.length;
        while (expectedIdCount-- > 0) {
            assertTrue(edge.next(), "at least " + expected.length + " relationships");
            final var ref = edge.relationshipReference();
            assertTrue(uniqueIds.add(ref), "The cursor found a duplicate edge: " + ref);
        }
        assertFalse(edge.next(), "no more than " + expected.length + " relationships");
        assertEquals(expected.length, uniqueIds.size(), "all relationships are unique");
        for (var expectedRelationship : expected) {
            assertTrue(uniqueIds.contains(expectedRelationship), "expected relationship " + expectedRelationship);
        }
    }

    static void assertRelationships(
            RelationshipIndexCursor edge, MutableLongSet uniqueIds, IndexOrder order, long... expected) {
        uniqueIds.clear();
        var previousId = LongReference.NULL;
        var expectedIdCount = expected.length;
        while (expectedIdCount-- > 0) {
            assertTrue(edge.next(), "at least " + expected.length + " relationships");
            final var currentId = edge.relationshipReference();
            assertTrue(uniqueIds.add(currentId));
            checkRelationshipOrder(order, previousId, currentId);
            previousId = currentId;
        }

        assertFalse(edge.next(), "no more than " + expected.length + " relationships");
        assertEquals(expected.length, uniqueIds.size(), "all relationships are unique");
        for (var expectedRelationship : expected) {
            assertTrue(uniqueIds.contains(expectedRelationship), "expected relationship " + expectedRelationship);
        }
    }

    static void assertRelationshipCount(RelationshipIndexCursor edge, int edges, MutableLongSet uniqueIds) {
        for (int i = 0; i < edges; i++) {
            assertTrue(edge.next(), "at least " + edges + " relationships");
            assertTrue(uniqueIds.add(edge.relationshipReference()));
        }
        assertFalse(edge.next(), "no more than " + edges + " relationships");
    }

    static void checkRelationshipOrder(IndexOrder expectedOrder, long previousId, long currentId) {
        if (previousId != LongReference.NULL) {
            switch (expectedOrder) {
                case ASCENDING -> assertThat(previousId).isLessThan(currentId);
                case DESCENDING -> assertThat(previousId).isGreaterThan(currentId);
                case NONE -> {}
            }
        }
    }
}
