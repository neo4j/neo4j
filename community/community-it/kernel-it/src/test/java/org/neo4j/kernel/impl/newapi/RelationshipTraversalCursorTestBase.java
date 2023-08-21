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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.newapi.RelationshipTestSupport.assertCounts;
import static org.neo4j.kernel.impl.newapi.RelationshipTestSupport.count;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.Degrees;

public abstract class RelationshipTraversalCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G> {
    private static long start, end;
    private static RelationshipTestSupport.StartNode sparse, dense;

    private static boolean supportsDirectTraversal() {
        return true;
    }

    private static boolean supportsSparseNodes() {
        return true;
    }

    private static void bareStartAndEnd(GraphDatabaseService graphDb) {
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            tx.createNode();

            Node x = tx.createNode(), y = tx.createNode();
            start = x.getId();
            end = y.getId();
            x.createRelationshipTo(y, withName("GEN"));

            tx.commit();
        }
    }

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        RelationshipTestSupport.someGraph(graphDb);
        bareStartAndEnd(graphDb);

        sparse = RelationshipTestSupport.sparse(graphDb);
        dense = RelationshipTestSupport.dense(graphDb);
    }

    @Test
    void shouldTraverseRelationshipsOfGivenType() {
        // given
        try (NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
                RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            int empty = 0;
            // when
            read.allNodesScan(node);
            while (node.next()) {
                // then
                Degrees degrees = node.degrees(ALL_RELATIONSHIPS);
                boolean none = true;
                for (int type : degrees.types()) {
                    none = false;
                    Sizes degree = new Sizes();
                    node.relationships(relationship, selection(type, BOTH));
                    while (relationship.next()) {
                        assertEquals(
                                type,
                                relationship.type(),
                                "node #" + node.nodeReference() + " relationship has label not part of selection");
                        if (relationship.sourceNodeReference() == node.nodeReference()) {
                            degree.outgoing++;
                        }
                        if (relationship.targetNodeReference() == node.nodeReference()) {
                            degree.incoming++;
                        }
                        degree.total++;
                    }

                    assertNotEquals(0, degree.total, "all");
                    assertEquals(
                            degrees.outgoingDegree(type),
                            degree.outgoing,
                            "node #" + node.nodeReference() + " outgoing");
                    assertEquals(
                            degrees.incomingDegree(type),
                            degree.incoming,
                            "node #" + node.nodeReference() + " incoming");
                    assertEquals(
                            degrees.totalDegree(type),
                            degree.total,
                            "node #" + node.nodeReference() + " all = incoming + outgoing - loop");
                }
                if (none) {
                    empty++;
                }
            }

            // then
            assertEquals(1, empty, "number of empty nodes");
        }
    }

    @Test
    void shouldFollowSpecificRelationship() {
        // given
        try (NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
                RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // when - traversing from start to end
            read.singleNode(start, node);
            assertTrue(node.next(), "access start node");
            int[] types = node.relationshipTypes();
            assertTrue(types.length > 0);
            node.relationships(relationship, selection(types[0], OUTGOING));
            assertTrue(relationship.next(), "access outgoing relationships");

            // then
            assertEquals(start, relationship.sourceNodeReference(), "source node");
            assertEquals(end, relationship.targetNodeReference(), "target node");

            assertEquals(start, relationship.originNodeReference(), "node of origin");
            assertEquals(end, relationship.otherNodeReference(), "neighbouring node");

            assertEquals(types[0], relationship.type(), "relationship should have correct label");

            assertFalse(relationship.next(), "only a single relationship");

            node.relationships(relationship, selection(types[0], INCOMING));
            assertFalse(relationship.next(), "no incoming relationships");

            // when - traversing from end to start
            read.singleNode(end, node);
            assertTrue(node.next(), "access start node");
            types = node.relationshipTypes();
            assertTrue(types.length > 0);
            node.relationships(relationship, selection(types[0], INCOMING));
            assertTrue(relationship.next(), "access incoming relationships");

            // then
            assertEquals(start, relationship.sourceNodeReference(), "source node");
            assertEquals(end, relationship.targetNodeReference(), "target node");

            assertEquals(end, relationship.originNodeReference(), "node of origin");
            assertEquals(start, relationship.otherNodeReference(), "neighbouring node");

            assertEquals(types[0], relationship.type(), "relationship should have correct label");

            assertFalse(relationship.next(), "only a single relationship");

            node.relationships(relationship, selection(types[0], OUTGOING));
            assertFalse(relationship.next(), "no outgoing relationships");
        }
    }

    @Test
    void shouldTraverseSparseNode() throws Exception {
        assumeTrue(supportsSparseNodes() && supportsDirectTraversal());
        traverse(sparse, false);
    }

    @Test
    void shouldTraverseDenseNode() throws Exception {
        assumeTrue(supportsDirectTraversal());
        traverse(dense, false);
    }

    @Test
    void shouldTraverseSparseNodeWithDetachedReferences() throws Exception {
        assumeTrue(supportsSparseNodes());
        traverse(sparse, true);
    }

    @Test
    void shouldTraverseDenseNodeWithDetachedReferences() throws Exception {
        assumeTrue(supportsDirectTraversal());
        traverse(dense, true);
    }

    private void traverse(RelationshipTestSupport.StartNode start, boolean detached) throws KernelException {
        // given
        try (NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
                RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // when
            read.singleNode(start.id, node);
            assertTrue(node.next(), "access node");

            if (detached) {
                read.relationships(start.id, node.relationshipsReference(), ALL_RELATIONSHIPS, relationship);
            } else {
                node.relationships(relationship, ALL_RELATIONSHIPS);
            }

            Map<String, Integer> counts = count(tx, relationship);

            // then
            assertCounts(start.expectedCounts(), counts);
        }
    }

    private static class Sizes {
        int incoming, outgoing, total;
    }
}
