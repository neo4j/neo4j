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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.token.TokenHolders;

public abstract class DeepRelationshipTraversalCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G> {
    private static long three_root;
    private static int expected_total, expected_unique;

    private RelationshipType PARENT = withName("PARENT");
    private int parentRelationshipTypeId;

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            Node root = tx.createNode();
            three_root = root.getId();

            Node[] leafs = new Node[32];
            for (int i = 0; i < leafs.length; i++) {
                leafs[i] = tx.createNode();
            }
            int offset = 0, duplicate = 12;

            Node interdup = tx.createNode();
            interdup.createRelationshipTo(root, PARENT);
            offset = relate(duplicate, leafs, offset, interdup);
            for (int i = 0; i < 5; i++) {
                Node inter = tx.createNode();
                inter.createRelationshipTo(root, PARENT);
                offset = relate(3 + i, leafs, offset, inter);
            }
            interdup.createRelationshipTo(root, PARENT);
            for (int i = 0; i < 4; i++) {
                Node inter = tx.createNode();
                inter.createRelationshipTo(root, PARENT);
                offset = relate(2 + i, leafs, offset, inter);
            }

            Node inter = tx.createNode();
            inter.createRelationshipTo(root, PARENT);
            offset = relate(1, leafs, offset, inter);

            expected_total = offset + duplicate;
            expected_unique = leafs.length;
            parentRelationshipTypeId = ((GraphDatabaseAPI) graphDb)
                    .getDependencyResolver()
                    .resolveDependency(TokenHolders.class)
                    .relationshipTypeTokens()
                    .getIdByName(PARENT.name());

            tx.commit();
        }
    }

    private int relate(int count, Node[] selection, int offset, Node parent) {
        for (int i = 0; i < count; i++) {
            selection[offset++ % selection.length].createRelationshipTo(parent, PARENT);
        }
        return offset;
    }

    @Test
    void shouldTraverseTreeOfDepthThree() {
        try (NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
                RelationshipTraversalCursor relationship1 = cursors.allocateRelationshipTraversalCursor(NULL_CONTEXT);
                RelationshipTraversalCursor relationship2 = cursors.allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            MutableLongSet leafs = new LongHashSet();
            long total = 0;

            // when
            read.singleNode(three_root, node);
            assertTrue(node.next(), "access root node");

            node.relationships(relationship1, selection(parentRelationshipTypeId, Direction.INCOMING));
            while (relationship1.next()) {
                relationship1.otherNode(node);

                assertTrue(node.next(), "child level 1");
                node.relationships(relationship2, selection(parentRelationshipTypeId, Direction.INCOMING));
                while (relationship2.next()) {
                    leafs.add(relationship2.otherNodeReference());
                    total++;
                }
            }

            // then
            assertEquals(expected_total, total, "total number of leaf nodes");
            assertEquals(expected_unique, leafs.size(), "number of distinct leaf nodes");
        }
    }
}
