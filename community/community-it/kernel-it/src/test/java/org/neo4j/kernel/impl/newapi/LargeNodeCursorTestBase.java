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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.NodeCursor;

public abstract class LargeNodeCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G> {
    private static final List<Long> NODE_IDS = new ArrayList<>();
    private static final int N_NODES = 10000;

    private static final Random RANDOM = new Random();

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        List<Node> deleted = new ArrayList<>();
        try (Transaction tx = graphDb.beginTx()) {
            for (int i = 0; i < N_NODES; i++) {
                Node node = tx.createNode();
                if (RANDOM.nextBoolean()) {
                    NODE_IDS.add(node.getId());
                } else {
                    deleted.add(node);
                }
            }
            tx.commit();
        }

        try (Transaction tx = graphDb.beginTx()) {
            for (Node node : deleted) {
                tx.getNodeById(node.getId()).delete();
            }
            tx.commit();
        }
    }

    @Test
    void shouldScanNodes() {
        // given
        List<Long> ids = new ArrayList<>();
        try (NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            read.allNodesScan(nodes);
            while (nodes.next()) {
                ids.add(nodes.nodeReference());
            }
        }

        // then
        assertEquals(NODE_IDS, ids);
    }

    @Test
    void shouldAccessNodesByReference() {
        // given
        try (NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            for (long id : NODE_IDS) {
                // when
                read.singleNode(id, nodes);

                // then
                assertTrue(nodes.next(), "should access defined node");
                assertEquals(id, nodes.nodeReference(), "should access the correct node");
                assertFalse(nodes.next(), "should only access a single node");
            }
        }
    }
}
