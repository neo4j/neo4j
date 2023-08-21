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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.MathUtil.ceil;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.newapi.TestUtils.assertDistinct;
import static org.neo4j.kernel.impl.newapi.TestUtils.closeWorkContexts;
import static org.neo4j.kernel.impl.newapi.TestUtils.concat;
import static org.neo4j.kernel.impl.newapi.TestUtils.createContexts;
import static org.neo4j.kernel.impl.newapi.TestUtils.createRandomWorkers;
import static org.neo4j.kernel.impl.newapi.TestUtils.createWorkers;
import static org.neo4j.util.concurrent.Futures.getAllResults;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.kernel.api.WorkerContext;

public abstract class ParallelNodeCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G> {
    private static LongList NODE_IDS;
    private static final int NUMBER_OF_NODES = 128;
    private static final ToLongFunction<NodeCursor> NODE_GET = NodeCursor::nodeReference;

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            MutableLongList list = new LongArrayList(NUMBER_OF_NODES);
            for (int i = 0; i < NUMBER_OF_NODES; i++) {
                list.add(tx.createNode().getId());
            }
            NODE_IDS = list;
            tx.commit();
        }
    }

    @Test
    void shouldScanASubsetOfNodes() {
        try (NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            Scan<NodeCursor> scan = read.allNodesScan();
            assertTrue(scan.reserveBatch(
                    nodes, 3, NULL_CONTEXT, tx.securityContext().mode()));

            assertTrue(nodes.next());
            assertEquals(NODE_IDS.get(0), nodes.nodeReference());
            assertTrue(nodes.next());
            assertEquals(NODE_IDS.get(1), nodes.nodeReference());
            assertTrue(nodes.next());
            assertEquals(NODE_IDS.get(2), nodes.nodeReference());
            assertFalse(nodes.next());
        }
    }

    @Test
    void shouldHandleSizeHintLargerThanNumberOfNodes() {
        try (NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            Scan<NodeCursor> scan = read.allNodesScan();
            assertTrue(scan.reserveBatch(
                    nodes,
                    NUMBER_OF_NODES * 2,
                    NULL_CONTEXT,
                    tx.securityContext().mode()));

            LongArrayList ids = new LongArrayList();
            while (nodes.next()) {
                ids.add(nodes.nodeReference());
            }

            assertEquals(NODE_IDS, ids);
        }
    }

    @Test
    void shouldHandleMaxSizeHint() {
        try (NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            Scan<NodeCursor> scan = read.allNodesScan();
            LongArrayList ids = new LongArrayList();

            // scan a quarter
            assertTrue(scan.reserveBatch(
                    nodes,
                    ceil(NUMBER_OF_NODES, 4),
                    NULL_CONTEXT,
                    tx.securityContext().mode()));

            while (nodes.next()) {
                ids.add(nodes.nodeReference());
            }

            // scan the rest
            assertTrue(scan.reserveBatch(
                    nodes, Integer.MAX_VALUE, NULL_CONTEXT, tx.securityContext().mode()));

            while (nodes.next()) {
                ids.add(nodes.nodeReference());
            }

            assertEquals(NODE_IDS, ids);
        }
    }

    @Test
    void shouldFailForSizeHintZero() {
        try (NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // given
            Scan<NodeCursor> scan = read.allNodesScan();

            // when
            assertThrows(
                    IllegalArgumentException.class,
                    () -> scan.reserveBatch(
                            nodes, 0, NULL_CONTEXT, tx.securityContext().mode()));
        }
    }

    @Test
    void shouldScanAllNodesInBatches() {
        // given
        LongArrayList ids = new LongArrayList();
        try (NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            Scan<NodeCursor> scan = read.allNodesScan();
            while (scan.reserveBatch(
                    nodes, 3, NULL_CONTEXT, tx.securityContext().mode())) {
                while (nodes.next()) {
                    ids.add(nodes.nodeReference());
                }
            }
        }

        // then
        assertEquals(NODE_IDS, ids);
    }

    @Test
    void shouldScanAllNodesFromMultipleThreads() throws InterruptedException, ExecutionException {
        // given
        int numberOfWorkers = 4;
        ExecutorService service = Executors.newFixedThreadPool(numberOfWorkers);
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        try {
            var workerContexts = createContexts(tx, cursors::allocateNodeCursor, numberOfWorkers);
            var futures = service.invokeAll(
                    createWorkers(32, scan, numberOfWorkers, workerContexts, NodeCursor::nodeReference));

            List<LongList> ids = getAllResults(futures);
            closeWorkContexts(workerContexts);

            TestUtils.assertDistinct(ids);
            assertEquals(NODE_IDS, TestUtils.concat(ids).toSortedList());
        } finally {
            service.shutdown();
            service.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    @Test
    void shouldScanAllNodesFromMultipleThreadWithBigSizeHints() throws InterruptedException, ExecutionException {
        int numberOfWorkers = 4;
        ExecutorService service = Executors.newFixedThreadPool(numberOfWorkers);
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        try {
            List<WorkerContext<NodeCursor>> workerContexts =
                    createContexts(tx, cursors::allocateNodeCursor, numberOfWorkers);
            List<Future<LongList>> futures =
                    service.invokeAll(createWorkers(100, scan, numberOfWorkers, workerContexts, NODE_GET));

            List<LongList> ids = getAllResults(futures);
            closeWorkContexts(workerContexts);

            TestUtils.assertDistinct(ids);
            assertEquals(NODE_IDS, concat(ids).toSortedList());
        } finally {
            service.shutdown();
            service.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    @Test
    void shouldScanAllNodesFromRandomlySizedWorkers() throws InterruptedException, ExecutionException {
        // given
        int numberOfWorkers = 10;
        ExecutorService service = Executors.newFixedThreadPool(numberOfWorkers);
        Scan<NodeCursor> scan = read.allNodesScan();
        CursorFactory cursors = testSupport.kernelToTest().cursors();

        try {
            List<WorkerContext<NodeCursor>> workerContexts =
                    createContexts(tx, cursors::allocateNodeCursor, numberOfWorkers);
            List<Future<LongList>> futures =
                    service.invokeAll(createRandomWorkers(scan, numberOfWorkers, workerContexts, NODE_GET));

            service.shutdown();
            service.awaitTermination(1, TimeUnit.MINUTES);

            // then
            List<LongList> lists = getAllResults(futures);
            closeWorkContexts(workerContexts);

            assertDistinct(lists);
            LongList concat = concat(lists).toSortedList();
            assertEquals(NODE_IDS, concat);
        } finally {
            service.shutdown();
            service.awaitTermination(1, TimeUnit.MINUTES);
        }
    }
}
