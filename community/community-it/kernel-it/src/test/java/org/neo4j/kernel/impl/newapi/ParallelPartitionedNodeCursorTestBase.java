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
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.newapi.TestUtils.assertDistinct;
import static org.neo4j.kernel.impl.newapi.TestUtils.closeWorkContexts;
import static org.neo4j.kernel.impl.newapi.TestUtils.concat;
import static org.neo4j.kernel.impl.newapi.TestUtils.createContexts;
import static org.neo4j.kernel.impl.newapi.TestUtils.createWorkers;
import static org.neo4j.util.concurrent.Futures.getAllResults;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.ToLongFunction;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.WorkerContext;
import org.neo4j.kernel.impl.newapi.TestUtils.PartitionedScanAPI;

public abstract class ParallelPartitionedNodeCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G> {
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

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldScanASubsetOfNodes(PartitionedScanAPI api) {
        try (var statement = tx.acquireStatement();
                var executionContext = tx.createExecutionContext();
                NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            PartitionedScan<NodeCursor> scan = read.allNodesScan(32, NULL_CONTEXT);
            assertTrue(api.reservePartition(scan, nodes, tx, executionContext));

            assertTrue(nodes.next());
            assertEquals(NODE_IDS.get(0), nodes.nodeReference());
            assertTrue(nodes.next());
            assertEquals(NODE_IDS.get(1), nodes.nodeReference());
            assertTrue(nodes.next());
            assertEquals(NODE_IDS.get(2), nodes.nodeReference());
            assertTrue(nodes.next());
            assertEquals(NODE_IDS.get(3), nodes.nodeReference());
            assertFalse(nodes.next());

            executionContext.complete();
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldHandleSinglePartition(PartitionedScanAPI api) {
        try (var statement = tx.acquireStatement();
                var executionContext = tx.createExecutionContext();
                NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            PartitionedScan<NodeCursor> scan = read.allNodesScan(1, NULL_CONTEXT);
            assertTrue(api.reservePartition(scan, nodes, tx, executionContext));

            LongArrayList ids = new LongArrayList();
            while (nodes.next()) {
                ids.add(nodes.nodeReference());
            }

            assertEquals(NODE_IDS, ids);

            executionContext.complete();
        }
    }

    @Test
    void shouldFailOnZeroPartitions() {
        assertThrows(IllegalArgumentException.class, () -> read.allNodesScan(0, NULL_CONTEXT));
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldScanAllNodesInBatchesWithGetNumberOfPartitions(PartitionedScanAPI api) {
        // given
        LongArrayList ids = new LongArrayList();
        PartitionedScan<NodeCursor> scan = read.allNodesScan(10, NULL_CONTEXT);

        for (int i = 0; i < scan.getNumberOfPartitions(); i++) {
            // when
            try (var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext();
                    NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
                api.reservePartition(scan, nodes, tx, executionContext);
                {
                    while (nodes.next()) {
                        ids.add(nodes.nodeReference());
                    }
                }

                executionContext.complete();
            }
        }

        // then
        assertEquals(NODE_IDS, ids);
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldScanAllNodesInBatchesWithoutGetNumberOfPartitions(PartitionedScanAPI api) {
        // given
        PartitionedScan<NodeCursor> scan = read.allNodesScan(10, NULL_CONTEXT);
        LongArrayList ids = new LongArrayList();

        try (var statement = tx.acquireStatement();
                var executionContext = tx.createExecutionContext();
                NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            while (api.reservePartition(scan, nodes, tx, executionContext)) {
                while (nodes.next()) {
                    ids.add(nodes.nodeReference());
                }
            }
            // when
            executionContext.complete();
        }

        // then
        assertEquals(NODE_IDS, ids);
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldHandleMorePartitionsThanNodes(PartitionedScanAPI api) {
        // given
        LongArrayList ids = new LongArrayList();
        PartitionedScan<NodeCursor> scan = read.allNodesScan(2 * NUMBER_OF_NODES, NULL_CONTEXT);

        for (int i = 0; i < scan.getNumberOfPartitions(); i++) {
            // when
            try (var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext();
                    NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
                api.reservePartition(scan, nodes, tx, executionContext);
                {
                    while (nodes.next()) {
                        ids.add(nodes.nodeReference());
                    }
                }
                executionContext.complete();
            }
        }

        // then
        assertEquals(NODE_IDS, ids);
    }

    @Test
    void shouldScanAllNodesFromMultipleThreads() throws InterruptedException, ExecutionException {
        // given
        PartitionedScan<NodeCursor> scan = read.allNodesScan(4, NULL_CONTEXT);
        ExecutorService service = Executors.newFixedThreadPool(scan.getNumberOfPartitions());
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        try {
            var workerContexts = createContexts(tx, cursors::allocateNodeCursor, scan.getNumberOfPartitions());
            var futures = service.invokeAll(createWorkers(scan, workerContexts, NodeCursor::nodeReference));

            List<LongList> ids = getAllResults(futures);
            closeWorkContexts(workerContexts);

            TestUtils.assertDistinct(ids);
            assertEquals(NODE_IDS, TestUtils.concat(ids).toSortedList());
        } finally {
            service.shutdown();
            assertTrue(service.awaitTermination(1, TimeUnit.MINUTES));
        }
    }

    @Test
    void shouldHandleRandomNumberOfPartitions() throws InterruptedException, ExecutionException {
        // given
        int desiredNumberOfPartitions = ThreadLocalRandom.current().nextInt(NUMBER_OF_NODES) + 1;
        PartitionedScan<NodeCursor> scan = read.allNodesScan(desiredNumberOfPartitions, NULL_CONTEXT);
        ExecutorService service = Executors.newFixedThreadPool(scan.getNumberOfPartitions());
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        String errorMessage = "Failed with: desiredNumberOfPartitions=" + desiredNumberOfPartitions;

        try {
            List<WorkerContext<NodeCursor>> workerContexts =
                    createContexts(tx, cursors::allocateNodeCursor, scan.getNumberOfPartitions());
            List<Future<LongList>> futures = service.invokeAll(createWorkers(scan, workerContexts, NODE_GET));

            service.shutdown();
            assertTrue(service.awaitTermination(1, TimeUnit.MINUTES), errorMessage);

            // then
            List<LongList> lists = getAllResults(futures);
            closeWorkContexts(workerContexts);

            assertDistinct(lists, errorMessage);
            LongList concat = concat(lists).toSortedList();
            assertEquals(NODE_IDS, concat, errorMessage);
        } finally {
            service.shutdown();
            assertTrue(service.awaitTermination(1, TimeUnit.MINUTES), errorMessage);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldBeReadCommitted(PartitionedScanAPI api)
            throws ExecutionException, InterruptedException, TimeoutException {
        // given
        MutableLongSet ids = new LongHashSet();
        PartitionedScan<NodeCursor> scan = read.allNodesScan(10, NULL_CONTEXT);

        // and some new nodes added after initialization
        LongList newNodes = createNodesInSeparateTransaction(5);
        for (int i = 0; i < scan.getNumberOfPartitions(); i++) {
            // when
            try (var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext();
                    NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
                api.reservePartition(scan, nodes, tx, executionContext);
                {
                    while (nodes.next()) {
                        ids.add(nodes.nodeReference());
                    }
                }

                executionContext.complete();
            }
        }

        // then
        newNodes.forEach((LongProcedure) newNode -> assertTrue(ids.contains(newNode)));
        // and clean up
        newNodes.forEach((LongProcedure) newNode -> {
            try {
                tx.dataWrite().nodeDelete(newNode);
            } catch (InvalidTransactionTypeKernelException e) {
                throw new AssertionError(e);
            }
        });
    }

    private LongList createNodesInSeparateTransaction(int numberOfNodesToCreate)
            throws ExecutionException, InterruptedException, TimeoutException {
        ExecutorService service = Executors.newSingleThreadExecutor();
        var futureList = service.submit((Callable<LongList>) () -> {
            var newNodes = new LongArrayList(numberOfNodesToCreate);

            try (var tx = testSupport
                    .kernelToTest()
                    .beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
                for (int i = 0; i < numberOfNodesToCreate; i++) {
                    newNodes.add(tx.dataWrite().nodeCreate());
                }
                tx.commit();
            }
            return newNodes;
        });

        return futureList.get(1, TimeUnit.MINUTES);
    }
}
