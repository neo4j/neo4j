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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.newapi.TestUtils.closeWorkContexts;
import static org.neo4j.kernel.impl.newapi.TestUtils.createContexts;
import static org.neo4j.kernel.impl.newapi.TestUtils.createWorkers;
import static org.neo4j.util.concurrent.Futures.getAllResults;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.TestUtils.PartitionedScanAPI;

public abstract class ParallelPartitionedNodeLabelScanCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G> {
    private static final int NUMBER_OF_NODES = 1000;
    private static int FOO_LABEL;
    private static int BAR_LABEL;
    private static LongSet FOO_NODES;
    private static LongSet BAR_NODES;

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        MutableLongSet fooNodes = LongSets.mutable.empty();
        MutableLongSet barNodes = LongSets.mutable.empty();
        try (KernelTransaction tx = beginTransaction()) {
            TokenWrite tokenWrite = tx.tokenWrite();
            FOO_LABEL = tokenWrite.labelGetOrCreateForName("foo");
            BAR_LABEL = tokenWrite.labelGetOrCreateForName("bar");
            Write write = tx.dataWrite();
            for (int i = 0; i < NUMBER_OF_NODES; i++) {
                long node = write.nodeCreate();

                if (i % 2 == 0) {
                    write.nodeAddLabel(node, FOO_LABEL);
                    fooNodes.add(node);
                } else {
                    write.nodeAddLabel(node, BAR_LABEL);
                    barNodes.add(node);
                }
            }

            FOO_NODES = fooNodes;
            BAR_NODES = barNodes;

            tx.commit();
        } catch (KernelException e) {
            throw new AssertionError(e);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldScanASubsetOfFooNodes(PartitionedScanAPI api) throws KernelException {
        try (var statement = tx.acquireStatement();
                var executionContext = tx.createExecutionContext();
                NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            // when
            var tokenReadSession = read.tokenReadSession(tx.schemaRead()
                    .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                    .next());
            PartitionedScan<NodeLabelIndexCursor> scan =
                    read.nodeLabelScan(tokenReadSession, 32, NULL_CONTEXT, new TokenPredicate(FOO_LABEL));

            assertThat(api.reservePartition(scan, nodes, tx, executionContext)).isTrue();
            while (nodes.next()) {
                assertThat(FOO_NODES.contains(nodes.nodeReference())).isTrue();
            }

            executionContext.complete();
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldHandleSinglePartition(PartitionedScanAPI api) throws KernelException {
        try (var statement = tx.acquireStatement();
                var executionContext = tx.createExecutionContext();
                NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            // when
            var tokenReadSession = read.tokenReadSession(tx.schemaRead()
                    .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                    .next());
            PartitionedScan<NodeLabelIndexCursor> scan =
                    read.nodeLabelScan(tokenReadSession, 1, NULL_CONTEXT, new TokenPredicate(FOO_LABEL));

            assertThat(api.reservePartition(scan, nodes, tx, executionContext)).isTrue();

            LongArrayList ids = new LongArrayList();
            while (nodes.next()) {
                ids.add(nodes.nodeReference());
            }

            assertThat(FOO_NODES.containsAll(ids)).isTrue();
            assertThat(BAR_NODES.containsNone(ids)).isTrue();

            executionContext.complete();
        }
    }

    @Test
    void shouldFailOnZeroPartitions() throws KernelException {
        try (var statement = tx.acquireStatement();
                var executionContext = tx.createExecutionContext()) {
            // when
            var tokenReadSession = read.tokenReadSession(tx.schemaRead()
                    .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                    .next());

            assertThrows(
                    IllegalArgumentException.class,
                    () -> read.nodeLabelScan(tokenReadSession, 0, NULL_CONTEXT, new TokenPredicate(FOO_LABEL)));
            executionContext.complete();
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldScanFooNodesInBatchesWithGetNumberOfPartitions(PartitionedScanAPI api) throws KernelException {
        var tokenReadSession = read.tokenReadSession(tx.schemaRead()
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next());
        PartitionedScan<NodeLabelIndexCursor> scan =
                read.nodeLabelScan(tokenReadSession, 32, NULL_CONTEXT, new TokenPredicate(FOO_LABEL));

        LongArrayList ids = new LongArrayList();
        for (int i = 0; i < scan.getNumberOfPartitions(); i++) {
            // when
            try (var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext();
                    NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
                api.reservePartition(scan, nodes, tx, executionContext);
                while (nodes.next()) {
                    ids.add(nodes.nodeReference());
                }
                executionContext.complete();
            }
        }

        assertThat(ids.size()).isEqualTo(FOO_NODES.size());
        assertThat(ids.containsAll(FOO_NODES)).isTrue();
        assertThat(ids.containsNone(BAR_NODES)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldScanFooNodesInBatchesWithoutGetNumberOfPartitions(PartitionedScanAPI api) throws KernelException {
        var tokenReadSession = read.tokenReadSession(tx.schemaRead()
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next());
        PartitionedScan<NodeLabelIndexCursor> scan =
                read.nodeLabelScan(tokenReadSession, 32, NULL_CONTEXT, new TokenPredicate(FOO_LABEL));

        LongArrayList ids = new LongArrayList();
        try (var statement = tx.acquireStatement();
                var executionContext = tx.createExecutionContext();
                NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            while (api.reservePartition(scan, nodes, tx, executionContext)) {
                while (nodes.next()) {
                    ids.add(nodes.nodeReference());
                }
            }
            executionContext.complete();
        }

        assertThat(ids.size()).isEqualTo(FOO_NODES.size());
        assertThat(ids.containsAll(FOO_NODES)).isTrue();
        assertThat(ids.containsNone(BAR_NODES)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldHandleMorePartitionsThanNodes(PartitionedScanAPI api) throws KernelException {
        var tokenReadSession = read.tokenReadSession(tx.schemaRead()
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next());
        PartitionedScan<NodeLabelIndexCursor> scan =
                read.nodeLabelScan(tokenReadSession, 2 * NUMBER_OF_NODES, NULL_CONTEXT, new TokenPredicate(FOO_LABEL));

        LongArrayList ids = new LongArrayList();
        for (int i = 0; i < scan.getNumberOfPartitions(); i++) {
            // when
            try (var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext();
                    NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
                api.reservePartition(scan, nodes, tx, executionContext);
                while (nodes.next()) {
                    ids.add(nodes.nodeReference());
                }
                executionContext.complete();
            }
        }

        assertThat(ids.size()).isEqualTo(FOO_NODES.size());
        assertThat(ids.containsAll(FOO_NODES)).isTrue();
        assertThat(ids.containsNone(BAR_NODES)).isTrue();
    }

    @Test
    void shouldScanFooNodesFromMultipleThreads() throws InterruptedException, ExecutionException, KernelException {
        // given
        var tokenReadSession = read.tokenReadSession(tx.schemaRead()
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next());
        PartitionedScan<NodeLabelIndexCursor> scan =
                read.nodeLabelScan(tokenReadSession, 32, NULL_CONTEXT, new TokenPredicate(FOO_LABEL));
        ExecutorService service = Executors.newFixedThreadPool(scan.getNumberOfPartitions());

        CursorFactory cursors = testSupport.kernelToTest().cursors();
        try {
            var workerContexts =
                    createContexts(tx, cursors::allocateNodeLabelIndexCursor, scan.getNumberOfPartitions());
            var futures = service.invokeAll(createWorkers(scan, workerContexts, NodeIndexCursor::nodeReference));

            List<LongList> ids = getAllResults(futures);
            closeWorkContexts(workerContexts);

            TestUtils.assertDistinct(ids);
            assertThat(FOO_NODES.containsAll(TestUtils.concat(ids))).isTrue();
        } finally {
            service.shutdown();
            assertTrue(service.awaitTermination(1, TimeUnit.MINUTES));
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldBeReadCommitted(PartitionedScanAPI api)
            throws ExecutionException, InterruptedException, TimeoutException, KernelException {
        // given
        MutableLongSet ids = new LongHashSet();
        var tokenReadSession = read.tokenReadSession(tx.schemaRead()
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next());
        PartitionedScan<NodeLabelIndexCursor> scan =
                read.nodeLabelScan(tokenReadSession, 32, NULL_CONTEXT, new TokenPredicate(FOO_LABEL));

        // and some new nodes added after initialization
        LongList newFooNodes = createNodesInSeparateTransaction(5, FOO_LABEL);
        LongList newBarNodes = createNodesInSeparateTransaction(5, BAR_LABEL);

        for (int i = 0; i < scan.getNumberOfPartitions(); i++) {
            // when
            try (var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext();
                    NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
                api.reservePartition(scan, nodes, tx, executionContext);
                while (nodes.next()) {
                    ids.add(nodes.nodeReference());
                }
                executionContext.complete();
            }
        }

        // then
        assertThat(ids.containsAll(newFooNodes)).isTrue();
        assertThat(ids.containsNone(newBarNodes)).isTrue();
        // and clean up
        newFooNodes.forEach((LongProcedure) newNode -> {
            try {
                tx.dataWrite().nodeDelete(newNode);
            } catch (InvalidTransactionTypeKernelException e) {
                throw new AssertionError(e);
            }
        });
        newBarNodes.forEach((LongProcedure) newNode -> {
            try {
                tx.dataWrite().nodeDelete(newNode);
            } catch (InvalidTransactionTypeKernelException e) {
                throw new AssertionError(e);
            }
        });
    }

    private LongList createNodesInSeparateTransaction(int numberOfNodesToCreate, int label)
            throws ExecutionException, InterruptedException, TimeoutException {
        ExecutorService service = Executors.newSingleThreadExecutor();
        var futureList = service.submit((Callable<LongList>) () -> {
            var newNodes = new LongArrayList(numberOfNodesToCreate);

            try (var tx = testSupport
                    .kernelToTest()
                    .beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
                for (int i = 0; i < numberOfNodesToCreate; i++) {
                    long newNode = tx.dataWrite().nodeCreate();
                    tx.dataWrite().nodeAddLabel(newNode, label);
                    newNodes.add(newNode);
                }
                tx.commit();
            }
            return newNodes;
        });

        return futureList.get(1, TimeUnit.MINUTES);
    }
}
