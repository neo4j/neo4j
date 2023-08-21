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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.kernel.impl.newapi.TestUtils.assertDistinct;
import static org.neo4j.kernel.impl.newapi.TestUtils.closeWorkContexts;
import static org.neo4j.kernel.impl.newapi.TestUtils.concat;
import static org.neo4j.kernel.impl.newapi.TestUtils.createContexts;
import static org.neo4j.kernel.impl.newapi.TestUtils.createRandomWorkers;
import static org.neo4j.kernel.impl.newapi.TestUtils.createWorkers;
import static org.neo4j.util.concurrent.Futures.getAllResults;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.WorkerContext;

public abstract class ParallelNodeLabelScanTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G> {
    private static final int NUMBER_OF_NODES = 1000;
    private static int FOO_LABEL;
    private static int BAR_LABEL;
    private static LongSet FOO_NODES;
    private static LongSet BAR_NODES;
    private static final int[] ALL_LABELS = new int[] {FOO_LABEL, BAR_LABEL};
    private static final ToLongFunction<NodeLabelIndexCursor> NODE_GET = NodeLabelIndexCursor::nodeReference;

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

    @Test
    void shouldScanASubsetOfNodes() {
        CursorContext cursorContext = tx.cursorContext();
        try (NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor(cursorContext)) {
            for (int label : ALL_LABELS) {
                Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan(label);
                assertTrue(scan.reserveBatch(
                        nodes, 11, cursorContext, tx.securityContext().mode()));

                MutableLongList found = LongLists.mutable.empty();
                while (nodes.next()) {
                    found.add(nodes.nodeReference());
                }

                assertThat(found.size()).isGreaterThan(0);

                if (label == FOO_LABEL) {

                    assertTrue(FOO_NODES.containsAll(found));
                    assertTrue(found.noneSatisfy(f -> BAR_NODES.contains(f)));
                } else if (label == BAR_LABEL) {
                    assertTrue(BAR_NODES.containsAll(found));
                    assertTrue(found.noneSatisfy(f -> FOO_NODES.contains(f)));
                } else {
                    fail();
                }
            }
        }
    }

    @Test
    void shouldHandleSizeHintOverflow() {
        CursorContext cursorContext = tx.cursorContext();
        try (NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor(tx.cursorContext())) {
            // when
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan(FOO_LABEL);
            assertTrue(scan.reserveBatch(
                    nodes,
                    NUMBER_OF_NODES * 2,
                    cursorContext,
                    tx.securityContext().mode()));

            MutableLongList ids = LongLists.mutable.empty();
            while (nodes.next()) {
                ids.add(nodes.nodeReference());
            }

            assertEquals(FOO_NODES.size(), ids.size());
            assertTrue(FOO_NODES.containsAll(ids));
            assertTrue(ids.noneSatisfy(f -> BAR_NODES.contains(f)));
        }
    }

    @Test
    void shouldFailForSizeHintZero() {
        CursorContext cursorContext = tx.cursorContext();
        try (NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor(cursorContext)) {
            // given
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan(FOO_LABEL);

            // when
            assertThrows(
                    IllegalArgumentException.class,
                    () -> scan.reserveBatch(
                            nodes, 0, cursorContext, tx.securityContext().mode()));
        }
    }

    @Test
    void shouldScanAllNodesInBatches() {
        // given
        CursorContext cursorContext = tx.cursorContext();
        try (NodeLabelIndexCursor nodes = cursors.allocateNodeLabelIndexCursor(cursorContext)) {
            // when
            Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan(FOO_LABEL);
            var ids = new ArrayList<Long>();
            while (scan.reserveBatch(
                    nodes, 3, cursorContext, tx.securityContext().mode())) {
                while (nodes.next()) {
                    ids.add(nodes.nodeReference());
                }
            }

            // then
            assertThat(ids).containsExactlyInAnyOrderElementsOf(FOO_NODES.collect(Long::valueOf));
        }
    }

    @Test
    void shouldScanAllNodesFromMultipleThreads() throws InterruptedException, ExecutionException {
        // given
        int numberOfWorkers = 4;
        int sizeHint = NUMBER_OF_NODES / numberOfWorkers;
        ExecutorService service = Executors.newFixedThreadPool(numberOfWorkers);
        Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan(BAR_LABEL);
        try {
            // when
            List<WorkerContext<NodeLabelIndexCursor>> workers =
                    createContexts(tx, c -> cursors.allocateNodeLabelIndexCursor(c), numberOfWorkers);
            List<Future<LongList>> futures =
                    service.invokeAll(createWorkers(sizeHint, scan, numberOfWorkers, workers, NODE_GET));

            // then
            List<LongList> results = getAllResults(futures);
            closeWorkContexts(workers);

            LongList[] longListsArray = results.toArray(LongList[]::new);
            assertDistinct(longListsArray);
            assertEquals(BAR_NODES, LongSets.immutable.withAll(concat(longListsArray)));
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
        Scan<NodeLabelIndexCursor> scan = read.nodeLabelScan(FOO_LABEL);
        try {
            // when
            List<WorkerContext<NodeLabelIndexCursor>> workers =
                    createContexts(tx, c -> cursors.allocateNodeLabelIndexCursor(c), numberOfWorkers);
            List<Future<LongList>> futures =
                    service.invokeAll(createRandomWorkers(scan, numberOfWorkers, workers, NODE_GET));

            // then
            List<LongList> lists = getAllResults(futures);
            closeWorkContexts(workers);

            assertDistinct(lists);
            assertEquals(FOO_NODES, LongSets.immutable.withAll(concat(lists)));
        } finally {
            service.shutdown();
            service.awaitTermination(1, TimeUnit.MINUTES);
        }
    }
}
