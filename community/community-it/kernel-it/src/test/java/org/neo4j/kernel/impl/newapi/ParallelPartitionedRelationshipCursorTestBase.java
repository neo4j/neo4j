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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.WorkerContext;
import org.neo4j.kernel.impl.newapi.TestUtils.PartitionedScanAPI;

public abstract class ParallelPartitionedRelationshipCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G> {
    private static LongList RELATIONSHIPS;
    private static final int NUMBER_OF_RELATIONSHIPS = 128;
    private static final ToLongFunction<RelationshipScanCursor> REL_GET = RelationshipScanCursor::relationshipReference;

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            MutableLongList list = new LongArrayList(NUMBER_OF_RELATIONSHIPS);
            for (int i = 0; i < NUMBER_OF_RELATIONSHIPS; i++) {
                list.add(tx.createNode()
                        .createRelationshipTo(tx.createNode(), RelationshipType.withName("R"))
                        .getId());
            }
            RELATIONSHIPS = list;
            tx.commit();
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldScanASubsetOfRelationships(PartitionedScanAPI api) {
        var cursorContext = tx.cursorContext();
        try (var statement = tx.acquireStatement();
                var executionContext = tx.createExecutionContext();
                var relationships = cursors.allocateRelationshipScanCursor(cursorContext)) {
            var scan = read.allRelationshipsScan(64, NULL_CONTEXT);
            // Iterate over one subset/batch/partition. Given the test graph this should
            // have some but not all relationships. Exact numbers will depend on if the storage
            // engine stores relationships as independent entities or embeds relationships on nodes.
            api.reservePartition(scan, relationships, tx, executionContext);
            var ids = new LongArrayList();
            while (relationships.next()) {
                ids.add(relationships.relationshipReference());
            }
            assertTrue(ids.size() > 0);
            assertTrue(ids.size() < RELATIONSHIPS.size());
            assertTrue(RELATIONSHIPS.containsAll(ids));

            executionContext.complete();
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldHandleSinglePartition(PartitionedScanAPI api) {
        CursorContext cursorContext = tx.cursorContext();
        try (var statement = tx.acquireStatement();
                var executionContext = tx.createExecutionContext();
                RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(cursorContext)) {
            // when
            PartitionedScan<RelationshipScanCursor> scan = read.allRelationshipsScan(1, NULL_CONTEXT);
            assertTrue(api.reservePartition(scan, relationships, tx, executionContext));

            LongArrayList ids = new LongArrayList();
            while (relationships.next()) {
                ids.add(relationships.relationshipReference());
            }

            assertEquals(RELATIONSHIPS, ids);

            executionContext.complete();
        }
    }

    @Test
    void shouldFailOnZeroPartitions() {
        assertThrows(IllegalArgumentException.class, () -> read.allRelationshipsScan(0, NULL_CONTEXT));
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldScanAllRelationshipsInBatchesWithGetNumberOfPartitions(PartitionedScanAPI api) {
        // given
        LongArrayList ids = new LongArrayList();
        PartitionedScan<RelationshipScanCursor> scan = read.allRelationshipsScan(10, NULL_CONTEXT);

        for (int i = 0; i < scan.getNumberOfPartitions(); i++) {
            // when
            try (var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext();
                    RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(NULL_CONTEXT)) {
                api.reservePartition(scan, relationships, tx, executionContext);
                {
                    while (relationships.next()) {
                        ids.add(relationships.relationshipReference());
                    }
                }

                executionContext.complete();
            }
        }

        // then
        assertEquals(RELATIONSHIPS, ids);
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldScanAllRelationshipsInBatchesWithoutGetNumberOfPartitions(PartitionedScanAPI api) {
        // given
        PartitionedScan<RelationshipScanCursor> scan = read.allRelationshipsScan(10, NULL_CONTEXT);
        LongArrayList ids = new LongArrayList();

        try (var statement = tx.acquireStatement();
                var executionContext = tx.createExecutionContext();
                RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(NULL_CONTEXT)) {
            while (api.reservePartition(scan, relationships, tx, executionContext)) {
                while (relationships.next()) {
                    ids.add(relationships.relationshipReference());
                }
            }

            executionContext.complete();
            // when
        }

        // then
        assertEquals(RELATIONSHIPS, ids);
    }

    @ParameterizedTest
    @EnumSource(PartitionedScanAPI.class)
    void shouldHandleMorePartitionsThanRelationships(PartitionedScanAPI api) {
        // given
        LongArrayList ids = new LongArrayList();
        PartitionedScan<RelationshipScanCursor> scan =
                read.allRelationshipsScan(2 * NUMBER_OF_RELATIONSHIPS, NULL_CONTEXT);

        for (int i = 0; i < scan.getNumberOfPartitions(); i++) {
            // when
            try (var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext();
                    RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(NULL_CONTEXT)) {
                api.reservePartition(scan, relationships, tx, executionContext);
                {
                    while (relationships.next()) {
                        ids.add(relationships.relationshipReference());
                    }
                }

                executionContext.complete();
            }
        }

        // then
        assertEquals(RELATIONSHIPS, ids);
    }

    @Test
    void shouldScanAllRelationshipsFromMultipleThreads() throws InterruptedException, ExecutionException {
        // given
        PartitionedScan<RelationshipScanCursor> scan = read.allRelationshipsScan(4, NULL_CONTEXT);
        ExecutorService service = Executors.newFixedThreadPool(scan.getNumberOfPartitions());
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        try {
            var workerContexts =
                    createContexts(tx, cursors::allocateRelationshipScanCursor, scan.getNumberOfPartitions());
            var futures = service.invokeAll(
                    createWorkers(scan, workerContexts, RelationshipScanCursor::relationshipReference));

            List<LongList> ids = getAllResults(futures);
            closeWorkContexts(workerContexts);

            TestUtils.assertDistinct(ids);
            assertEquals(RELATIONSHIPS, TestUtils.concat(ids).toSortedList());
        } finally {
            service.shutdown();
            assertTrue(service.awaitTermination(1, TimeUnit.MINUTES));
        }
    }

    @Test
    void shouldHandleRandomNumberOfPartitions() throws InterruptedException, ExecutionException {
        // given
        int desiredNumberOfPartitions = ThreadLocalRandom.current().nextInt(NUMBER_OF_RELATIONSHIPS) + 1;
        PartitionedScan<RelationshipScanCursor> scan =
                read.allRelationshipsScan(desiredNumberOfPartitions, NULL_CONTEXT);
        ExecutorService service = Executors.newFixedThreadPool(scan.getNumberOfPartitions());
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        String errorMessage = "Failed with: desiredNumberOfPartitions=" + desiredNumberOfPartitions;

        try {
            List<WorkerContext<RelationshipScanCursor>> workerContexts =
                    createContexts(tx, cursors::allocateRelationshipScanCursor, scan.getNumberOfPartitions());
            List<Future<LongList>> futures = service.invokeAll(createWorkers(scan, workerContexts, REL_GET));

            service.shutdown();
            assertTrue(service.awaitTermination(1, TimeUnit.MINUTES), errorMessage);

            // then
            List<LongList> lists = getAllResults(futures);
            closeWorkContexts(workerContexts);

            assertDistinct(lists, errorMessage);
            LongList concat = concat(lists).toSortedList();
            assertEquals(RELATIONSHIPS, concat, errorMessage);
        } finally {
            service.shutdown();
            assertTrue(service.awaitTermination(1, TimeUnit.MINUTES), errorMessage);
        }
    }
}
