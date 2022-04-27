/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.WorkerContext;

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

    @Test
    void shouldScanASubsetOfRelationships() {
        CursorContext cursorContext = tx.cursorContext();
        try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(cursorContext)) {
            // when
            PartitionedScan<RelationshipScanCursor> scan = read.allRelationshipsScan(32, NULL_CONTEXT);
            assertTrue(scan.reservePartition(
                    relationships, NULL_CONTEXT, tx.securityContext().mode()));

            assertTrue(relationships.next());
            assertEquals(RELATIONSHIPS.get(0), relationships.relationshipReference());
            assertTrue(relationships.next());
            assertEquals(RELATIONSHIPS.get(1), relationships.relationshipReference());
            assertTrue(relationships.next());
            assertEquals(RELATIONSHIPS.get(2), relationships.relationshipReference());
            assertTrue(relationships.next());
            assertEquals(RELATIONSHIPS.get(3), relationships.relationshipReference());
            assertFalse(relationships.next());
        }
    }

    @Test
    void shouldHandleSinglePartition() {
        CursorContext cursorContext = tx.cursorContext();
        try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(cursorContext)) {
            // when
            PartitionedScan<RelationshipScanCursor> scan = read.allRelationshipsScan(1, NULL_CONTEXT);
            assertTrue(scan.reservePartition(
                    relationships, NULL_CONTEXT, tx.securityContext().mode()));

            LongArrayList ids = new LongArrayList();
            while (relationships.next()) {
                ids.add(relationships.relationshipReference());
            }

            assertEquals(RELATIONSHIPS, ids);
        }
    }

    @Test
    void shouldFailOnZeroPartitions() {
        assertThrows(IllegalArgumentException.class, () -> read.allRelationshipsScan(0, NULL_CONTEXT));
    }

    @Test
    void shouldScanAllRelationshipsInBatchesWithGetNumberOfPartitions() {
        // given
        LongArrayList ids = new LongArrayList();
        PartitionedScan<RelationshipScanCursor> scan = read.allRelationshipsScan(10, NULL_CONTEXT);

        for (int i = 0; i < scan.getNumberOfPartitions(); i++) {
            // when
            try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(NULL_CONTEXT)) {
                scan.reservePartition(
                        relationships, NULL_CONTEXT, tx.securityContext().mode());
                {
                    while (relationships.next()) {
                        ids.add(relationships.relationshipReference());
                    }
                }
            }
        }

        // then
        assertEquals(RELATIONSHIPS, ids);
    }

    @Test
    void shouldScanAllRelationshipsInBatchesWithoutGetNumberOfPartitions() {
        // given
        PartitionedScan<RelationshipScanCursor> scan = read.allRelationshipsScan(10, NULL_CONTEXT);
        LongArrayList ids = new LongArrayList();

        try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(NULL_CONTEXT)) {
            while (scan.reservePartition(
                    relationships, NULL_CONTEXT, tx.securityContext().mode())) {
                while (relationships.next()) {
                    ids.add(relationships.relationshipReference());
                }
            }
            // when
        }

        // then
        assertEquals(RELATIONSHIPS, ids);
    }

    @Test
    void shouldHandleMorePartitionsThanRelationships() {
        // given
        LongArrayList ids = new LongArrayList();
        PartitionedScan<RelationshipScanCursor> scan =
                read.allRelationshipsScan(2 * NUMBER_OF_RELATIONSHIPS, NULL_CONTEXT);

        for (int i = 0; i < scan.getNumberOfPartitions(); i++) {
            // when
            try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(NULL_CONTEXT)) {
                scan.reservePartition(
                        relationships, NULL_CONTEXT, tx.securityContext().mode());
                {
                    while (relationships.next()) {
                        ids.add(relationships.relationshipReference());
                    }
                }
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
        int desiredNumberOfPartitions = ThreadLocalRandom.current().nextInt(NUMBER_OF_RELATIONSHIPS);
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
