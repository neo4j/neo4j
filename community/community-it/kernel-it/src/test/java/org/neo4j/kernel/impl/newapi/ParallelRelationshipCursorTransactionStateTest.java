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

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.newapi.TestUtils.count;
import static org.neo4j.kernel.impl.newapi.TestUtils.randomBatchWorker;
import static org.neo4j.kernel.impl.newapi.TestUtils.singleBatchWorker;
import static org.neo4j.util.concurrent.Futures.getAllResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.block.procedure.checked.primitive.CheckedLongProcedure;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;

class ParallelRelationshipCursorTransactionStateTest extends KernelAPIWriteTestBase<WriteTestSupport> {
    private static final ToLongFunction<RelationshipScanCursor> REL_GET = RelationshipScanCursor::relationshipReference;

    @Test
    void shouldHandleEmptyDatabase() throws TransactionFailureException {
        try (KernelTransaction tx = beginTransaction()) {
            CursorContext cursorContext = tx.cursorContext();
            try (RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor(cursorContext)) {
                Scan<RelationshipScanCursor> scan = tx.dataRead().allRelationshipsScan();
                while (scan.reserveBatch(
                        cursor, 23, cursorContext, tx.securityContext().mode())) {
                    assertFalse(cursor.next());
                }
            }
        }
    }

    @Test
    void scanShouldNotSeeDeletedRelationships() throws Exception {
        int size = 100;
        MutableLongSet created = LongSets.mutable.empty();
        MutableLongSet deleted = LongSets.mutable.empty();
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            for (int i = 0; i < size; i++) {
                created.add(write.relationshipCreate(write.nodeCreate(), type, write.nodeCreate()));
                deleted.add(write.relationshipCreate(write.nodeCreate(), type, write.nodeCreate()));
            }
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            deleted.each(new CheckedLongProcedure() {
                @Override
                public void safeValue(long item) throws Exception {
                    tx.dataWrite().relationshipDelete(item);
                }
            });

            CursorContext cursorContext = tx.cursorContext();
            try (RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor(cursorContext)) {
                Scan<RelationshipScanCursor> scan = tx.dataRead().allRelationshipsScan();
                MutableLongSet seen = LongSets.mutable.empty();
                while (scan.reserveBatch(
                        cursor, 17, cursorContext, tx.securityContext().mode())) {
                    while (cursor.next()) {
                        long relationshipId = cursor.relationshipReference();
                        assertTrue(seen.add(relationshipId));
                        assertTrue(created.remove(relationshipId));
                    }
                }

                assertTrue(created.isEmpty());
            }
        }
    }

    @Test
    void scanShouldSeeAddedRelationships() throws Exception {
        int size = 100;
        MutableLongSet existing = createRelationships(size);
        MutableLongSet added = LongSets.mutable.empty();

        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");

            for (int i = 0; i < size; i++) {
                added.add(write.relationshipCreate(write.nodeCreate(), type, write.nodeCreate()));
            }

            CursorContext cursorContext = tx.cursorContext();
            try (RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor(cursorContext)) {
                Scan<RelationshipScanCursor> scan = tx.dataRead().allRelationshipsScan();
                MutableLongSet seen = LongSets.mutable.empty();
                while (scan.reserveBatch(
                        cursor, 17, cursorContext, tx.securityContext().mode())) {
                    while (cursor.next()) {
                        long relationshipId = cursor.relationshipReference();
                        assertTrue(seen.add(relationshipId));
                        assertTrue(existing.remove(relationshipId) || added.remove(relationshipId));
                    }
                }

                // make sure we have seen all relationships
                assertTrue(existing.isEmpty());
                assertTrue(added.isEmpty());
            }
        }
    }

    @Test
    void shouldReserveBatchFromTxState() throws KernelException {
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            for (int i = 0; i < 11; i++) {
                write.relationshipCreate(write.nodeCreate(), type, write.nodeCreate());
            }

            CursorContext cursorContext = tx.cursorContext();
            AccessMode accessMode = tx.securityContext().mode();
            try (RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor(cursorContext)) {
                Scan<RelationshipScanCursor> scan = tx.dataRead().allRelationshipsScan();
                assertTrue(scan.reserveBatch(cursor, 5, cursorContext, accessMode));
                assertEquals(5, count(cursor));
                assertTrue(scan.reserveBatch(cursor, 4, cursorContext, accessMode));
                assertEquals(4, count(cursor));
                assertTrue(scan.reserveBatch(cursor, 6, cursorContext, accessMode));
                assertEquals(2, count(cursor));
                // now we should have fetched all relationships
                while (scan.reserveBatch(cursor, 3, cursorContext, accessMode)) {
                    assertFalse(cursor.next());
                }
            }
        }
    }

    @Test
    void shouldScanAllRelationshipsFromMultipleThreads()
            throws InterruptedException, ExecutionException, KernelException, IOException {
        // given
        int numberOfWorkers = 4;
        ExecutorService service = Executors.newFixedThreadPool(numberOfWorkers);
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        int size = 128;
        LongArrayList ids = new LongArrayList();
        List<AutoCloseable> resources = new ArrayList<>();
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            for (int i = 0; i < size; i++) {
                ids.add(write.relationshipCreate(write.nodeCreate(), type, write.nodeCreate()));
            }

            org.neo4j.internal.kernel.api.Read read = tx.dataRead();
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();

            AccessMode accessMode = tx.securityContext().mode();
            ArrayList<Callable<LongList>> workers = new ArrayList<>(numberOfWorkers);
            for (int i = 0; i < numberOfWorkers; i++) {
                var cursor = cursors.allocateRelationshipScanCursor(NULL_CONTEXT);
                resources.add(cursor);
                workers.add(singleBatchWorker(scan, cursor, NULL_CONTEXT, accessMode, REL_GET, size / numberOfWorkers));
            }

            var futures = service.invokeAll(workers);

            List<LongList> lists = getAllResults(futures);

            TestUtils.assertDistinct(lists);
            LongList concat = TestUtils.concat(lists);
            assertEquals(ids.toSortedList(), concat.toSortedList());
            tx.rollback();
        } finally {
            IOUtils.closeAllUnchecked(resources);
            service.shutdown();
            service.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    @Test
    void shouldScanAllRelationshipsFromMultipleThreadWithBigSizeHints()
            throws InterruptedException, ExecutionException, KernelException, IOException {
        // given
        int numberOfWorkers = 4;
        ExecutorService service = Executors.newFixedThreadPool(numberOfWorkers);
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        int size = 128;
        LongArrayList ids = new LongArrayList();
        List<AutoCloseable> resources = new ArrayList<>();
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            for (int i = 0; i < size; i++) {
                ids.add(write.relationshipCreate(write.nodeCreate(), type, write.nodeCreate()));
            }

            org.neo4j.internal.kernel.api.Read read = tx.dataRead();
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();

            AccessMode accessMode = tx.securityContext().mode();
            ArrayList<Callable<LongList>> workers = new ArrayList<>(numberOfWorkers);
            for (int i = 0; i < numberOfWorkers; i++) {
                var cursor = cursors.allocateRelationshipScanCursor(NULL_CONTEXT);
                resources.add(cursor);
                workers.add(singleBatchWorker(scan, cursor, NULL_CONTEXT, accessMode, REL_GET, size / numberOfWorkers));
            }

            var futures = service.invokeAll(workers);

            List<LongList> lists = getAllResults(futures);
            IOUtils.closeAll(resources);

            TestUtils.assertDistinct(lists);
            LongList concat = TestUtils.concat(lists);
            assertEquals(ids.toSortedList(), concat.toSortedList());
        } finally {
            IOUtils.closeAllUnchecked(resources);
            service.shutdown();
            service.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    @Test
    void shouldScanAllRelationshipFromRandomlySizedWorkers()
            throws InterruptedException, KernelException, ExecutionException, IOException {
        // given
        ExecutorService service = Executors.newFixedThreadPool(4);
        int size = 128;
        LongArrayList ids = new LongArrayList();

        List<AutoCloseable> resources = new ArrayList<>();
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            for (int i = 0; i < size; i++) {
                ids.add(write.relationshipCreate(write.nodeCreate(), type, write.nodeCreate()));
            }

            Read read = tx.dataRead();
            Scan<RelationshipScanCursor> scan = read.allRelationshipsScan();
            CursorFactory cursors = testSupport.kernelToTest().cursors();

            // when
            int numberOfWorkers = 10;
            AccessMode accessMode = tx.securityContext().mode();
            ArrayList<Callable<LongList>> workers = new ArrayList<>(numberOfWorkers);
            for (int i = 0; i < numberOfWorkers; i++) {
                var cursor = cursors.allocateRelationshipScanCursor(NULL_CONTEXT);
                resources.add(cursor);
                workers.add(randomBatchWorker(scan, cursor, NULL_CONTEXT, accessMode, REL_GET));
            }
            var futures = service.invokeAll(workers);

            List<LongList> lists = getAllResults(futures);

            TestUtils.assertDistinct(lists);
            LongList concat = TestUtils.concat(lists);

            assertEquals(ids.toSortedList(), concat.toSortedList());
            tx.rollback();
        } finally {
            IOUtils.closeAllUnchecked(resources);
            service.shutdown();
            service.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    @Test
    void parallelTxStateScanStressTest() throws InterruptedException, KernelException, ExecutionException, IOException {
        LongSet existingRelationships = createRelationships(77);
        int numberOfWorkers = Runtime.getRuntime().availableProcessors();
        ExecutorService threadPool = Executors.newFixedThreadPool(numberOfWorkers);
        CursorFactory cursors = testSupport.kernelToTest().cursors();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        try {
            for (int i = 0; i < 1000; i++) {
                MutableLongSet allRels = LongSets.mutable.withAll(existingRelationships);
                List<AutoCloseable> resources = new ArrayList<>();
                try (KernelTransaction tx = beginTransaction()) {
                    int relationshipsInTx = random.nextInt(100);
                    Write write = tx.dataWrite();
                    int type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
                    for (int j = 0; j < relationshipsInTx; j++) {
                        allRels.add(write.relationshipCreate(write.nodeCreate(), type, write.nodeCreate()));
                    }

                    Scan<RelationshipScanCursor> scan = tx.dataRead().allRelationshipsScan();

                    AccessMode accessMode = tx.securityContext().mode();
                    ArrayList<Callable<LongList>> workers = new ArrayList<>(numberOfWorkers);
                    for (int w = 0; w < numberOfWorkers; w++) {
                        var cursor = cursors.allocateRelationshipScanCursor(NULL_CONTEXT);
                        resources.add(cursor);
                        workers.add(randomBatchWorker(scan, cursor, NULL_CONTEXT, accessMode, REL_GET));
                    }
                    var futures = threadPool.invokeAll(workers);

                    List<LongList> lists = getAllResults(futures);

                    TestUtils.assertDistinct(lists);
                    LongList concat = TestUtils.concat(lists);
                    assertEquals(
                            allRels,
                            LongSets.immutable.withAll(concat),
                            format(
                                    "relationships=%d, seen=%d, all=%d",
                                    relationshipsInTx, concat.size(), allRels.size()));
                    assertEquals(allRels.size(), concat.size(), format("relationships=%d", relationshipsInTx));
                    tx.rollback();
                } finally {
                    IOUtils.closeAllUnchecked(resources);
                }
            }
        } finally {
            threadPool.shutdown();
            threadPool.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    private MutableLongSet createRelationships(int size) throws KernelException {
        MutableLongSet rels = LongSets.mutable.empty();
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            for (int i = 0; i < size; i++) {
                rels.add(write.relationshipCreate(write.nodeCreate(), type, write.nodeCreate()));
            }
            tx.commit();
        }
        return rels;
    }

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }
}
