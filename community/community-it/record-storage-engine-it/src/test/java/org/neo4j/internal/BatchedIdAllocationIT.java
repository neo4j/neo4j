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
package org.neo4j.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.StoreType.LABEL_TOKEN;
import static org.neo4j.kernel.impl.store.StoreType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.StoreType.NODE;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.id.IdCapacityExceededException;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.id.BatchedTransactionIdSequenceProvider;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.concurrent.Futures;

@DbmsExtension
class BatchedIdAllocationIT {
    @Inject
    private RecordStorageEngine storageEngine;

    private NeoStores neoStores;

    @BeforeEach
    void setUp() {
        neoStores = storageEngine.testAccessNeoStores();
    }

    @Test
    void pageBatchedIdAllocation() {
        int nodeRecordsPerPage = neoStores.getNodeStore().getRecordsPerPage();

        var idSequenceProvider1 = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequenceProvider2 = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequenceProvider3 = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequence1 = idSequenceProvider1.getIdSequence(NODE);
        var idSequence2 = idSequenceProvider2.getIdSequence(NODE);
        var idSequence3 = idSequenceProvider3.getIdSequence(NODE);

        assertEquals(0, idSequence1.nextId(NULL_CONTEXT));
        assertEquals(nodeRecordsPerPage, idSequence2.nextId(NULL_CONTEXT));
        assertEquals(2L * nodeRecordsPerPage, idSequence3.nextId(NULL_CONTEXT));

        // whole first batch is here
        for (int i = 1; i < nodeRecordsPerPage; i++) {
            assertEquals(i, idSequence1.nextId(NULL_CONTEXT));
        }
        // whole second batch is here
        for (int i = nodeRecordsPerPage + 1; i < nodeRecordsPerPage * 2; i++) {
            assertEquals(i, idSequence2.nextId(NULL_CONTEXT));
        }
        // whole third batch is here
        for (int i = nodeRecordsPerPage * 2 + 1; i < 3 * nodeRecordsPerPage; i++) {
            assertEquals(i, idSequence3.nextId(NULL_CONTEXT));
        }
    }

    @Test
    void reuseReleasedIdsInNextBatch() {
        var relationshipStore = neoStores.getRelationshipStore();
        var idGenerator = relationshipStore.getIdGenerator();
        int relsPerPage = relationshipStore.getRecordsPerPage();

        var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
        IdSequence idSequence = idSequenceProvider.getIdSequence(StoreType.RELATIONSHIP);
        int freeCounter = 0;
        for (int i = 0; i < relsPerPage; i++) {
            long id = idSequence.nextId(NULL_CONTEXT);
            assertEquals(i, id);
            if ((i & 1) == 0) {
                // delete every second id
                try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                    marker.markUnallocated(i);
                }
                freeCounter++;
            }
        }
        int expectedFreeIds = relsPerPage / 2;
        assertEquals(expectedFreeIds, freeCounter);

        idGenerator.maintenance(NULL_CONTEXT);

        // not we have first page ids available to reuse
        for (int i = 0; i < relsPerPage / 2; i++) {
            long reusedId = idSequence.nextId(NULL_CONTEXT);
            assertEquals(i * 2, reusedId);
        }

        // not we have another new page again
        for (int i = relsPerPage; i < relsPerPage * 2; i++) {
            assertEquals(i, idSequence.nextId(NULL_CONTEXT));
        }
    }

    @Test
    void reuseSeveralPagesOfReleasedIds() {
        var relationshipStore = neoStores.getRelationshipStore();
        var idGenerator = relationshipStore.getIdGenerator();
        int relsPerPage = relationshipStore.getRecordsPerPage();
        int numberOfPageAllocations = 10;

        var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
        IdSequence idSequence = idSequenceProvider.getIdSequence(StoreType.RELATIONSHIP);
        int freeCounter = 0;
        for (int page = 0; page < numberOfPageAllocations; page++) {
            for (int i = page * relsPerPage; i < page * relsPerPage + relsPerPage; i++) {
                assertEquals(i, idSequence.nextId(NULL_CONTEXT));
            }
        }

        for (int i = 0; i < numberOfPageAllocations * relsPerPage; i += 2) {
            // delete every second id
            try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                marker.markUnallocated(i);
            }
            freeCounter++;
        }

        int expectedFreeIds = numberOfPageAllocations * (relsPerPage / 2);
        assertEquals(expectedFreeIds, freeCounter);

        idGenerator.maintenance(NULL_CONTEXT);

        // not we have first N batches of ids that should be half page size in terms of number of ids
        // that are marked as unallocated, and we should be able to reuse those now on our new requests
        for (int page = 0; page < numberOfPageAllocations; page++) {
            for (int i = page * relsPerPage; i < page * relsPerPage + relsPerPage; i += 2) {
                assertEquals(i, idSequence.nextId(NULL_CONTEXT));
            }
        }

        // not we have should allocate new full page of ids
        for (int i = numberOfPageAllocations * relsPerPage;
                i < numberOfPageAllocations * relsPerPage + relsPerPage;
                i++) {
            assertEquals(i, idSequence.nextId(NULL_CONTEXT));
        }
    }

    @Test
    void concurrentPageIdBatchedAllocationAndReleaseGenerateUniqueIds() throws ExecutionException {
        var nodeStore = neoStores.getNodeStore();
        var idGenerator = nodeStore.getIdGenerator();
        int numberOfWorkers = 10;
        int nodesPerWorker = 100_000;
        var totalRemovedEntities = new AtomicLong();
        var perWorkerIds = new CopyOnWriteArrayList<LongSet>();
        var setsFactory = LongSets.mutable;

        var executors = Executors.newFixedThreadPool(numberOfWorkers);
        var futures = new ArrayList<Future<?>>(numberOfWorkers);
        var latch = new CountDownLatch(1);
        try {
            for (int worker = 0; worker < numberOfWorkers; worker++) {
                futures.add(executors.submit(() -> {
                    try {
                        latch.await();
                        var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
                        IdSequence idSequence = idSequenceProvider.getIdSequence(NODE);
                        var workerIds = setsFactory.empty();
                        int freeCounter = 0;

                        for (int i = 0; i < nodesPerWorker; i++) {
                            long id = idSequence.nextId(NULL_CONTEXT);
                            if ((i & 1) == 0) {
                                // unallocate every second id
                                try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                                    marker.markUnallocated(id);
                                }
                                freeCounter++;
                            } else {
                                if (!workerIds.add(id)) {
                                    throw new RuntimeException("Duplicated id allocated in worker. Id: " + id);
                                }
                            }
                        }
                        idGenerator.maintenance(NULL_CONTEXT);

                        totalRemovedEntities.addAndGet(freeCounter);
                        perWorkerIds.add(workerIds);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            latch.countDown();
            Futures.getAll(futures);

            assertEquals(numberOfWorkers * (nodesPerWorker / 2), totalRemovedEntities.get());
            var allocatedIds = setsFactory.withInitialCapacity(nodesPerWorker * numberOfWorkers);
            for (LongSet perWorkerId : perWorkerIds) {
                assertEquals(nodesPerWorker / 2, perWorkerId.size());
                allocatedIds.addAll(perWorkerId);
            }
            assertEquals(
                    numberOfWorkers * (nodesPerWorker / 2),
                    allocatedIds.size(),
                    "Expected number of unique allocated ids.");
        } finally {
            executors.shutdown();
        }
    }

    @Test
    void requestNextIdPageBatchOnBatchEnd() {
        int propertyRecordsPerPage = neoStores.getPropertyStore().getRecordsPerPage();
        int firstAvailablePropertyId = 8;

        var idSequenceProvider1 = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequenceProvider2 = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequence1 = idSequenceProvider1.getIdSequence(PROPERTY);
        var idSequence2 = idSequenceProvider2.getIdSequence(PROPERTY);

        assertEquals(firstAvailablePropertyId, idSequence1.nextId(NULL_CONTEXT));
        assertEquals(propertyRecordsPerPage, idSequence2.nextId(NULL_CONTEXT));

        for (int i = firstAvailablePropertyId + 1; i < propertyRecordsPerPage; i++) {
            assertEquals(i, idSequence1.nextId(NULL_CONTEXT));
        }

        assertEquals(2L * propertyRecordsPerPage, idSequence1.nextId(NULL_CONTEXT));

        // we already switched to next page in allocator above, but we still have ids available in our page
        for (int i = propertyRecordsPerPage + 1; i < 2 * propertyRecordsPerPage; i++) {
            assertEquals(i, idSequence2.nextId(NULL_CONTEXT));
        }
    }

    @Test
    void concurrentPageIdBatchedAllocation() throws ExecutionException {
        int numberOfExecutors = 20;
        long labelTokenPerPage = neoStores.getRecordStore(LABEL_TOKEN_NAME).getRecordsPerPage();
        var uniqueIDs = ConcurrentHashMap.newKeySet((int) (labelTokenPerPage * numberOfExecutors));

        var executors = Executors.newFixedThreadPool(numberOfExecutors);
        try {
            var futures = new ArrayList<Future<?>>(numberOfExecutors);
            CountDownLatch startLatch = new CountDownLatch(1);
            for (int i = 0; i < numberOfExecutors; i++) {
                futures.add(executors.submit(() -> {
                    try {
                        startLatch.await();
                        var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
                        var idSequence = idSequenceProvider.getIdSequence(LABEL_TOKEN_NAME);
                        long firstIdInBatch = idSequence.nextId(NULL_CONTEXT);
                        if (!uniqueIDs.add(firstIdInBatch)) {
                            throw new IllegalStateException("Duplicated id " + firstIdInBatch + " generated");
                        }
                        for (long expectedId = firstIdInBatch + 1;
                                expectedId < firstIdInBatch + labelTokenPerPage - 1;
                                expectedId++) {
                            long nextId = idSequence.nextId(NULL_CONTEXT);
                            if (!uniqueIDs.add(nextId)) {
                                throw new IllegalStateException("Duplicated id " + nextId + " generated");
                            }
                            assertEquals(expectedId, nextId);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown();
            Futures.getAll(futures);

            var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
            var idSequence = idSequenceProvider.getIdSequence(LABEL_TOKEN_NAME);
            // post concurrent workload batch has predictable first id
            assertEquals(labelTokenPerPage * numberOfExecutors, idSequence.nextId(NULL_CONTEXT));
        } finally {
            executors.shutdown();
        }
    }

    @Test
    void releaseNotFullyUsedIdPageBatch() {
        int recordsPerPage = neoStores.getRelationshipGroupStore().getRecordsPerPage();

        for (int id = 1; id < recordsPerPage; id++) {
            var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
            var idSequence = idSequenceProvider.getIdSequence(StoreType.RELATIONSHIP_GROUP);
            assertEquals(id, idSequence.nextId(NULL_CONTEXT));
            idSequenceProvider.release(NULL_CONTEXT);
        }

        var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequence = idSequenceProvider.getIdSequence(StoreType.RELATIONSHIP_GROUP);
        assertEquals(recordsPerPage, idSequence.nextId(NULL_CONTEXT));
    }

    @Test
    void concurrentReleaseOfPageIdBatches() throws ExecutionException {
        long labelTokenPerPage = neoStores.getRecordStore(LABEL_TOKEN).getRecordsPerPage();
        int numberOfExecutors = 20;
        var uniqueIDs = ConcurrentHashMap.newKeySet((int) (labelTokenPerPage * numberOfExecutors));

        var executors = Executors.newFixedThreadPool(numberOfExecutors);
        try {
            var futures = new ArrayList<Future<?>>(numberOfExecutors);
            CountDownLatch startLatch = new CountDownLatch(1);
            for (int i = 0; i < numberOfExecutors; i++) {
                futures.add(executors.submit(() -> {
                    try {
                        startLatch.await();
                        for (int id = 0; id < labelTokenPerPage; id++) {
                            var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
                            var idSequence = idSequenceProvider.getIdSequence(LABEL_TOKEN);
                            long nextId = idSequence.nextId(NULL_CONTEXT);
                            if (!uniqueIDs.add(nextId)) {
                                throw new IllegalStateException("Duplicated id " + nextId + " generated");
                            }
                            idSequenceProvider.release(NULL_CONTEXT);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown();
            Futures.getAll(futures);

            assertThat(uniqueIDs).hasSize((int) (numberOfExecutors * labelTokenPerPage));
        } finally {
            executors.shutdown();
        }
    }

    @Test
    void allocatedBatchedRespectReservedId() {
        int recordsPerPage = neoStores.getNodeStore().getRecordsPerPage();
        long reservedId = IdValidator.INTEGER_MINUS_ONE;

        IdSequence idSequence;
        do {
            var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
            idSequence = idSequenceProvider.getIdSequence(NODE);
        } while (idSequence.nextId(NULL_CONTEXT) < reservedId - recordsPerPage);

        for (int i = 0; i < recordsPerPage; i++) {
            assertNotEquals(reservedId, idSequence.nextId(NULL_CONTEXT));
        }
    }

    @Test
    void allocatedBatchesRespectMaxIdGeneratorCapacity() {
        assertThrows(IdCapacityExceededException.class, () -> {
            //noinspection InfiniteLoopStatement
            while (true) {
                var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
                var idSequence = idSequenceProvider.getIdSequence(NODE);
                idSequence.nextId(NULL_CONTEXT);
            }
        });
    }

    @Test
    void pagedBatchedAllocationWithIncorrectlyCalledNextCalls() {
        NodeStore nodeStore = neoStores.getNodeStore();
        int recordsPerPage = nodeStore.getRecordsPerPage();

        var nodesIdGenerator = nodeStore.getIdGenerator();
        // consume some ids that should be part of first page batch.
        nodesIdGenerator.nextId(NULL_CONTEXT);
        nodesIdGenerator.nextId(NULL_CONTEXT);

        var idSequenceProvider1 = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequence1 = idSequenceProvider1.getIdSequence(NODE);
        assertEquals(2, idSequence1.nextId(NULL_CONTEXT));

        var idSequenceProvider2 = new BatchedTransactionIdSequenceProvider(neoStores);
        var idSequence2 = idSequenceProvider2.getIdSequence(NODE);
        assertEquals(recordsPerPage, idSequence2.nextId(NULL_CONTEXT));

        // first batch contains all other ids from the first batch
        for (int i = 3; i < recordsPerPage; i++) {
            assertEquals(i, idSequence1.nextId(NULL_CONTEXT));
        }
    }

    @Test
    void differentThreadsWorkingWithDifferentPagesOfIds() throws ExecutionException {
        int numberOfExecutors = 20;
        int workerIterations = 100;
        long recordsPerPage = neoStores.getRecordStore(PROPERTY_ARRAY).getRecordsPerPage();
        var idPages = ConcurrentHashMap.newKeySet(numberOfExecutors);

        var executors = Executors.newFixedThreadPool(numberOfExecutors);
        try {
            var futures = new ArrayList<Future<?>>(numberOfExecutors);
            CountDownLatch startLatch = new CountDownLatch(1);
            for (int i = 0; i < numberOfExecutors; i++) {
                futures.add(executors.submit(() -> {
                    try {
                        startLatch.await();
                        for (int iteration = 0; iteration < workerIterations; iteration++) {
                            var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
                            var idSequence = idSequenceProvider.getIdSequence(PROPERTY_ARRAY);
                            long firstIdInBatch = idSequence.nextId(NULL_CONTEXT);
                            long page = firstIdInBatch / recordsPerPage;
                            if (!idPages.add(page)) {
                                throw new IllegalStateException("Duplicated page id " + page + " generated.");
                            }
                            for (long expectedId = firstIdInBatch + 1;
                                    expectedId < firstIdInBatch + recordsPerPage - 1;
                                    expectedId++) {
                                assertEquals(expectedId, idSequence.nextId(NULL_CONTEXT));
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown();
            Futures.getAll(futures);

            var idSequenceProvider = new BatchedTransactionIdSequenceProvider(neoStores);
            var idSequence = idSequenceProvider.getIdSequence(PROPERTY_ARRAY);
            // post concurrent workload batch has predictable first id
            assertEquals(workerIterations * recordsPerPage * numberOfExecutors, idSequence.nextId(NULL_CONTEXT));
        } finally {
            executors.shutdown();
        }
    }
}
