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
package org.neo4j.kernel.impl.transaction.state.storeview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;
import static org.neo4j.internal.batchimport.staging.ProcessorAssignmentStrategies.saturateSpecificStep;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.api.index.StoreScan.NO_EXTERNAL_UPDATES;
import static org.neo4j.test.DoubleLatch.awaitLatch;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan.ExternalUpdatesCheck;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.PropertyAwareEntityStoreScan.CursorEntityIdIterator;
import org.neo4j.lock.Lock;
import org.neo4j.lock.LockService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;

class StoreScanStageTest {
    private static final int WORKERS = 8;
    private static final int LABEL = 1;
    private static final String KEY = "key";
    private static final int NUMBER_OF_BATCHES = 4;

    private final Config dbConfig = Config.defaults(GraphDatabaseInternalSettings.index_population_workers, WORKERS);
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    private final Configuration config = new Configuration() {
        @Override
        public int maxNumberOfWorkerThreads() {
            return WORKERS;
        }

        @Override
        public int batchSize() {
            return 10;
        }
    };
    private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();

    @AfterEach
    void tearDown() {
        jobScheduler.close();
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest(name = "parallelWrite={0}")
    void shouldGenerateUpdatesInParallel(boolean parallelWrite) {
        // given
        StubStorageCursors data = someData();
        EntityIdIterator entityIdIterator =
                new CursorEntityIdIterator<>(data.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL));
        var propertyConsumer = new ThreadCapturingPropertyConsumer();
        var tokenConsumer = new ThreadCapturingTokenConsumer();
        ControlledLockFunction lockFunction = new ControlledLockFunction();
        StoreScanStage<StorageNodeCursor> scan = new StoreScanStage<>(
                dbConfig,
                config,
                (ct, sc) -> entityIdIterator,
                NO_EXTERNAL_UPDATES,
                new AtomicBoolean(true),
                data,
                any -> StoreCursors.NULL,
                new int[] {LABEL},
                PropertySelection.ALL_PROPERTIES,
                propertyConsumer,
                tokenConsumer,
                new NodeCursorBehaviour(data),
                lockFunction,
                parallelWrite,
                jobScheduler,
                CONTEXT_FACTORY,
                EmptyMemoryTracker.INSTANCE,
                true);

        // when
        runScan(scan);

        // then it completes and we see > 1 threads
        assertThat(lockFunction.seenThreads.size()).isGreaterThan(1);
        if (parallelWrite) {
            assertThat(propertyConsumer.seenThreads.size()).isGreaterThan(1);
            assertThat(tokenConsumer.seenThreads.size()).isGreaterThan(1);
        } else {
            assertThat(propertyConsumer.seenThreads.size()).isEqualTo(1);
            assertThat(tokenConsumer.seenThreads.size()).isEqualTo(1);
        }
    }

    @Test
    void shouldPanicAndExitStageOnWriteFailure() {
        // given
        StubStorageCursors data = someData();
        EntityIdIterator entityIdIterator =
                new CursorEntityIdIterator<>(data.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL));

        var failingWriter = new PropertyConsumer(() -> {
            throw new IllegalStateException("Failed to write");
        });
        StoreScanStage<StorageNodeCursor> scan = new StoreScanStage<>(
                dbConfig,
                config,
                (ct, sc) -> entityIdIterator,
                NO_EXTERNAL_UPDATES,
                new AtomicBoolean(true),
                data,
                any -> StoreCursors.NULL,
                new int[] {LABEL},
                PropertySelection.ALL_PROPERTIES,
                failingWriter,
                null,
                new NodeCursorBehaviour(data),
                id -> null,
                true,
                jobScheduler,
                CONTEXT_FACTORY,
                EmptyMemoryTracker.INSTANCE,
                true);

        // when/then
        assertThatThrownBy(() -> runScan(scan))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to write");
    }

    @Test
    void shouldApplyExternalUpdatesIfThereAreSuch() {
        // given
        StubStorageCursors data = someData();
        EntityIdIterator entityIdIterator =
                new CursorEntityIdIterator<>(data.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL));
        AtomicInteger numBatchesProcessed = new AtomicInteger();
        ControlledExternalUpdatesCheck externalUpdatesCheck =
                new ControlledExternalUpdatesCheck(config.batchSize(), 2, numBatchesProcessed, true);
        var writer = new PropertyConsumer(numBatchesProcessed::incrementAndGet);

        StoreScanStage<StorageNodeCursor> scan = new StoreScanStage<>(
                dbConfig,
                config,
                (ct, sc) -> entityIdIterator,
                externalUpdatesCheck,
                new AtomicBoolean(true),
                data,
                any -> StoreCursors.NULL,
                new int[] {LABEL},
                PropertySelection.ALL_PROPERTIES,
                writer,
                null,
                new NodeCursorBehaviour(data),
                id -> null,
                true,
                jobScheduler,
                CONTEXT_FACTORY,
                EmptyMemoryTracker.INSTANCE,
                true);

        // when
        runScan(scan);

        // then
        assertThat(externalUpdatesCheck.applyCallCount).isEqualTo(1);
    }

    @Test
    void shouldAbortScanOnStopped() {
        // given
        StubStorageCursors data = someData();
        EntityIdIterator entityIdIterator =
                new CursorEntityIdIterator<>(data.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL));
        AtomicInteger numBatchesProcessed = new AtomicInteger();
        AtomicBoolean continueScanning = new AtomicBoolean(true);
        AbortingExternalUpdatesCheck externalUpdatesCheck = new AbortingExternalUpdatesCheck(1, continueScanning);
        var writer = new PropertyConsumer(numBatchesProcessed::incrementAndGet);
        StoreScanStage<StorageNodeCursor> scan = new StoreScanStage<>(
                dbConfig,
                config,
                (ct, sc) -> entityIdIterator,
                externalUpdatesCheck,
                continueScanning,
                data,
                any -> StoreCursors.NULL,
                new int[] {LABEL},
                PropertySelection.ALL_PROPERTIES,
                writer,
                null,
                new NodeCursorBehaviour(data),
                id -> null,
                true,
                jobScheduler,
                CONTEXT_FACTORY,
                EmptyMemoryTracker.INSTANCE,
                true);

        // when
        runScan(scan);

        // then
        assertThat(numBatchesProcessed.get()).isEqualTo(2);
    }

    @Test
    void shouldReportCorrectNumberOfCompletedEntities() {
        // given
        StubStorageCursors data = someData();
        EntityIdIterator entityIdIterator =
                new CursorEntityIdIterator<>(data.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL));
        StoreScanStage<StorageNodeCursor> scan = new StoreScanStage<>(
                dbConfig,
                config,
                (ct, sc) -> entityIdIterator,
                NO_EXTERNAL_UPDATES,
                new AtomicBoolean(true),
                data,
                any -> StoreCursors.NULL,
                new int[] {LABEL},
                PropertySelection.ALL_PROPERTIES,
                new ThreadCapturingPropertyConsumer(),
                new ThreadCapturingTokenConsumer(),
                new NodeCursorBehaviour(data),
                l -> LockService.NO_LOCK,
                true,
                jobScheduler,
                CONTEXT_FACTORY,
                EmptyMemoryTracker.INSTANCE,
                true);

        // when
        runScan(scan);

        // then
        assertThat(scan.numberOfCompletedEntities()).isEqualTo((long) config.batchSize() * NUMBER_OF_BATCHES);
    }

    @Test
    void shouldProvideMaxIdIfCannotDetermineCutOffPoint() {
        // given
        StubStorageCursors data = someData();
        EntityIdIterator entityIdIterator =
                new CursorEntityIdIterator<>(data.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL));
        AtomicInteger numBatchesProcessed = new AtomicInteger();
        ControlledExternalUpdatesCheck externalUpdatesCheck =
                new ControlledExternalUpdatesCheck(config.batchSize(), 2, numBatchesProcessed, false);
        var writer = new PropertyConsumer(numBatchesProcessed::incrementAndGet);

        StoreScanStage<StorageNodeCursor> scan = new StoreScanStage<>(
                dbConfig,
                config,
                (ct, sc) -> entityIdIterator,
                externalUpdatesCheck,
                new AtomicBoolean(true),
                data,
                any -> StoreCursors.NULL,
                new int[] {LABEL},
                PropertySelection.ALL_PROPERTIES,
                writer,
                null,
                new NodeCursorBehaviour(data),
                id -> null,
                true,
                jobScheduler,
                CONTEXT_FACTORY,
                EmptyMemoryTracker.INSTANCE,
                false);

        // when
        runScan(scan);

        // then
        assertThat(externalUpdatesCheck.applyCallCount).isEqualTo(1);
    }

    private static void runScan(StoreScanStage<StorageNodeCursor> scan) {
        superviseDynamicExecution(saturateSpecificStep(1), scan);
    }

    private StubStorageCursors someData() {
        StubStorageCursors data = new StubStorageCursors();
        for (int i = 0; i < config.batchSize() * NUMBER_OF_BATCHES; i++) {
            data.withNode(i).labels(LABEL).properties(KEY, stringValue("name_" + i));
        }
        return data;
    }

    private static class ControlledLockFunction implements LongFunction<Lock> {
        private final Set<Thread> seenThreads = ConcurrentHashMap.newKeySet();
        private final CountDownLatch latch = new CountDownLatch(2);

        @Override
        public Lock apply(long id) {
            // We know that there'll be > 1 updates generator thread, therefore block the first batch that comes in
            // and let another one trigger us to continue. This proves that there are at least 2 threads generating
            // updates
            seenThreads.add(Thread.currentThread());
            latch.countDown();
            awaitLatch(latch);
            return null;
        }
    }

    private static class ControlledExternalUpdatesCheck implements ExternalUpdatesCheck {
        private final long expectedNodeId;
        private final int applyOnBatchIndex;
        private final AtomicInteger numBatchesProcessed;
        private int checkCallCount;
        private volatile int applyCallCount;

        ControlledExternalUpdatesCheck(
                int batchSize,
                int applyOnBatchIndex,
                AtomicInteger numBatchesProcessed,
                boolean canDetermineCutOffPoint) {
            this.applyOnBatchIndex = applyOnBatchIndex;
            this.numBatchesProcessed = numBatchesProcessed;
            this.expectedNodeId = canDetermineCutOffPoint ? batchSize * applyOnBatchIndex - 1 : Long.MAX_VALUE;
        }

        @Override
        public boolean needToApplyExternalUpdates() {
            return checkCallCount++ == applyOnBatchIndex;
        }

        @Override
        public void applyExternalUpdates(long currentlyIndexedNodeId) {
            assertThat(currentlyIndexedNodeId).isEqualTo(expectedNodeId);
            assertThat(numBatchesProcessed.get()).isEqualTo(applyOnBatchIndex);
            applyCallCount++;
        }
    }

    private static class PropertyConsumer implements PropertyScanConsumer {
        private final Runnable action;

        PropertyConsumer(Runnable action) {
            this.action = action;
        }

        @Override
        public Batch newBatch() {
            return new Batch() {

                @Override
                public void addRecord(long entityId, int[] tokens, Map<Integer, Value> properties) {}

                @Override
                public void process() {
                    action.run();
                }
            };
        }
    }

    private static class ThreadCapturingPropertyConsumer implements PropertyScanConsumer {

        private final Set<Thread> seenThreads = ConcurrentHashMap.newKeySet();

        @Override
        public Batch newBatch() {
            return new Batch() {

                @Override
                public void addRecord(long entityId, int[] tokens, Map<Integer, Value> properties) {}

                @Override
                public void process() {
                    seenThreads.add(Thread.currentThread());
                }
            };
        }
    }

    private static class ThreadCapturingTokenConsumer implements TokenScanConsumer {

        private final Set<Thread> seenThreads = ConcurrentHashMap.newKeySet();

        @Override
        public Batch newBatch() {
            return new Batch() {
                @Override
                public void addRecord(long entityId, int[] tokens) {}

                @Override
                public void process() {
                    seenThreads.add(Thread.currentThread());
                }
            };
        }
    }

    // Used to hook into the ReadEntityIdsStep to know when to trigger the stop, it's not actually doing external
    // updates
    // The way this is called in the step is that disabling scanning won't trigger until the next batch
    private static class AbortingExternalUpdatesCheck implements ExternalUpdatesCheck {
        private final int abortAfterBatch;
        private final AtomicBoolean continueScanning;
        private int callCount;

        AbortingExternalUpdatesCheck(int abortAfterBatch, AtomicBoolean continueScanning) {
            this.abortAfterBatch = abortAfterBatch;
            this.continueScanning = continueScanning;
        }

        @Override
        public boolean needToApplyExternalUpdates() {
            if (callCount++ == abortAfterBatch) {
                continueScanning.set(false);
            }
            return false;
        }

        @Override
        public void applyExternalUpdates(long currentlyIndexedNodeId) {
            throw new IllegalStateException("Should not be called");
        }
    }
}
