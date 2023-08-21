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
package org.neo4j.kernel.impl.index.schema;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.BlockBasedIndexPopulator.NO_MONITOR;
import static org.neo4j.kernel.impl.index.schema.IndexEntryTestUtil.generateStringValueResultingInIndexEntrySize;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.Race.throwing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.memory.UnsafeDirectByteBufferAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.BlockBasedIndexPopulator.Monitor;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ThreadSafePeakMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.scheduler.JobSchedulerAdapter;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Value;

@ActorsExtension
@EphemeralPageCacheExtension
abstract class BlockBasedIndexPopulatorTest<KEY extends NativeIndexKey<KEY>> {
    private static final LabelSchemaDescriptor SCHEMA_DESCRIPTOR = SchemaDescriptors.forLabel(1, 1);
    final IndexDescriptor INDEX_DESCRIPTOR = IndexPrototype.forSchema(SCHEMA_DESCRIPTOR)
            .withIndexType(indexType())
            .withName("index")
            .materialise(1);
    public static final int SUFFICIENTLY_LARGE_BUFFER_SIZE = (int) ByteUnit.kibiBytes(50);
    final TokenNameLookup tokenNameLookup = SIMPLE_NAME_LOOKUP;

    @Inject
    Actor merger;

    @Inject
    Actor closer;

    @Inject
    FileSystemAbstraction fs;

    @Inject
    TestDirectory testDir;

    @Inject
    PageCache pageCache;

    IndexFiles indexFiles;
    DatabaseIndexContext databaseIndexContext;
    private JobScheduler jobScheduler;
    IndexPopulator.PopulationWorkScheduler populationWorkScheduler;

    abstract IndexType indexType();

    abstract BlockBasedIndexPopulator<KEY> instantiatePopulator(
            Monitor monitor, ByteBufferFactory bufferFactory, MemoryTracker memoryTracker) throws IOException;

    abstract Layout<KEY, NullValue> layout();

    abstract Value supportedValue(int i);

    @BeforeEach
    void setup() {
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor("test", "v1");
        IndexDirectoryStructure directoryStructure =
                directoriesByProvider(testDir.homePath()).forProvider(providerDescriptor);
        indexFiles = new IndexFiles(fs, directoryStructure, INDEX_DESCRIPTOR.getId());
        var pageCacheTracer = PageCacheTracer.NULL;
        databaseIndexContext = DatabaseIndexContext.builder(
                        pageCache,
                        fs,
                        new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        pageCacheTracer,
                        DEFAULT_DATABASE_NAME)
                .build();
        jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        populationWorkScheduler = wrapScheduler(jobScheduler);
    }

    @AfterEach
    void tearDown() throws Exception {
        jobScheduler.shutdown();
    }

    private static IndexPopulator.PopulationWorkScheduler wrapScheduler(JobScheduler jobScheduler) {
        return new IndexPopulator.PopulationWorkScheduler() {

            @Override
            public <T> JobHandle<T> schedule(
                    IndexPopulator.JobDescriptionSupplier descriptionSupplier, Callable<T> job) {
                return jobScheduler.schedule(
                        Group.INDEX_POPULATION_WORK, new JobMonitoringParams(null, null, null), job);
            }
        };
    }

    @Test
    void shouldAwaitMergeToBeFullyAbortedBeforeLeavingCloseMethod() throws Exception {
        // given
        TrappingMonitor monitor = new TrappingMonitor(ignore -> false);
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(monitor);
        boolean closed = false;
        try {
            populator.add(batchOfUpdates(), NULL_CONTEXT);

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = merger.submit(scanCompletedTask(populator));
            // and waiting for merge to get going
            monitor.barrier.awaitUninterruptibly();
            // calling close here should wait for the merge future, so that checking the merge future for "done"
            // immediately afterwards must say true
            Future<Void> closeFuture = closer.submit(() -> populator.close(false, NULL_CONTEXT));
            closer.untilWaiting();
            monitor.barrier.release();
            closeFuture.get();
            closed = true;

            // then
            assertThat(monitor.scanCompletedEnded).isTrue();
            mergeFuture.get();
            assertTrue(mergeFuture.isDone());
        } finally {
            if (!closed) {
                populator.close(true, NULL_CONTEXT);
            }
        }
    }

    private Callable<Object> scanCompletedTask(BlockBasedIndexPopulator<KEY> populator) {
        return () -> {
            populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
            return null;
        };
    }

    @Test
    void shouldHandleBeingAbortedWhileMerging() throws Exception {
        // given
        TrappingMonitor monitor = new TrappingMonitor(numberOfBlocks -> numberOfBlocks == 2);
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(monitor);
        boolean closed = false;
        try {
            populator.add(batchOfUpdates(), NULL_CONTEXT);

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = merger.submit(scanCompletedTask(populator));
            // and waiting for merge to get going
            monitor.barrier.await();
            monitor.barrier.release();
            monitor.mergeFinishedBarrier.awaitUninterruptibly();
            // calling close here should wait for the merge future, so that checking the merge future for "done"
            // immediately afterwards must say true
            Future<Void> closeFuture = closer.submit(() -> populator.close(false, NULL_CONTEXT));
            closer.untilWaiting();
            monitor.mergeFinishedBarrier.release();
            closeFuture.get();
            closed = true;

            // then let's make sure scanComplete was cancelled, not throwing exception or anything.
            mergeFuture.get();
        } finally {
            if (!closed) {
                populator.close(false, NULL_CONTEXT);
            }
        }
    }

    @Test
    void shouldReportAccurateProgressThroughoutThePhases() throws Exception {
        // given
        TrappingMonitor monitor = new TrappingMonitor(numberOfBlocks -> numberOfBlocks == 1);
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(monitor);
        try {
            populator.add(batchOfUpdates(), NULL_CONTEXT);

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = merger.submit(scanCompletedTask(populator));
            // and waiting for merge to get going
            monitor.barrier.awaitUninterruptibly();
            // this is a bit fuzzy, but what we want is to assert that the scan doesn't represent 100% of the work
            assertEquals(0.5f, populator.progress(PopulationProgress.DONE).getProgress(), 0.1f);
            monitor.barrier.release();
            monitor.mergeFinishedBarrier.awaitUninterruptibly();
            assertEquals(0.7f, populator.progress(PopulationProgress.DONE).getProgress(), 0.1f);
            monitor.mergeFinishedBarrier.release();
            mergeFuture.get();
            assertEquals(1f, populator.progress(PopulationProgress.DONE).getProgress(), 0f);
        } finally {
            populator.close(true, NULL_CONTEXT);
        }
    }

    @Test
    void shouldCorrectlyDecideToAwaitMergeDependingOnProgress() throws Throwable {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(NO_MONITOR);
        boolean closed = false;
        try {
            populator.add(batchOfUpdates(), NULL_CONTEXT);

            // when
            Race race = new Race();
            race.addContestant(
                    throwing(() -> populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT)));
            race.addContestant(throwing(() -> populator.close(false, NULL_CONTEXT)));
            race.go();
            closed = true;

            // then regardless of who wins (close/merge) after close call returns no files should still be mapped
            EphemeralFileSystemAbstraction ephemeralFileSystem = (EphemeralFileSystemAbstraction) fs;
            ephemeralFileSystem.assertNoOpenFiles();
        } finally {
            if (!closed) {
                populator.close(true, NULL_CONTEXT);
            }
        }
    }

    @Test
    void shouldDeleteDirectoryOnDrop() throws Exception {
        // given
        TrappingMonitor monitor = new TrappingMonitor(ignore -> false);
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(monitor);
        boolean closed = false;
        try {
            populator.add(batchOfUpdates(), NULL_CONTEXT);

            // when starting to merge (in a separate thread)
            Future<Object> mergeFuture = merger.submit(scanCompletedTask(populator));
            // and waiting for merge to get going
            monitor.barrier.awaitUninterruptibly();
            // calling drop here should wait for the merge future and then delete index directory
            assertTrue(fs.fileExists(indexFiles.getBase()));
            assertTrue(fs.isDirectory(indexFiles.getBase()));
            assertTrue(fs.listFiles(indexFiles.getBase()).length > 0);

            Future<Void> dropFuture = closer.submit(populator::drop);
            closer.untilWaiting();
            monitor.barrier.release();
            dropFuture.get();
            closed = true;

            // then
            mergeFuture.get();
            assertFalse(fs.fileExists(indexFiles.getBase()));
        } finally {
            if (!closed) {
                populator.close(true, NULL_CONTEXT);
            }
        }
    }

    @Test
    void shouldDeallocateAllAllocatedMemoryOnClose() throws IndexEntryConflictException, IOException {
        // given
        ThreadSafePeakMemoryTracker memoryTracker = new ThreadSafePeakMemoryTracker();
        ByteBufferFactory bufferFactory = new ByteBufferFactory(UnsafeDirectByteBufferAllocator::new, 100);
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(NO_MONITOR, bufferFactory, memoryTracker);
        boolean closed = false;
        try {
            // when
            Collection<IndexEntryUpdate<?>> updates = batchOfUpdates();
            populator.add(updates, NULL_CONTEXT);
            int nextId = updates.size();
            externalUpdates(populator, nextId, nextId + 10);
            nextId = nextId + 10;
            long memoryBeforeScanCompleted = memoryTracker.usedNativeMemory();
            populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
            externalUpdates(populator, nextId, nextId + 10);

            // then
            assertTrue(
                    memoryTracker.peakMemoryUsage() > memoryBeforeScanCompleted,
                    "expected some memory to have been temporarily allocated in scanCompleted");
            populator.close(true, NULL_CONTEXT);
            closed = true;

            bufferFactory.close();
            assertEquals(0, memoryTracker.usedNativeMemory());
        } finally {
            if (!closed) {
                populator.close(true, NULL_CONTEXT);
            }
        }
    }

    @Test
    void shouldDeallocateAllAllocatedMemoryOnDrop() throws IndexEntryConflictException, IOException {
        // given
        ThreadSafePeakMemoryTracker memoryTracker = new ThreadSafePeakMemoryTracker();
        ByteBufferFactory bufferFactory = new ByteBufferFactory(UnsafeDirectByteBufferAllocator::new, 100);
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(NO_MONITOR, bufferFactory, memoryTracker);
        boolean closed = false;
        try {
            // when
            Collection<IndexEntryUpdate<?>> updates = batchOfUpdates();
            populator.add(updates, NULL_CONTEXT);
            int nextId = updates.size();
            externalUpdates(populator, nextId, nextId + 10);
            nextId = nextId + 10;
            long memoryBeforeScanCompleted = memoryTracker.usedNativeMemory();
            populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
            externalUpdates(populator, nextId, nextId + 10);

            // then
            assertTrue(
                    memoryTracker.peakMemoryUsage() > memoryBeforeScanCompleted,
                    "expected some memory to have been temporarily allocated in scanCompleted");
            populator.drop();
            closed = true;
            bufferFactory.close();
            assertEquals(0, memoryTracker.usedNativeMemory());
        } finally {
            if (!closed) {
                populator.close(true, NULL_CONTEXT);
            }
        }
    }

    @Test
    void shouldBuildNonUniqueSampleAsPartOfScanCompleted() throws IndexEntryConflictException, IOException {
        // given
        ThreadSafePeakMemoryTracker memoryTracker = new ThreadSafePeakMemoryTracker();
        ByteBufferFactory bufferFactory = new ByteBufferFactory(UnsafeDirectByteBufferAllocator::new, 100);
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(NO_MONITOR, bufferFactory, memoryTracker);
        Collection<IndexEntryUpdate<?>> populationUpdates = batchOfUpdates();
        populator.add(populationUpdates, NULL_CONTEXT);

        // when
        populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
        // Also a couple of updates afterwards
        int numberOfUpdatesAfterCompleted = 4;
        try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            for (int i = 0; i < numberOfUpdatesAfterCompleted; i++) {
                updater.process(IndexEntryUpdate.add(10_000 + i, INDEX_DESCRIPTOR, supportedValue(i)));
            }
        }
        populator.close(true, NULL_CONTEXT);

        // then
        IndexSample sample = populator.sample(NULL_CONTEXT);
        assertEquals(populationUpdates.size(), sample.indexSize());
        assertEquals(populationUpdates.size(), sample.sampleSize());
        assertEquals(populationUpdates.size(), sample.uniqueValues());
        assertEquals(numberOfUpdatesAfterCompleted, sample.updates());
    }

    @Test
    void shouldFlushTreeOnScanCompleted() throws IndexEntryConflictException, IOException {
        // given
        ThreadSafePeakMemoryTracker memoryTracker = new ThreadSafePeakMemoryTracker();
        ByteBufferFactory bufferFactory = new ByteBufferFactory(UnsafeDirectByteBufferAllocator::new, 100);
        AtomicInteger checkpoints = new AtomicInteger();
        GBPTree.Monitor treeMonitor = new GBPTree.Monitor.Adaptor() {
            @Override
            public void checkpointCompleted() {
                checkpoints.incrementAndGet();
            }
        };
        Monitors monitors = new Monitors(databaseIndexContext.monitors, NullLogProvider.getInstance());
        monitors.addMonitorListener(treeMonitor);
        databaseIndexContext = DatabaseIndexContext.builder(databaseIndexContext)
                .withMonitors(monitors)
                .build();
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(NO_MONITOR, bufferFactory, memoryTracker);
        try {
            // when
            int numberOfCheckPointsBeforeScanCompleted = checkpoints.get();
            populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);

            // then
            assertEquals(numberOfCheckPointsBeforeScanCompleted + 1, checkpoints.get());
        } finally {
            populator.close(true, NULL_CONTEXT);
        }
    }

    @Test
    void shouldScheduleMergeOnJobSchedulerWithCorrectGroup() throws IndexEntryConflictException, IOException {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(NO_MONITOR);
        boolean closed = false;
        try {
            populator.add(batchOfUpdates(), NULL_CONTEXT);

            // when
            MutableBoolean called = new MutableBoolean();
            JobScheduler trackingJobScheduler = new JobSchedulerAdapter() {
                @Override
                public <T> JobHandle<T> schedule(
                        Group group, JobMonitoringParams jobMonitoringParams, Callable<T> job) {
                    called.setTrue();
                    assertThat(group).isSameAs(Group.INDEX_POPULATION_WORK);
                    return jobScheduler.schedule(group, jobMonitoringParams, job);
                }
            };
            populator.scanCompleted(nullInstance, wrapScheduler(trackingJobScheduler), NULL_CONTEXT);
            assertTrue(called.booleanValue());
            populator.close(true, NULL_CONTEXT);
            closed = true;
        } finally {
            if (!closed) {
                populator.close(true, NULL_CONTEXT);
            }
        }
    }

    @Test
    void shouldFailOnBatchAddedTooLargeValue() throws IOException {
        /// given
        ByteBufferFactory bufferFactory =
                new ByteBufferFactory(UnsafeDirectByteBufferAllocator::new, SUFFICIENTLY_LARGE_BUFFER_SIZE);
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(NO_MONITOR, bufferFactory, INSTANCE);
        try {
            int size = populator.tree.keyValueSizeCap() + 1;
            assertThrows(
                    IllegalArgumentException.class,
                    () -> populator.add(
                            singletonList(IndexEntryUpdate.add(
                                    0, INDEX_DESCRIPTOR, generateStringValueResultingInIndexEntrySize(layout(), size))),
                            NULL_CONTEXT));
        } finally {
            populator.close(false, NULL_CONTEXT);
        }
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldFailOnUpdatedTooLargeValue(boolean updateBeforeScanCompleted)
            throws IndexEntryConflictException, IOException {
        /// given
        ByteBufferFactory bufferFactory =
                new ByteBufferFactory(UnsafeDirectByteBufferAllocator::new, SUFFICIENTLY_LARGE_BUFFER_SIZE);
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(NO_MONITOR, bufferFactory, INSTANCE);
        try {
            int size = populator.tree.keyValueSizeCap() + 1;
            if (!updateBeforeScanCompleted) {
                populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
            }
            assertThrows(IllegalArgumentException.class, () -> {
                try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
                    updater.process(IndexEntryUpdate.add(
                            0, INDEX_DESCRIPTOR, generateStringValueResultingInIndexEntrySize(layout(), size)));
                }
            });
        } finally {
            populator.close(false, NULL_CONTEXT);
        }
    }

    @Test
    void shouldCountExternalUpdatesAsSampleUpdates() throws IOException, IndexEntryConflictException {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(NO_MONITOR);
        try {
            populator.add(List.of(add(0), add(1)), NULL_CONTEXT);
            try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
                updater.process(add(10));
                updater.process(add(11));
                updater.process(add(12));
            }

            // when
            populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);

            try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
                updater.process(remove(10));
            }

            // then
            IndexSample sample = populator.sample(NULL_CONTEXT);
            assertThat(sample.updates()).isEqualTo(4);
        } finally {
            populator.close(true, NULL_CONTEXT);
        }
    }

    Seeker<KEY, NullValue> seek(GBPTree<KEY, NullValue> tree, Layout<KEY, NullValue> layout) throws IOException {
        KEY low = layout.newKey();
        low.initialize(Long.MIN_VALUE);
        low.initValuesAsLowest();
        KEY high = layout.newKey();
        high.initialize(Long.MAX_VALUE);
        high.initValuesAsHighest();
        return tree.seek(low, high, NULL_CONTEXT);
    }

    private void externalUpdates(BlockBasedIndexPopulator<KEY> populator, int firstId, int lastId)
            throws IndexEntryConflictException {
        try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            for (int i = firstId; i < lastId; i++) {
                updater.process(add(i));
            }
        }
    }

    protected BlockBasedIndexPopulator<KEY> instantiatePopulator(Monitor monitor) throws IOException {
        return instantiatePopulator(monitor, heapBufferFactory(100), INSTANCE);
    }

    private Collection<IndexEntryUpdate<?>> batchOfUpdates() {
        List<IndexEntryUpdate<?>> updates = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            updates.add(add(i));
        }
        return updates;
    }

    private IndexEntryUpdate<IndexDescriptor> add(int i) {
        return IndexEntryUpdate.add(i, INDEX_DESCRIPTOR, supportedValue(i));
    }

    private IndexEntryUpdate<IndexDescriptor> remove(int i) {
        return IndexEntryUpdate.remove(i, INDEX_DESCRIPTOR, supportedValue(i));
    }

    private static class TrappingMonitor extends Monitor.Adapter {
        private final Barrier.Control barrier = new Barrier.Control();
        private final Barrier.Control mergeFinishedBarrier = new Barrier.Control();
        private final LongPredicate trapForMergeIterationFinished;
        private volatile boolean scanCompletedEnded;

        TrappingMonitor(LongPredicate trapForMergeIterationFinished) {
            this.trapForMergeIterationFinished = trapForMergeIterationFinished;
        }

        @Override
        public void mergedBlocks(long resultingBlockSize, long resultingEntryCount, long numberOfBlocks) {
            barrier.reached();
        }

        @Override
        public void mergeIterationFinished(long numberOfBlocksBefore, long numberOfBlocksAfter) {
            if (trapForMergeIterationFinished.test(numberOfBlocksAfter)) {
                mergeFinishedBarrier.reached();
            }
        }

        @Override
        public void scanCompletedEnded() {
            scanCompletedEnded = true;
        }
    }
}
