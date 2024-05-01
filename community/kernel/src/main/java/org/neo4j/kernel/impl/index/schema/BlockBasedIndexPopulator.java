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

import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.index.internal.gbptree.DataTree.W_SPLIT_KEEP_ALL_LEFT;
import static org.neo4j.internal.helpers.collection.Iterables.first;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.kernel.impl.index.schema.NativeIndexUpdater.initializeKeyFromUpdate;
import static org.neo4j.util.concurrent.Runnables.runAll;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.memory.ByteBufferFactory.Allocator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.IndexValueValidator;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.updater.DelegatingIndexUpdater;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

/**
 * {@link IndexPopulator} for native indexes that stores scan updates in parallel append-only files. When all scan updates have been collected
 * each file is sorted and then all of them merged together into the resulting index.
 * <p>
 * Note on buffers: basically each thread adding scan updates will make use of a {@link ByteBufferFactory#acquireThreadLocalBuffer(MemoryTracker)}
 * thread-local buffer}.
 * This together with {@link ByteBufferFactory#globalAllocator() a global buffer for external updates} and carefully reused
 * {@link ByteBufferFactory#newLocalAllocator() local buffers} for merging allows memory consumption to stay virtually the same regardless
 * how many indexes are being built concurrently by the same job and regardless of index sizes. Formula for peak number of buffers in use is roughly
 * {@code 10 * numberOfPopulationWorkers} where numberOfPopulationWorkers is currently capped to 8. So given a buffer size of 1 MiB then maximum memory
 * usage for one population job (which can populate multiple index) is ~80 MiB.
 * <p>
 * Regarding block size: as entries gets written to a BlockStorage, they are buffered up to this size, then sorted and written out.
 * As blocks gets merged into bigger blocks, this is still the size of the read buffer for each block no matter its size.
 * Each thread has its own buffer when writing and each thread has {@link #mergeFactor} buffers when merging.
 * The memory usage will be at its biggest during merge and a total memory usage sum can be calculated like so:
 *
 * <pre>
 * blockSize * numberOfPopulationWorkers * {@link #mergeFactor}
 * </pre>
 * <p>
 * where {@link GraphDatabaseInternalSettings#index_population_workers} controls the number of population workers.
 *
 * @param <KEY>
 */
public abstract class BlockBasedIndexPopulator<KEY extends NativeIndexKey<KEY>> extends NativeIndexPopulator<KEY> {
    public static final Monitor NO_MONITOR = new Monitor.Adapter();

    private final boolean archiveFailedIndex;
    private final MemoryTracker memoryTracker;
    /**
     * When merging all blocks together the algorithm does multiple passes over the block storage, until the number of blocks reaches 1.
     * Every pass does one or more merges and every merge merges up to {@link #mergeFactor} number of blocks into one block,
     * i.e. the number of blocks shrinks by a factor {@link #mergeFactor} every pass, until one block is left.
     */
    private final int mergeFactor;

    private final Monitor monitor;
    // written to in a synchronized method when creating new thread-local instances, read from when population completes
    private final List<ThreadLocalBlockStorage> allScanUpdates = new CopyOnWriteArrayList<>();
    private final ThreadLocal<ThreadLocalBlockStorage> scanUpdates;
    private final ByteBufferFactory bufferFactory;
    private IndexUpdateStorage<KEY> externalUpdates;
    // written in a synchronized method when creating new thread-local instances, read when processing external updates
    private volatile boolean scanCompleted;
    private final CloseCancellation cancellation = new CloseCancellation();
    // Will be instantiated right before merging and can be used to neatly await merge to complete
    private volatile CountDownLatch mergeOngoingLatch;
    private IndexSample nonUniqueIndexSample;
    private final AtomicLong numberOfIndexUpdatesSinceSample = new AtomicLong();
    private IndexValueValidator validator;

    // progress state
    private final AtomicLong numberOfAppliedScanUpdates = new AtomicLong();
    private final AtomicLong numberOfAppliedExternalUpdates = new AtomicLong();

    BlockBasedIndexPopulator(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexLayout<KEY> layout,
            IndexDescriptor descriptor,
            boolean archiveFailedIndex,
            ByteBufferFactory bufferFactory,
            Config config,
            MemoryTracker memoryTracker,
            Monitor monitor,
            ImmutableSet<OpenOption> openOptions) {
        super(databaseIndexContext, indexFiles, layout, descriptor, openOptions);
        this.archiveFailedIndex = archiveFailedIndex;
        this.memoryTracker = memoryTracker;
        this.mergeFactor = config.get(GraphDatabaseInternalSettings.index_populator_merge_factor);
        this.monitor = monitor;
        this.scanUpdates = ThreadLocal.withInitial(this::newThreadLocalBlockStorage);
        this.bufferFactory = bufferFactory;
    }

    private synchronized ThreadLocalBlockStorage newThreadLocalBlockStorage() {
        Preconditions.checkState(!cancellation.cancelled(), "Already closed");
        Preconditions.checkState(!scanCompleted, "Scan has already been completed");
        try {
            int id = allScanUpdates.size();
            ThreadLocalBlockStorage blockStorage = new ThreadLocalBlockStorage(id);
            allScanUpdates.add(blockStorage);
            return blockStorage;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void create() throws IOException {
        if (archiveFailedIndex) {
            indexFiles.archiveIndex();
        }
        super.create();
        Path storeFile = indexFiles.getStoreFile();
        Path externalUpdatesFile = storeFile.resolveSibling(storeFile.getFileName() + ".ext");
        validator = instantiateValueValidator();
        externalUpdates = new IndexUpdateStorage<>(
                fileSystem,
                externalUpdatesFile,
                bufferFactory.globalAllocator(),
                smallerBufferSize(),
                layout,
                memoryTracker);
    }

    protected abstract IndexValueValidator instantiateValueValidator();

    private int smallerBufferSize() {
        return bufferFactory.bufferSize() / 2;
    }

    @Override
    public void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext) {
        if (!updates.isEmpty()) {
            BlockStorage<KEY, NullValue> blockStorage = null;
            for (IndexEntryUpdate<?> update : updates) {
                ValueIndexEntryUpdate<?> valueUpdate = (ValueIndexEntryUpdate<?>) update;
                if (ignoreStrategy.ignore(valueUpdate.values())) {
                    continue;
                }

                // Allocate the block storage lazily, so we don't end up with
                // an empty block storage in case all updates in the batch are ignored.
                // Producing an empty block storage is slightly illogical
                // and the code dealing with the block storage is not ready for this option.
                if (blockStorage == null) {
                    blockStorage = scanUpdates.get().blockStorage;
                }
                storeUpdate(update.getEntityId(), valueUpdate.values(), blockStorage);
            }
        }
    }

    private void storeUpdate(long entityId, Value[] values, BlockStorage<KEY, NullValue> blockStorage) {
        try {
            validator.validate(entityId, values);
            KEY key = layout.newKey();
            initializeKeyFromUpdate(key, entityId, values);
            blockStorage.add(key, NullValue.INSTANCE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private synchronized boolean markMergeStarted() {
        scanCompleted = true;
        if (cancellation.cancelled()) {
            return false;
        }
        mergeOngoingLatch = new CountDownLatch(1);
        return true;
    }

    @Override
    public void scanCompleted(
            PhaseTracker phaseTracker,
            PopulationWorkScheduler populationWorkScheduler,
            IndexEntryConflictHandler conflictHandler,
            CursorContext cursorContext)
            throws IndexEntryConflictException {
        if (!markMergeStarted()) {
            // This populator has already been closed, either from an external cancel or drop call.
            // Either way we're not supposed to do this merge.
            return;
        }

        try {
            monitor.scanCompletedStarted();
            phaseTracker.enterPhase(PhaseTracker.Phase.MERGE);
            if (!allScanUpdates.isEmpty()) {
                mergeScanUpdates(populationWorkScheduler);
            }

            externalUpdates.doneAdding();
            // don't merge and sort the external updates

            // Build the tree from the scan updates
            if (cancellation.cancelled()) {
                // Do one additional check before starting to write to the tree
                return;
            }
            phaseTracker.enterPhase(PhaseTracker.Phase.BUILD);
            Path storeFile = indexFiles.getStoreFile();
            Path duplicatesFile = storeFile.resolveSibling(storeFile.getFileName() + ".dup");
            int readBufferSize = smallerBufferSize();
            try (var allocator = bufferFactory.newLocalAllocator();
                    var indexKeyStorage = new IndexKeyStorage<>(
                            fileSystem, duplicatesFile, allocator, readBufferSize, layout, memoryTracker)) {
                RecordingConflictDetector<KEY> recordingConflictDetector =
                        new RecordingConflictDetector<>(!descriptor.isUnique(), indexKeyStorage);
                nonUniqueIndexSample = writeScanUpdatesToTree(
                        populationWorkScheduler, recordingConflictDetector, allocator, readBufferSize, cursorContext);

                // Apply the external updates
                phaseTracker.enterPhase(PhaseTracker.Phase.APPLY_EXTERNAL);
                writeExternalUpdatesToTree(recordingConflictDetector, cursorContext);

                // Verify uniqueness
                if (descriptor.isUnique()) {
                    try (IndexKeyStorage.KeyEntryCursor<KEY> allConflictingKeys =
                            recordingConflictDetector.allConflicts()) {
                        verifyUniqueKeys(allConflictingKeys, conflictHandler, cursorContext);
                    }
                }
            }

            // Flush the tree here, but keep its state as populating. This is done so that the "actual"
            // flush-and-mark-online during flip
            // becomes way faster and so the flip lock time is reduced.
            try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                flushTreeAndMarkAs(BYTE_POPULATING, flushEvent, cursorContext);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Got interrupted, so merge not completed", e);
        } catch (ExecutionException e) {
            // Propagating merge exception from other thread
            Throwable executionException = e.getCause();
            Exceptions.throwIfUnchecked(executionException);
            throw new RuntimeException(executionException);
        } finally {
            monitor.scanCompletedEnded();
            mergeOngoingLatch.countDown();
        }
    }

    private void mergeScanUpdates(PopulationWorkScheduler populationWorkScheduler)
            throws InterruptedException, ExecutionException, IOException {
        List<JobHandle<?>> mergeFutures = new ArrayList<>();
        for (ThreadLocalBlockStorage part : allScanUpdates) {
            BlockStorage<KEY, NullValue> scanUpdates = part.blockStorage;
            // Call doneAdding here so that the buffer it allocates if it needs to flush something will be shared with
            // other indexes
            scanUpdates.doneAdding();
            mergeFutures.add(
                    populationWorkScheduler.schedule(indexName -> "Block merging for '" + indexName + "'", () -> {
                        scanUpdates.merge(mergeFactor, cancellation);
                        return null;
                    }));
        }
        // Wait for merge jobs to finish and let potential exceptions in the merge threads have a chance to propagate
        for (JobHandle<?> mergeFuture : mergeFutures) {
            mergeFuture.get();
        }
    }

    /**
     * We will loop over all external updates once to add them to the tree. This is done without checking any uniqueness.
     * If index is a uniqueness index we will then loop over external updates again and for each ADD or CHANGED update
     * we will verify that those entries are unique in the tree and throw as soon as we find a duplicate.
     *
     * @throws IOException                 If something goes wrong while reading from index.
     * @throws IndexEntryConflictException If a duplicate is found.
     */
    private void writeExternalUpdatesToTree(
            RecordingConflictDetector<KEY> recordingConflictDetector, CursorContext cursorContext)
            throws IOException, IndexEntryConflictException {
        try (Writer<KEY, NullValue> writer = tree.writer(W_BATCHED_SINGLE_THREADED, cursorContext);
                IndexUpdateCursor<KEY, NullValue> updates = externalUpdates.reader()) {
            while (updates.next() && !cancellation.cancelled()) {
                switch (updates.updateMode()) {
                    case ADDED -> writeToTree(writer, recordingConflictDetector, updates.key());
                    case REMOVED -> writer.remove(updates.key());
                    case CHANGED -> {
                        writer.remove(updates.key());
                        writeToTree(writer, recordingConflictDetector, updates.key2());
                    }
                    default -> throw new IllegalArgumentException("Unknown update mode " + updates.updateMode());
                }
                numberOfAppliedExternalUpdates.incrementAndGet();
                numberOfIndexUpdatesSinceSample.incrementAndGet();
            }
        }
    }

    private void verifyUniqueKeys(
            IndexKeyStorage.KeyEntryCursor<KEY> allConflictingKeys,
            IndexEntryConflictHandler conflictHandler,
            CursorContext cursorContext)
            throws IOException, IndexEntryConflictException {
        while (allConflictingKeys.next() && !cancellation.cancelled()) {
            KEY key = allConflictingKeys.key();
            key.setCompareId(false);
            try (var seeker = tree.seek(key, key, cursorContext)) {
                verifyUniqueSeek(seeker, conflictHandler, cursorContext);
            }
        }
    }

    private void verifyUniqueSeek(
            Seeker<KEY, NullValue> seek, IndexEntryConflictHandler conflictHandler, CursorContext cursorContext)
            throws IOException, IndexEntryConflictException {
        if (seek != null) {
            if (seek.next()) {
                KEY key = seek.key();
                long firstEntityId = key.getEntityId();
                while (seek.next()) {
                    long otherEntityId = key.getEntityId();
                    var values = key.asValues();
                    switch (conflictHandler.indexEntryConflict(firstEntityId, otherEntityId, values)) {
                        case THROW -> throw new IndexEntryConflictException(
                                descriptor.schema(), firstEntityId, otherEntityId, values);
                        case DELETE -> deleteConflict(seek.key(), cursorContext);
                    }
                }
            }
        }
    }

    private void deleteConflict(KEY key, CursorContext cursorContext) throws IOException {
        try (var writer = tree.writer(cursorContext)) {
            writer.remove(key);
        }
    }

    private IndexSample writeScanUpdatesToTree(
            PopulationWorkScheduler populationWorkScheduler,
            RecordingConflictDetector<KEY> recordingConflictDetector,
            Allocator allocator,
            int bufferSize,
            CursorContext cursorContext)
            throws IOException, IndexEntryConflictException {
        if (allScanUpdates.isEmpty()) {
            return new IndexSample(0, 0, 0);
        }

        // Merge the (sorted) scan updates from all the different threads in pairs until only one stream remain,
        // and direct that stream towards the tree writer (which itself is only single threaded)
        try (var readBuffers = new CompositeBuffer();
                var singleBlockScopedBuffer = allocator.allocate((int) kibiBytes(8), memoryTracker)) {
            // Get the initial list of parts
            List<BlockEntryCursor<KEY, NullValue>> parts = new ArrayList<>();
            for (ThreadLocalBlockStorage part : allScanUpdates) {
                var readScopedBuffer = allocator.allocate(bufferSize, memoryTracker);
                readBuffers.addBuffer(readScopedBuffer);
                try (var reader = part.blockStorage.reader(true)) {
                    // reader has a channel open, but only for the purpose of traversing the blocks.
                    // nextBlock will open its own channel so it's OK to close the reader after getting that block
                    parts.add(reader.nextBlock(readScopedBuffer));
                    Preconditions.checkState(
                            reader.nextBlock(singleBlockScopedBuffer) == null,
                            "Final BlockStorage had multiple blocks");
                }
            }

            Comparator<KEY> samplingComparator = descriptor.isUnique() ? null : layout::compareValue;
            try (var merger = new PartMerger<>(
                            populationWorkScheduler,
                            parts,
                            layout,
                            samplingComparator,
                            cancellation,
                            PartMerger.DEFAULT_BATCH_SIZE);
                    var allEntries = merger.startMerge();
                    var writer = tree.writer(W_BATCHED_SINGLE_THREADED | W_SPLIT_KEEP_ALL_LEFT, cursorContext)) {
                while (allEntries.next() && !cancellation.cancelled()) {
                    writeToTree(writer, recordingConflictDetector, allEntries.key());
                    numberOfAppliedScanUpdates.incrementAndGet();
                }
                return descriptor.isUnique() ? null : allEntries.buildIndexSample();
            }
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
        if (scanCompleted) {
            // Will need the reader from newReader, which a sub-class of this class implements
            return new DelegatingIndexUpdater(super.newPopulatingUpdater(cursorContext)) {
                @Override
                public void process(IndexEntryUpdate<?> update) throws IndexEntryConflictException {
                    ValueIndexEntryUpdate<?> valueUpdate = asValueUpdate(update);
                    validateUpdate(valueUpdate);
                    if (ignoreStrategy.ignore(valueUpdate)) {
                        return;
                    }
                    numberOfIndexUpdatesSinceSample.incrementAndGet();
                    super.process(valueUpdate);
                }
            };
        }

        return new IndexUpdater() {
            private volatile boolean closed;

            @Override
            public void process(IndexEntryUpdate<?> update) {
                assertOpen();
                ValueIndexEntryUpdate<?> valueUpdate = asValueUpdate(update);
                try {
                    validateUpdate(valueUpdate);
                    if (ignoreStrategy.ignore(valueUpdate)) {
                        return;
                    }
                    // A change might just be an add or a remove for indexes not supporting all value types.
                    // Let's do any necessary conversion now and store it as the actual update the index needs.
                    valueUpdate = ignoreStrategy.toEquivalentUpdate((ValueIndexEntryUpdate<?>) update);
                    externalUpdates.add(valueUpdate);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void close() {
                closed = true;
            }

            private void assertOpen() {
                if (closed) {
                    throw new IllegalStateException("Updater has been closed");
                }
            }
        };
    }

    private void validateUpdate(ValueIndexEntryUpdate<?> update) {
        if (update.updateMode() != UpdateMode.REMOVED) {
            validator.validate(update.getEntityId(), update.values());
        }
    }

    @Override
    public synchronized void drop() {
        runAll(
                "Failed while trying to drop index",
                this::closeBlockStorage /* Close internal resources */,
                super::drop /* Super drop will close inherited resources */);
    }

    @Override
    public synchronized void close(boolean populationCompletedSuccessfully, CursorContext cursorContext) {
        runAll(
                "Failed while trying to close index",
                this::closeBlockStorage /* Close internal resources */,
                () -> super.close(
                        populationCompletedSuccessfully,
                        cursorContext) /* Super close will close inherited resources */);
    }

    // Always called from synchronized method
    private void closeBlockStorage() {
        // This method may be called while scanCompleted is running. This could be a drop or shutdown(?) which happens
        // when this population
        // is in its final stages. scanCompleted merges things in multiple threads. Those threads will abort when they
        // see that setCancel
        // has been called.
        cancellation.setCancel();

        // If there's a merge concurrently running it will very soon notice the cancel request and abort whatever it's
        // doing as soon as it can.
        // Let's wait for that merge to be fully aborted by simply waiting for the merge latch.
        if (mergeOngoingLatch != null) {
            try {
                // We want to await any ongoing merge because it becomes problematic to close the channels otherwise
                mergeOngoingLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // We still want to go ahead and try to close things properly, so get by only restoring the interrupt
                // flag on the thread
            }
        }

        List<Closeable> toClose = allScanUpdates.stream()
                .map(local -> local.blockStorage)
                .collect(Collectors.toCollection(ArrayList::new));
        toClose.add(externalUpdates);
        IOUtils.closeAllUnchecked(toClose);
    }

    @Override
    public PopulationProgress progress(PopulationProgress scanProgress) {
        // A general note on scanProgress.getTotal(). Before the scan is completed most progress parts will base their
        // estimates on that value.
        // It is known that it may be slightly higher since it'll be based on store high-id, not the actual count.
        // This is fine, but it creates this small "jump" in the progress in the middle somewhere when it switches from
        // scan to merge.
        // This also exists in the most basic population progress reports, but there it will be less visible since it
        // will jump from
        // some close-to-100 percentage to 100% and ONLINE.

        // This progress report will consist of a couple of smaller parts, weighted differently based on empirically
        // collected values.
        // The weights will not be absolutely correct in all environments, but they don't have to be either, it will
        // just result in some
        // slices of the percentage progression range progressing at slightly different paces. However, progression of
        // progress reporting
        // naturally fluctuates anyway due to data set and I/O etc. so this is not an actual problem.
        PopulationProgress.MultiBuilder builder = PopulationProgress.multiple();

        // Add scan progress (this one weights a bit heavier than the others)
        builder.add(scanProgress, 4);

        // Add merge progress
        if (!allScanUpdates.isEmpty()) {
            // The parts are merged in parallel so just take the first one and it will represent the whole merge
            // progress.
            // It will be fairly accurate, but slightly off sometimes if other threads gets scheduling problems, i.e. if
            // this part
            // finish far apart from others.
            long completed = 0;
            long total = 0;
            if (scanCompleted) {
                // We know the actual entry count to write during merge since we have been monitoring those values
                ThreadLocalBlockStorage part = first(allScanUpdates);
                completed = part.entriesMerged.get();
                total = part.totalEntriesToMerge;
            }
            builder.add(PopulationProgress.single(completed, total), 1);
        }

        // Add tree building incl. external updates
        PopulationProgress treeBuildProgress;
        if (allScanUpdates.stream().allMatch(part -> part.mergeStarted)) {
            long entryCount =
                    allScanUpdates.stream().mapToLong(part -> part.count).sum() + externalUpdates.count();
            treeBuildProgress = PopulationProgress.single(
                    numberOfAppliedScanUpdates.get() + numberOfAppliedExternalUpdates.get(), entryCount);
        } else {
            treeBuildProgress = PopulationProgress.NONE;
        }
        builder.add(treeBuildProgress, 2);

        return builder.build();
    }

    /**
     * Write key and value to tree and record duplicates if any.
     */
    private void writeToTree(
            Writer<KEY, NullValue> writer, RecordingConflictDetector<KEY> recordingConflictDetector, KEY key)
            throws IndexEntryConflictException {
        recordingConflictDetector.controlConflictDetection(key);
        writer.merge(key, NullValue.INSTANCE, recordingConflictDetector);
        handleMergeConflict(writer, recordingConflictDetector, key);
    }

    /**
     * Will check if recording conflict detector saw a conflict. If it did, that conflict has been recorded and we will verify uniqueness for this
     * value later on. But for now we try and insert conflicting value again but with a relaxed uniqueness constraint. Insert is done with a throwing
     * conflict checker which means it will throw if we see same value AND same id in one key.
     */
    private void handleMergeConflict(
            Writer<KEY, NullValue> writer, RecordingConflictDetector<KEY> recordingConflictDetector, KEY key)
            throws IndexEntryConflictException {
        if (recordingConflictDetector.wasConflicting()) {
            // Report conflict
            KEY copy = layout.newKey();
            layout.copyKey(key, copy);
            recordingConflictDetector.reportConflict(copy);

            // Insert and overwrite with relaxed uniqueness constraint
            recordingConflictDetector.relaxUniqueness(key);
            writer.put(key, NullValue.INSTANCE);
        }
    }

    @Override
    IndexSample buildNonUniqueIndexSample(CursorContext cursorContext) {
        return new IndexSample(
                nonUniqueIndexSample.indexSize(),
                nonUniqueIndexSample.uniqueValues(),
                nonUniqueIndexSample.sampleSize(),
                numberOfIndexUpdatesSinceSample.get());
    }

    /**
     * Keeps track of a {@link BlockStorage} instance as well as monitoring some aspects of it to be able to provide a fairly accurate
     * progress report from {@link BlockBasedIndexPopulator#progress(PopulationProgress)}.
     */
    private class ThreadLocalBlockStorage extends BlockStorage.Monitor.Delegate {
        private final BlockStorage<KEY, NullValue> blockStorage;
        private volatile long count;
        private volatile boolean mergeStarted;
        private volatile long totalEntriesToMerge;
        private final AtomicLong entriesMerged = new AtomicLong();

        ThreadLocalBlockStorage(int id) throws IOException {
            super(monitor);
            Path storeFile = indexFiles.getStoreFile();
            Path blockFile = storeFile.resolveSibling(storeFile.getFileName() + ".scan-" + id);
            this.blockStorage = new BlockStorage<>(layout, bufferFactory, fileSystem, blockFile, this, memoryTracker);
        }

        @Override
        public void mergeStarted(long entryCount, long totalEntriesToWriteDuringMerge) {
            super.mergeStarted(entryCount, totalEntriesToWriteDuringMerge);
            this.count = entryCount;
            this.totalEntriesToMerge = totalEntriesToWriteDuringMerge;
            this.mergeStarted = true;
        }

        @Override
        public void entriesMerged(int entries) {
            super.entriesMerged(entries);
            entriesMerged.addAndGet(entries);
        }
    }

    private static class CloseCancellation implements BlockStorage.Cancellation {
        private volatile boolean cancelled;

        void setCancel() {
            this.cancelled = true;
        }

        @Override
        public boolean cancelled() {
            return cancelled;
        }
    }

    private static class RecordingConflictDetector<KEY extends NativeIndexKey<KEY>>
            extends ConflictDetectingValueMerger<KEY, KEY> {
        private final IndexKeyStorage<KEY> allConflictingKeys;

        RecordingConflictDetector(boolean compareEntityIds, IndexKeyStorage<KEY> indexKeyStorage) {
            super(compareEntityIds);
            allConflictingKeys = indexKeyStorage;
        }

        @Override
        void doReportConflict(long existingNodeId, long addedNodeId, KEY conflictingKey) {
            try {
                allConflictingKeys.add(conflictingKey);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        IndexKeyStorage.KeyEntryCursor<KEY> allConflicts() throws IOException {
            allConflictingKeys.doneAdding();
            return allConflictingKeys.reader();
        }

        void relaxUniqueness(KEY key) {
            key.setCompareId(true);
        }
    }

    private static class CompositeBuffer implements AutoCloseable {
        private final Collection<AutoCloseable> buffers = new ArrayList<>();

        public void addBuffer(AutoCloseable buffer) {
            buffers.add(buffer);
        }

        @Override
        public void close() {
            closeAllUnchecked(buffers);
        }
    }

    public interface Monitor extends BlockStorage.Monitor {
        void scanCompletedStarted();

        void scanCompletedEnded();

        class Adapter extends BlockStorage.Monitor.Adapter implements Monitor {
            @Override
            public void scanCompletedStarted() {}

            @Override
            public void scanCompletedEnded() {}
        }
    }
}
