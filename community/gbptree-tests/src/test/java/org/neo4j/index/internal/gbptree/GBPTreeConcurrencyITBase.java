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
package org.neo4j.index.internal.gbptree;

import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.consistencyCheckStrict;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.test.utils.PageCacheConfig.config;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

/**
 * From a range of keys two disjunct sets are generated, "toAdd" and "toRemove".
 * In each "iteration" writer will grab enough work from toAdd and toRemove to fill up one "batch".
 * The batch will be applied to the GB+Tree during this iteration. The batch is also used to update
 * a set of keys that all readers MUST see.
 *
 * Readers are allowed to see more keys because they race with concurrent insertions, but they should
 * at least see every key that has been inserted in previous iterations or not yet removed in current
 * or previous iterations.
 *
 * The {@link TestCoordinator} is responsible for "planning" the execution of the test. It generates
 * toAdd and toRemove, prepare the GB+Tree with entries and serve readers and writer with information
 * about what they should do next.
 */
@EphemeralTestDirectoryExtension
@ExtendWith(RandomExtension.class)
public abstract class GBPTreeConcurrencyITBase<KEY, VALUE> {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private RandomSupport random;

    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();

    private TestLayout<KEY, VALUE> layout;
    private GBPTree<KEY, VALUE> index;
    private PageCache pageCache;
    private final ExecutorService threadPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private GBPTree<KEY, VALUE> createIndex() {
        int pageSize = 512;
        pageCache = PageCacheSupportExtension.getPageCache(
                fileSystem, config().withPageSize(pageSize).withAccessChecks(true));
        var openOptions = getOpenOptions();
        layout = getLayout(random, GBPTreeTestUtil.calculatePayloadSize(pageCache, openOptions));
        return this.index = new GBPTreeBuilder<>(pageCache, fileSystem, testDirectory.file("index"), layout)
                .with(openOptions)
                .build();
    }

    protected abstract TestLayout<KEY, VALUE> getLayout(RandomSupport random, int payloadSize);

    ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    @AfterEach
    void consistencyCheckAndClose() throws IOException {
        threadPool.shutdownNow();
        consistencyCheckStrict(index);
        index.close();
        pageCache.close();
    }

    @Test
    void shouldReadForwardCorrectlyWithConcurrentInsert() throws Throwable {
        TestCoordinator testCoordinator = new TestCoordinator(random.random(), true, 1);
        shouldReadCorrectlyWithConcurrentUpdates(testCoordinator);
    }

    @Test
    void shouldPartitionedReadForwardCorrectlyWithConcurrentInsert() throws Throwable {
        TestCoordinator testCoordinator = new TestCoordinator(random.random(), true, 1, true);
        shouldReadCorrectlyWithConcurrentUpdates(testCoordinator);
    }

    @Test
    void shouldReadBackwardCorrectlyWithConcurrentInsert() throws Throwable {
        TestCoordinator testCoordinator = new TestCoordinator(random.random(), false, 1);
        shouldReadCorrectlyWithConcurrentUpdates(testCoordinator);
    }

    @Test
    void shouldReadForwardCorrectlyWithConcurrentRemove() throws Throwable {
        TestCoordinator testCoordinator = new TestCoordinator(random.random(), true, 0);
        shouldReadCorrectlyWithConcurrentUpdates(testCoordinator);
    }

    @Test
    void shouldPartitionedReadForwardCorrectlyWithConcurrentRemove() throws Throwable {
        TestCoordinator testCoordinator = new TestCoordinator(random.random(), true, 0, true);
        shouldReadCorrectlyWithConcurrentUpdates(testCoordinator);
    }

    @Test
    void shouldReadBackwardCorrectlyWithConcurrentRemove() throws Throwable {
        TestCoordinator testCoordinator = new TestCoordinator(random.random(), false, 0);
        shouldReadCorrectlyWithConcurrentUpdates(testCoordinator);
    }

    @Test
    void shouldReadForwardCorrectlyWithConcurrentUpdates() throws Throwable {
        TestCoordinator testCoordinator = new TestCoordinator(random.random(), true, 0.5);
        shouldReadCorrectlyWithConcurrentUpdates(testCoordinator);
    }

    @Test
    void shouldPartitionedReadForwardCorrectlyWithConcurrentUpdates() throws Throwable {
        TestCoordinator testCoordinator = new TestCoordinator(random.random(), true, 0.5, true);
        shouldReadCorrectlyWithConcurrentUpdates(testCoordinator);
    }

    @Test
    void shouldReadBackwardCorrectlyWithConcurrentUpdates() throws Throwable {
        TestCoordinator testCoordinator = new TestCoordinator(random.random(), false, 0.5);
        shouldReadCorrectlyWithConcurrentUpdates(testCoordinator);
    }

    private void shouldReadCorrectlyWithConcurrentUpdates(TestCoordinator testCoordinator) throws Throwable {
        // Readers config
        int readers = max(1, Runtime.getRuntime().availableProcessors() - 1);

        // Thread communication
        CountDownLatch readerReadySignal = new CountDownLatch(readers);
        CountDownLatch readerStartSignal = new CountDownLatch(1);
        AtomicBoolean endSignal = testCoordinator.endSignal();
        AtomicBoolean failHalt = new AtomicBoolean(); // Readers signal to writer that there is a failure
        AtomicReference<Throwable> readerError = new AtomicReference<>();

        // GIVEN
        index = createIndex();
        testCoordinator.prepare(index);

        // WHEN starting the readers
        RunnableReader readerTask = new RunnableReader(
                testCoordinator, readerReadySignal, readerStartSignal, endSignal, failHalt, readerError);
        for (int i = 0; i < readers; i++) {
            threadPool.submit(readerTask);
        }

        // and starting the checkpointer
        threadPool.submit(checkpointThread(endSignal, readerError, failHalt));

        // and starting the writer
        try {
            write(testCoordinator, readerReadySignal, readerStartSignal, endSignal, failHalt);
        } finally {
            // THEN no reader should have failed by the time we have finished all the scheduled updates.
            // A successful read means that all results were ordered and we saw all inserted values and
            // none of the removed values at the point of making the seek call.
            endSignal.set(true);
            threadPool.shutdown();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
            if (readerError.get() != null) {
                //noinspection ThrowFromFinallyBlock
                throw readerError.get();
            }
        }
    }

    private class TestCoordinator implements Supplier<ReaderInstruction> {
        private final Random random;

        // Range
        final long minRange = 0;
        final long maxRange = 1 << 13; // 8192

        // Instructions for writer
        private final int writeBatchSize;

        // Instructions for reader
        private final boolean forwardsSeek;
        private final double writePercentage;
        private final AtomicReference<ReaderInstruction> currentReaderInstruction;
        private final boolean partitionedSeek;
        SortedSet<Long> readersShouldSee;

        // Progress
        private final AtomicBoolean endSignal;

        // Control for ADD and REMOVE
        Queue<Long> toRemove = new LinkedList<>();
        Queue<Long> toAdd = new LinkedList<>();
        List<UpdateOperation> updatesForNextIteration = new ArrayList<>();

        TestCoordinator(Random random, boolean forwardsSeek, double writePercentage) {
            this(random, forwardsSeek, writePercentage, false);
        }

        TestCoordinator(Random random, boolean forwardsSeek, double writePercentage, boolean partitionedSeek) {
            this.partitionedSeek = partitionedSeek;
            this.endSignal = new AtomicBoolean();
            this.random = random;
            this.forwardsSeek = forwardsSeek;
            this.writePercentage = writePercentage;
            this.writeBatchSize = random.nextInt(990) + 10; // 10-999
            currentReaderInstruction = new AtomicReference<>();
            Comparator<Long> comparator = forwardsSeek ? Comparator.naturalOrder() : Comparator.reverseOrder();
            readersShouldSee = new TreeSet<>(comparator);
        }

        List<Long> shuffleToNewList(List<Long> sourceList, Random random) {
            List<Long> shuffledList = new ArrayList<>(sourceList);
            Collections.shuffle(shuffledList, random);
            return shuffledList;
        }

        void prepare(GBPTree<KEY, VALUE> index) throws IOException {
            prepareIndex(index, readersShouldSee, toRemove, toAdd, random);
            iterationFinished();
        }

        void prepareIndex(
                GBPTree<KEY, VALUE> index,
                Set<Long> dataInIndex,
                Queue<Long> toRemove,
                Queue<Long> toAdd,
                Random random)
                throws IOException {
            List<Long> fullRange = LongStream.range(minRange, maxRange).boxed().collect(Collectors.toList());
            List<Long> rangeOutOfOrder = shuffleToNewList(fullRange, random);
            try (Writer<KEY, VALUE> writer = index.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                for (Long key : rangeOutOfOrder) {
                    boolean addForRemoval = random.nextDouble() > writePercentage;
                    if (addForRemoval) {
                        writer.put(key(key), value(key));
                        dataInIndex.add(key);
                        toRemove.add(key);
                    } else {
                        toAdd.add(key);
                    }
                }
            }
        }

        void iterationFinished() {
            // Create new set to not modify set that readers use concurrently
            readersShouldSee = new TreeSet<>(readersShouldSee);
            updateRecentlyInsertedData(readersShouldSee, updatesForNextIteration);
            updatesForNextIteration = generateUpdatesForNextIteration();
            updateWithSoonToBeRemovedData(readersShouldSee, updatesForNextIteration);
            currentReaderInstruction.set(newReaderInstruction(minRange, maxRange, readersShouldSee));
        }

        void updateRecentlyInsertedData(Set<Long> readersShouldSee, List<UpdateOperation> updateBatch) {
            updateBatch.stream().filter(UpdateOperation::isInsert).forEach(uo -> uo.applyToSet(readersShouldSee));
        }

        void updateWithSoonToBeRemovedData(Set<Long> readersShouldSee, List<UpdateOperation> updateBatch) {
            updateBatch.stream().filter(uo -> !uo.isInsert()).forEach(uo -> uo.applyToSet(readersShouldSee));
        }

        private ReaderInstruction newReaderInstruction(long minRange, long maxRange, Set<Long> readersShouldSee) {
            return forwardsSeek
                    ? new ReaderInstruction(minRange, maxRange, readersShouldSee, partitionedSeek)
                    : new ReaderInstruction(maxRange - 1, minRange, readersShouldSee, partitionedSeek);
        }

        private List<UpdateOperation> generateUpdatesForNextIteration() {
            List<UpdateOperation> updateOperations = new ArrayList<>();
            if (toAdd.isEmpty() && toRemove.isEmpty()) {
                endSignal.set(true);
                return updateOperations;
            }

            int operationsInIteration = readersShouldSee.size() < 1000 ? 100 : readersShouldSee.size() / 10;
            int count = 0;
            while (count < operationsInIteration && (!toAdd.isEmpty() || !toRemove.isEmpty())) {
                UpdateOperation operation;
                if (toAdd.isEmpty()) {
                    operation = new RemoveOperation(toRemove.poll());
                } else if (toRemove.isEmpty()) {
                    operation = new PutOperation(toAdd.poll());
                } else {
                    boolean remove = random.nextDouble() > writePercentage;
                    if (remove) {
                        operation = new RemoveOperation(toRemove.poll());
                    } else {
                        operation = new PutOperation(toAdd.poll());
                    }
                }
                updateOperations.add(operation);
                count++;
            }
            return updateOperations;
        }

        Iterable<UpdateOperation> nextToWrite() {
            return updatesForNextIteration;
        }

        @Override
        public ReaderInstruction get() {
            return currentReaderInstruction.get();
        }

        AtomicBoolean endSignal() {
            return endSignal;
        }

        int writeBatchSize() {
            return writeBatchSize;
        }

        boolean isReallyExpected(long nextToSee) {
            return readersShouldSee.contains(nextToSee);
        }
    }

    private abstract class UpdateOperation {
        final long key;

        UpdateOperation(long key) {
            this.key = key;
        }

        abstract void apply(Writer<KEY, VALUE> writer) throws IOException;

        abstract void applyToSet(Set<Long> set);

        abstract boolean isInsert();
    }

    private class PutOperation extends UpdateOperation {
        PutOperation(long key) {
            super(key);
        }

        @Override
        void apply(Writer<KEY, VALUE> writer) {
            writer.put(key(key), value(key));
        }

        @Override
        void applyToSet(Set<Long> set) {
            set.add(key);
        }

        @Override
        boolean isInsert() {
            return true;
        }
    }

    private class RemoveOperation extends UpdateOperation {
        RemoveOperation(long key) {
            super(key);
        }

        @Override
        void apply(Writer<KEY, VALUE> writer) {
            writer.remove(key(key));
        }

        @Override
        void applyToSet(Set<Long> set) {
            set.remove(key);
        }

        @Override
        boolean isInsert() {
            return false;
        }
    }

    private void write(
            TestCoordinator testCoordinator,
            CountDownLatch readerReadySignal,
            CountDownLatch readerStartSignal,
            AtomicBoolean endSignal,
            AtomicBoolean failHalt)
            throws InterruptedException, IOException {
        assertTrue(readerReadySignal.await(60, SECONDS)); // Ready, set...
        readerStartSignal.countDown(); // GO!

        while (!failHalt.get() && !endSignal.get()) {
            writeOneIteration(testCoordinator, failHalt);
            testCoordinator.iterationFinished();
        }
    }

    private void writeOneIteration(TestCoordinator testCoordinator, AtomicBoolean failHalt)
            throws IOException, InterruptedException {
        int batchSize = testCoordinator.writeBatchSize();
        Iterable<UpdateOperation> toWrite = testCoordinator.nextToWrite();
        Iterator<UpdateOperation> toWriteIterator = toWrite.iterator();
        while (toWriteIterator.hasNext()) {
            try (Writer<KEY, VALUE> writer = index.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                int inBatch = 0;
                while (toWriteIterator.hasNext() && inBatch < batchSize) {
                    UpdateOperation operation = toWriteIterator.next();
                    operation.apply(writer);
                    if (failHalt.get()) {
                        break;
                    }
                    inBatch++;
                }
            }
            // Sleep to allow checkpointer to step in
            MILLISECONDS.sleep(1);
        }
    }

    private class RunnableReader implements Runnable {
        private final CountDownLatch readerReadySignal;
        private final CountDownLatch readerStartSignal;
        private final AtomicBoolean endSignal;
        private final AtomicBoolean failHalt;
        private final AtomicReference<Throwable> readerError;
        private final TestCoordinator testCoordinator;
        private final boolean useReusableSeeker;

        RunnableReader(
                TestCoordinator testCoordinator,
                CountDownLatch readerReadySignal,
                CountDownLatch readerStartSignal,
                AtomicBoolean endSignal,
                AtomicBoolean failHalt,
                AtomicReference<Throwable> readerError) {
            this.readerReadySignal = readerReadySignal;
            this.readerStartSignal = readerStartSignal;
            this.endSignal = endSignal;
            this.failHalt = failHalt;
            this.readerError = readerError;
            this.testCoordinator = testCoordinator;
            this.useReusableSeeker = random.nextBoolean();
        }

        @Override
        public void run() {
            try (Seeker<KEY, VALUE> reusableSeeker = useReusableSeeker ? index.allocateSeeker(NULL_CONTEXT) : null) {
                readerReadySignal.countDown(); // Ready, set...
                readerStartSignal.await(); // GO!

                while (!endSignal.get() && !failHalt.get()) {
                    doRead(reusableSeeker);
                }
            } catch (Throwable e) {
                readerError.set(e);
                failHalt.set(true);
            }
        }

        private void doRead(Seeker<KEY, VALUE> reusableSeeker) throws IOException {
            ReaderInstruction readerInstruction = testCoordinator.get();
            Iterator<Long> expectToSee = readerInstruction.expectToSee().iterator();
            long start = readerInstruction.start();
            long end = readerInstruction.end();
            boolean forward = start <= end;
            Seeker<KEY, VALUE> cursor = null;
            try {
                cursor = readerInstruction.seek(index, reusableSeeker);
                if (expectToSee.hasNext()) {
                    long nextToSee = expectToSee.next();
                    while (cursor.next()) {
                        // Actual
                        long lastSeenKey = keySeed(cursor.key());
                        long lastSeenValue = valueSeed(cursor.value());

                        if (lastSeenKey != lastSeenValue) {
                            fail(String.format(
                                    "Read mismatching key value pair, key=%d, value=%d%n", lastSeenKey, lastSeenValue));
                        }

                        while ((forward && lastSeenKey > nextToSee) || (!forward && lastSeenKey < nextToSee)) {
                            if (testCoordinator.isReallyExpected(nextToSee)) {
                                fail(String.format(
                                        "Expected to see %d but went straight to %d. ", nextToSee, lastSeenKey));
                            }
                            if (expectToSee.hasNext()) {
                                nextToSee = expectToSee.next();
                            } else {
                                break;
                            }
                        }
                        if (nextToSee == lastSeenKey) {
                            if (expectToSee.hasNext()) {
                                nextToSee = expectToSee.next();
                            } else {
                                break;
                            }
                        }
                    }
                }
            } finally {
                // If we're using a reusable seeker then don't close it now (we'll reuse it for next seek),
                // instead it will be closed when this thread is done
                IOUtils.closeAll(useReusableSeeker ? null : cursor);
            }
        }
    }

    private Runnable checkpointThread(
            AtomicBoolean endSignal, AtomicReference<Throwable> readerError, AtomicBoolean failHalt) {
        return () -> {
            while (!endSignal.get()) {
                try {
                    index.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
                    // Sleep a little in between checkpoints
                    MILLISECONDS.sleep(20L);
                } catch (Throwable e) {
                    readerError.set(e);
                    failHalt.set(true);
                }
            }
        };
    }

    private class ReaderInstruction {
        private final long startRange;
        private final long endRange;
        private final Set<Long> expectToSee;
        private final boolean partitionedSeek;

        ReaderInstruction(long startRange, long endRange, Set<Long> expectToSee, boolean partitionedSeek) {
            this.startRange = startRange;
            this.endRange = endRange;
            this.expectToSee = expectToSee;
            this.partitionedSeek = partitionedSeek;
        }

        long start() {
            return startRange;
        }

        long end() {
            return endRange;
        }

        Set<Long> expectToSee() {
            return expectToSee;
        }

        Seeker<KEY, VALUE> seek(GBPTree<KEY, VALUE> tree, Seeker<KEY, VALUE> reusableSeeker) throws IOException {
            KEY from = key(start());
            KEY to = key(end());
            if (partitionedSeek) {
                List<KEY> partitionEdges = tree.partitionedSeek(from, to, 10, NULL_CONTEXT);
                List<Seeker<KEY, VALUE>> partitions = new ArrayList<>(partitionEdges.size() - 1);
                for (int i = 0; i < partitionEdges.size() - 1; i++) {
                    partitions.add(tree.seek(partitionEdges.get(i), partitionEdges.get(i + 1), NULL_CONTEXT));
                }
                return new PartitionBridgingSeeker<>(partitions);
            }

            if (reusableSeeker != null) {
                return tree.seek(reusableSeeker, from, to);
            }
            return tree.seek(from, to, NULL_CONTEXT);
        }
    }

    private static class PartitionBridgingSeeker<KEY, VALUE> implements Seeker<KEY, VALUE> {
        private final Collection<Seeker<KEY, VALUE>> partitionsToClose;
        private final Iterator<Seeker<KEY, VALUE>> partitions;
        private Seeker<KEY, VALUE> current;

        PartitionBridgingSeeker(Collection<Seeker<KEY, VALUE>> partitions) {
            this.partitionsToClose = partitions;
            this.partitions = partitions.iterator();
            current = this.partitions.next();
        }

        @Override
        public boolean next() throws IOException {
            while (true) {
                if (current.next()) {
                    return true;
                }
                if (partitions.hasNext()) {
                    current.close();
                    current = partitions.next();
                } else {
                    break;
                }
            }
            return false;
        }

        @Override
        public KEY key() {
            return current.key();
        }

        @Override
        public VALUE value() {
            return current.value();
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeAll(partitionsToClose);
        }
    }

    private KEY key(long seed) {
        return layout.key(seed);
    }

    private VALUE value(long seed) {
        return layout.value(seed);
    }

    private long keySeed(KEY key) {
        return layout.keySeed(key);
    }

    private long valueSeed(VALUE value) {
        return layout.valueSeed(value);
    }
}
