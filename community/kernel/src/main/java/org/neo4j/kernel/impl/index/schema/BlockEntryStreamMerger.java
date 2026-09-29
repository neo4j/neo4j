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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.util.Preconditions;

/**
 * A merger of {@link BlockEntry} in a streaming fashion. It takes as input one or more {@link BlockEntryCursor}, merges them
 * and on the fly adds batches of merged results to its output, which is consumable by either other {@link BlockEntryStreamMerger}
 * or a final consumer.
 *
 * Batches are recycled: once the consuming thread has read past a batch it is handed back to the merging thread, so the merge
 * runs with a fixed set of {@code queueSize + 2} batches and no per-entry allocation. Keys are copied into the batches since
 * the input cursors are free to reuse their key instances between calls to {@link BlockEntryCursor#next()}. Values are
 * stored by reference, which relies on the input cursors handing out value instances that stay valid, e.g. the singleton
 * {@link NullValue} that index population uses.
 *
 * For convenience instances are themselves valid as input into another {@link BlockEntryStreamMerger}.
 */
class BlockEntryStreamMerger<KEY, VALUE> implements BlockEntryCursor<KEY, VALUE>, Callable<Void> {
    static final int QUEUE_SIZE = 10;
    private static final int BATCHES_PER_CHUNK = 512;

    private final List<BlockEntryCursor<KEY, VALUE>> input;
    private final Layout<KEY, VALUE> layout;
    private final BlockStorage.Cancellation cancellation;
    private final ArrayBlockingQueue<Batch<KEY, VALUE>> mergedOutput;
    // Batches the consumer has finished reading, ready to be filled again by the merging thread
    private final ArrayBlockingQueue<Batch<KEY, VALUE>> freeBatches;
    private final int batchSize;
    private final int maxBatches;
    private final Comparator<KEY> samplingComparator;
    private final KEY prevKey;
    private boolean hasPrevKey;
    private long sampledValues;
    private long uniqueValues;
    private int allocatedBatches;
    private volatile boolean halted;
    // This cursor will be used by the single thread reading from this merged stream
    private Batch<KEY, VALUE> currentOutput;

    BlockEntryStreamMerger(
            List<BlockEntryCursor<KEY, VALUE>> input,
            Layout<KEY, VALUE> layout,
            Comparator<KEY> samplingComparator,
            BlockStorage.Cancellation cancellation,
            int batchSize,
            int queueSize) {
        this.input = input;
        this.layout = layout;
        this.cancellation = cancellation;
        this.batchSize = batchSize;
        // One batch being filled, queueSize batches in the queue and one being consumed
        this.maxBatches = queueSize + 2;
        this.mergedOutput = new ArrayBlockingQueue<>(queueSize);
        this.freeBatches = new ArrayBlockingQueue<>(maxBatches);
        this.samplingComparator = samplingComparator;
        this.prevKey = samplingComparator != null ? layout.newKey() : null;
    }

    @Override
    public Void call() throws IOException {
        try {
            if (input.isEmpty()) {
                return null;
            }
            MergingBlockEntryReader<KEY, VALUE> mergingReader = new MergingBlockEntryReader<>(layout);
            input.forEach(mergingReader::addSource);
            //noinspection StatementWithEmptyBody
            while (mergeChunk(mergingReader)) {}
            return null;
        } finally {
            halted = true;
        }
    }

    /**
     * @return {@code true} if there may be more entries to merge, or {@code false} if the end of the stream has been
     * reached or merging was stopped.
     */
    private boolean mergeChunk(MergingBlockEntryReader<KEY, VALUE> mergingReader) throws IOException {
        for (int batch = 0; batch < BATCHES_PER_CHUNK; batch++) {
            Batch<KEY, VALUE> merged = acquireBatch();
            if (merged == null) {
                return false;
            }
            while (!merged.isFull()) {
                if (!alive() || !mergingReader.next()) {
                    if (!merged.isEmpty()) {
                        offer(merged);
                    }
                    return false;
                }
                merged.add(mergingReader.key(), mergingReader.value());
            }
            offer(merged);
        }
        return true;
    }

    /**
     * Called from another entry processor, either another merger like this one or a writer of the final data stream.
     * @return {@code true} if a new entry was selected (accessed via {@link #key()} and {@link #value()}, or {@code false}
     * if the end of the stream has been reached.
     */
    @Override
    public boolean next() throws IOException {
        do {
            if (currentOutput != null) {
                if (currentOutput.next()) {
                    return true;
                }
                if (!freeBatches.offer(currentOutput)) {
                    throw new IllegalStateException("More batches allocated than recycling slots.");
                }
            }
            currentOutput = nextOutputBatchOrNull();
        } while (currentOutput != null);
        return false;
    }

    @Override
    public KEY key() {
        return currentOutput.key();
    }

    @Override
    public VALUE value() {
        return currentOutput.value();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeAll(input);
    }

    private boolean alive() {
        return !halted && !cancellation.cancelled();
    }

    /**
     * @return an empty batch to fill, or {@code null} if this merger was halted or cancelled while waiting for one.
     */
    private Batch<KEY, VALUE> acquireBatch() {
        Batch<KEY, VALUE> batch = freeBatches.poll();
        if (batch == null && allocatedBatches < maxBatches) {
            allocatedBatches++;
            return new Batch<>(layout, batchSize);
        }
        try {
            while (batch == null && alive()) {
                batch = freeBatches.poll(10, MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            halted = true;
            return null;
        }
        if (batch != null) {
            batch.reset();
        }
        return batch;
    }

    private void offer(Batch<KEY, VALUE> batch) {
        if (samplingComparator != null) {
            includeInSample(batch);
        }

        try {
            while (alive() && !mergedOutput.offer(batch, 10, MILLISECONDS)) { // Then just stay here and try
                Thread.onSpinWait();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            halted = true;
        }
    }

    void halt() {
        halted = true;
    }

    private void includeInSample(Batch<KEY, VALUE> batch) {
        KEY prev = hasPrevKey ? prevKey : null;
        for (int i = 0; i < batch.size; i++) {
            KEY key = batch.keys[i];
            if (prev == null || samplingComparator.compare(key, prev) != 0) {
                uniqueValues++;
            }
            sampledValues++;
            prev = key;
        }
        if (batch.size > 0) {
            // The batch will be recycled so keep a copy of its last key to compare the next batch against
            layout.copyKey(batch.keys[batch.size - 1], prevKey);
            hasPrevKey = true;
        }
    }

    IndexSample buildIndexSample() {
        Preconditions.checkState(samplingComparator != null, "I haven't been sampling at all.");
        return new IndexSample(sampledValues, uniqueValues, sampledValues);
    }

    private Batch<KEY, VALUE> nextOutputBatchOrNull() {
        // Keep polling the output if:
        // - output isn't empty
        // - output is empty but this merger is still going
        while (alive() || !mergedOutput.isEmpty()) {
            try {
                Batch<KEY, VALUE> result = mergedOutput.poll(10, TimeUnit.MILLISECONDS);
                if (result != null) {
                    return result;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    /**
     * A fixed-capacity batch of entries which owns its key instances so that it can be filled, read and filled again
     * without allocating.
     */
    private static class Batch<KEY, VALUE> implements BlockEntryCursor<KEY, VALUE> {
        private final Layout<KEY, VALUE> layout;
        private final KEY[] keys;
        private final VALUE[] values;
        private int size;
        private int position = -1;

        @SuppressWarnings("unchecked")
        Batch(Layout<KEY, VALUE> layout, int capacity) {
            this.layout = layout;
            this.keys = (KEY[]) new Object[capacity];
            this.values = (VALUE[]) new Object[capacity];

            for (int i = 0; i < keys.length; i++) {
                keys[i] = layout.newKey();
            }
        }

        void add(KEY key, VALUE value) {
            layout.copyKey(key, keys[size]);
            values[size] = value;
            size++;
        }

        boolean isFull() {
            return size == keys.length;
        }

        boolean isEmpty() {
            return size == 0;
        }

        void reset() {
            size = 0;
            position = -1;
        }

        @Override
        public boolean next() {
            if (position + 1 < size) {
                position++;
                return true;
            }
            return false;
        }

        @Override
        public KEY key() {
            return keys[position];
        }

        @Override
        public VALUE value() {
            return values[position];
        }

        @Override
        public void close() { // no-op
        }
    }
}
