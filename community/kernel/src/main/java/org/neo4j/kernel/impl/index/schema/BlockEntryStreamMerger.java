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
import java.util.ArrayList;
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
 * For convenience instances are themselves valid as input into another {@link BlockEntryStreamMerger}.
 */
class BlockEntryStreamMerger<KEY, VALUE> implements BlockEntryCursor<KEY, VALUE>, Callable<Void> {
    static final int QUEUE_SIZE = 10;

    private final List<BlockEntryCursor<KEY, VALUE>> input;
    private final Layout<KEY, VALUE> layout;
    private final BlockStorage.Cancellation cancellation;
    private final ArrayBlockingQueue<BlockEntryCursor<KEY, VALUE>> mergedOutput;
    private final int batchSize;
    private final Comparator<KEY> samplingComparator;
    private KEY prevKey;
    private long sampledValues;
    private long uniqueValues;
    private volatile boolean halted;
    // This cursor will be used by the single thread reading from this merged stream
    private BlockEntryCursor<KEY, VALUE> currentOutput;

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
        this.mergedOutput = new ArrayBlockingQueue<>(queueSize);
        this.samplingComparator = samplingComparator;
    }

    @Override
    public Void call() throws IOException {
        try {
            MergingBlockEntryReader<KEY, VALUE> mergingReader = new MergingBlockEntryReader<>(layout);
            input.forEach(mergingReader::addSource);
            List<BlockEntry<KEY, VALUE>> merged = new ArrayList<>(batchSize);
            while (alive() && mergingReader.next()) {
                merged.add(new BlockEntry<>(mergingReader.key(), mergingReader.value()));
                if (merged.size() == batchSize) {
                    offer(merged);
                    merged = new ArrayList<>(batchSize);
                }
            }
            if (!merged.isEmpty()) {
                offer(merged);
            }
            return null;
        } finally {
            halted = true;
        }
    }

    /**
     * Called from another entry processor, either another merger like this one or a writer of the final data stream.
     * @return {@code true} if a new entry was selected (accessed via {@link #key()} and {@link #value()}, or {@code false}
     * if the end of the stream has been reached.
     */
    @Override
    public boolean next() throws IOException {
        do {
            if (currentOutput != null && currentOutput.next()) {
                return true;
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

    private void offer(List<BlockEntry<KEY, VALUE>> entries) {
        if (samplingComparator != null) {
            includeInSample(entries);
        }

        ListBasedBlockEntryCursor<KEY, VALUE> batch = new ListBasedBlockEntryCursor<>(entries);
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

    private void includeInSample(List<BlockEntry<KEY, VALUE>> entries) {
        for (BlockEntry<KEY, VALUE> entry : entries) {
            KEY key = entry.key();
            if (prevKey == null || samplingComparator.compare(key, prevKey) != 0) {
                prevKey = key;
                uniqueValues++;
            }
            sampledValues++;
        }
    }

    IndexSample buildIndexSample() {
        Preconditions.checkState(samplingComparator != null, "I haven't been sampling at all");
        return new IndexSample(sampledValues, uniqueValues, sampledValues);
    }

    private BlockEntryCursor<KEY, VALUE> nextOutputBatchOrNull() {
        // Keep polling the output if:
        // - output isn't empty
        // - output is empty but this merger is still going
        while (alive() || !mergedOutput.isEmpty()) {
            try {
                BlockEntryCursor<KEY, VALUE> result = mergedOutput.poll(10, TimeUnit.MILLISECONDS);
                if (result != null) {
                    return result;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }
}
