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

import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.kernel.impl.index.schema.BlockEntryStreamMerger.QUEUE_SIZE;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.kernel.api.index.IndexPopulator.PopulationWorkScheduler;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobHandles;

/**
 * Merges multiple (already individually sorted) parts of {@link BlockEntry block entries} into one stream.
 * A single merger thread is spawned which merges all parts with a tournament tree, see {@link MergingBlockEntryReader},
 * and hands over batches of merged entries to the consuming thread through {@link BlockEntryStreamMerger}.
 * Output is a {@link BlockEntryCursor} which is a stream which gets populated over time.
 *
 * One merger thread keeps up with the single-threaded consumer regardless of the number of parts since selecting
 * the next entry only costs {@code log2(parts)} comparisons, whereas splitting the merge over several threads
 * mostly adds queue hand-offs and allocation between them.
 */
class PartMerger<KEY, VALUE> implements AutoCloseable {
    static final int DEFAULT_BATCH_SIZE = 1_000;

    private final PopulationWorkScheduler populationWorkScheduler;
    private final List<BlockEntryCursor<KEY, VALUE>> parts;
    private final Layout<KEY, VALUE> layout;
    private final BlockStorage.Cancellation cancellation;
    private final int batchSize;
    private final Comparator<KEY> samplingComparator;
    private BlockEntryStreamMerger<KEY, VALUE> merger;
    private JobHandle<Void> mergeHandle;

    PartMerger(
            PopulationWorkScheduler populationWorkScheduler,
            List<BlockEntryCursor<KEY, VALUE>> parts,
            Layout<KEY, VALUE> layout,
            Comparator<KEY> samplingComparator,
            BlockStorage.Cancellation cancellation,
            int batchSize) {
        this.populationWorkScheduler = populationWorkScheduler;
        this.parts = parts;
        this.layout = layout;
        this.cancellation = cancellation;
        this.batchSize = batchSize;
        this.samplingComparator = samplingComparator;
    }

    BlockEntryStreamMerger<KEY, VALUE> startMerge() {
        merger = new BlockEntryStreamMerger<>(parts, layout, samplingComparator, cancellation, batchSize, QUEUE_SIZE);
        mergeHandle = populationWorkScheduler.schedule(
                indexName -> "Part merger while writing scan update for " + indexName, merger);
        return merger;
    }

    @Override
    public void close() throws IOException {
        if (merger == null) {
            closeAll(parts);
            return;
        }
        merger.halt();
        try {
            JobHandles.getAllResults(List.of(mergeHandle), IOException.class, IOException::new);
        } finally {
            closeAll(merger, () -> closeAll(parts));
        }
    }
}
