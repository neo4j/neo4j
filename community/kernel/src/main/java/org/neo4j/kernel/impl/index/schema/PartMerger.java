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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.kernel.api.index.IndexPopulator.PopulationWorkScheduler;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobHandles;

/**
 * The idea is to merge multiple (already individually sorted) parts of {@link BlockEntry block entries} into one stream.
 * Input is the parts. One or more threads will be spawned to merge these parts with the given merge factor, making the number
 * of threads by roughly log4(numberOfParts). Output is a {@link BlockEntryCursor} which is a stream which gets populated over time.
 *
 * The part merging will look something like this:
 * <pre>
 *   (1) (2) (3) (4) (5) (6) (7) (8) (9) (10)
 *    \  |   |  /     \  |   |  /   /   /
 *    (---A---)       (---B---)   /   /
 *          \           |       /   /
 *         (----------C-------------)
 * </pre>
 */
class PartMerger<KEY, VALUE> implements AutoCloseable {
    static final int DEFAULT_BATCH_SIZE = 100;
    private static final int MERGE_FACTOR = 4;

    private final PopulationWorkScheduler populationWorkScheduler;
    private final List<BlockEntryCursor<KEY, VALUE>> parts;
    private final Layout<KEY, VALUE> layout;
    private final BlockStorage.Cancellation cancellation;
    private final int batchSize;
    private final Comparator<KEY> samplingComparator;
    private final List<BlockEntryStreamMerger<KEY, VALUE>> allMergers = new ArrayList<>();
    private final List<JobHandle<Void>> mergeHandles = new ArrayList<>();

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
        List<BlockEntryCursor<KEY, VALUE>> remainingParts = new ArrayList<>(parts);
        while (remainingParts.size() > MERGE_FACTOR) {
            // Build one "level" of mergers, each merger in this level merging "merge factor" number of streams
            List<BlockEntryCursor<KEY, VALUE>> current = new ArrayList<>();
            List<BlockEntryCursor<KEY, VALUE>> levelParts = new ArrayList<>();
            for (BlockEntryCursor<KEY, VALUE> remainingPart : remainingParts) {
                current.add(remainingPart);
                if (current.size() == MERGE_FACTOR) {
                    BlockEntryStreamMerger<KEY, VALUE> merger =
                            new BlockEntryStreamMerger<>(current, layout, null, cancellation, batchSize, QUEUE_SIZE);
                    allMergers.add(merger);
                    levelParts.add(merger);
                    current = new ArrayList<>();
                }
            }
            levelParts.addAll(current);
            remainingParts = levelParts;
        }

        BlockEntryStreamMerger<KEY, VALUE> merger = new BlockEntryStreamMerger<>(
                remainingParts, layout, samplingComparator, cancellation, batchSize, QUEUE_SIZE);
        allMergers.add(merger);
        allMergers.forEach(merge -> mergeHandles.add(populationWorkScheduler.schedule(
                indexName -> "Part merger while writing scan update for " + indexName, merge)));
        return merger;
    }

    @Override
    public void close() throws IOException {
        allMergers.forEach(BlockEntryStreamMerger::halt);
        try {
            JobHandles.getAllResults(mergeHandles);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException(e.getCause());
        } finally {
            closeAll(() -> closeAll(allMergers), () -> closeAll(parts));
        }
    }
}
