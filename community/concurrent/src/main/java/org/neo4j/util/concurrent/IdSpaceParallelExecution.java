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
package org.neo4j.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.internal.helpers.NamedThreadFactory;

public class IdSpaceParallelExecution {
    private IdSpaceParallelExecution() {}

    /**
     * Simple utility for splitting up an ID space into {@code threads} partitions and run each partition in parallel.
     * @param threadNamePrefix prefix that spawned threads will have.
     * @param threads number of threads running this, i.e. number of partitions to split up the ID space into.
     * @param highIndex the high end of the ID space (low is 0).
     * @param taskFactory for constructing {@link Callable} instances each thread will be running for its partition.
     * @throws ExecutionException on error in any of the partition tasks.
     */
    public static void runInParallel(
            String threadNamePrefix, int threads, long highIndex, Function<Partition, Callable<Void>> taskFactory)
            throws ExecutionException {
        long indexesPerThread = MathUtil.ceil(highIndex, threads);
        long index = 0;
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < threads && index < highIndex; i++) {
            long startIndex = index;
            long endIndex = Math.min(index + indexesPerThread, highIndex);
            tasks.add(taskFactory.apply(new Partition(startIndex, endIndex, i)));
            index = endIndex;
        }
        if (!tasks.isEmpty()) {
            var executor = Executors.newFixedThreadPool(tasks.size(), NamedThreadFactory.named(threadNamePrefix));
            try {
                for (var future : executor.invokeAll(tasks)) {
                    future.get();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                executor.shutdown();
            }
        }
    }

    public record Partition(long startInclusive, long endExclusive, int partitionId) {}
}
