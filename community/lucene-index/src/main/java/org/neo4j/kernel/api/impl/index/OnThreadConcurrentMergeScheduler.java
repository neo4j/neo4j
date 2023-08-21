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
package org.neo4j.kernel.api.impl.index;

import java.io.IOException;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;

/**
 * This is a {@link MergeScheduler} which is a version of {@link org.apache.lucene.index.SerialMergeScheduler},
 * but with the important difference that multiple threads can run merge of difference sources in parallel.
 * I.e. in the scenario of index population where the population threads that adds documents go and do merge
 * on their individual threads, in parallel with the other population threads. This effectively comes close
 * to the {@link org.apache.lucene.index.ConcurrentMergeScheduler} parallel-wise w/o spawning additional
 * background threads.
 */
public class OnThreadConcurrentMergeScheduler extends MergeScheduler {
    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
        while (true) {
            MergePolicy.OneMerge merge = nextMergeSynchronized(mergeSource);
            if (merge == null) {
                break;
            }
            mergeSource.merge(merge);
        }
    }

    private synchronized MergePolicy.OneMerge nextMergeSynchronized(MergeSource mergeSource) {
        return mergeSource.getNextMerge();
    }

    @Override
    public void close() {}
}
