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
package org.neo4j.kernel.api.impl.index.lucene.v10;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.SequencedCollection;
import java.util.concurrent.Future;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeScheduler.MergeSource;
import org.apache.lucene.index.MergeTrigger;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10Directory.OnThreadConcurrentMergeScheduler;
import org.neo4j.test.Barrier;
import org.neo4j.test.Barrier.Control;
import org.neo4j.test.OtherThreadExecutor;

class Lucene10OnThreadConcurrentMergeSchedulerTest {
    @Test
    void shouldMergeSourcesConcurrently() throws Exception {
        // given
        try (OnThreadConcurrentMergeScheduler scheduler = new OnThreadConcurrentMergeScheduler();
                OtherThreadExecutor t2 = new OtherThreadExecutor("T2")) {
            Control barrier = new Barrier.Control();
            MergeSource source = new ControlledMergeSource(barrier, 2, MergeBarrierPoint.MERGE);
            Future<Object> t2MergeFuture = t2.executeDontWait(() -> {
                scheduler.merge(source, MergeTrigger.EXPLICIT);
                return null;
            });
            barrier.await();
            // when first merge now waiting in the merge of the segment, then another thread should be able to
            // run an additional merge on that scheduler and allowed to run its merge concurrently
            scheduler.merge(source, MergeTrigger.EXPLICIT);
            barrier.release();
            t2MergeFuture.get();
            assertThat(source.getNextMerge()).isNull();
        }
    }

    @Test
    void shouldGetNextMergeSynchronized() throws Exception {
        // given
        try (OnThreadConcurrentMergeScheduler scheduler = new OnThreadConcurrentMergeScheduler();
                OtherThreadExecutor t2 = new OtherThreadExecutor("T2");
                OtherThreadExecutor t3 = new OtherThreadExecutor("T3")) {
            Control barrier = new Barrier.Control();
            MergeSource source = new ControlledMergeSource(barrier, 2, MergeBarrierPoint.NEXT_MERGE);
            Future<Object> t2MergeFuture = t2.executeDontWait(() -> {
                scheduler.merge(source, MergeTrigger.EXPLICIT);
                return null;
            });
            barrier.await();

            // when first merge now waiting in getting next merge, then other threads needs to wait
            // for that to complete before getting their next merge
            Future<Object> t3MergeFuture = t3.executeDontWait(() -> {
                scheduler.merge(source, MergeTrigger.EXPLICIT);
                return null;
            });
            t3.waitUntilBlocked(waitDetails -> waitDetails.isAt(OnThreadConcurrentMergeScheduler.class, "merge"));
            barrier.release();

            // then
            t2MergeFuture.get();
            t3MergeFuture.get();
            assertThat(source.getNextMerge()).isNull();
        }
    }

    private static class ControlledMergeSource implements MergeScheduler.MergeSource {
        private final int numMerges;
        private final MergeBarrierPoint barrierPoint;
        private final Barrier.Control barrier;
        private final SequencedCollection<OneMerge> merges = new ArrayList<>();

        public ControlledMergeSource(Barrier.Control barrier, int numMerges, MergeBarrierPoint barrierPoint) {
            this.barrier = barrier;
            this.numMerges = numMerges;
            this.barrierPoint = barrierPoint;
        }

        @Override
        public MergePolicy.OneMerge getNextMerge() {
            int currentlyHandedOutMerges = merges.size();
            if (barrierPoint == MergeBarrierPoint.NEXT_MERGE && currentlyHandedOutMerges == 0) {
                barrier.reached();
            }

            if (currentlyHandedOutMerges < numMerges) {
                OneMerge merge = mock(MergePolicy.OneMerge.class);
                merges.add(merge);
                return merge;
            }
            return null;
        }

        @Override
        public void onMergeFinished(MergePolicy.OneMerge oneMerge) {}

        @Override
        public boolean hasPendingMerges() {
            return false;
        }

        @Override
        public void merge(MergePolicy.OneMerge oneMerge) {
            if (barrierPoint == MergeBarrierPoint.MERGE && oneMerge == merges.getFirst()) {
                barrier.reached();
            }
        }
    }

    private enum MergeBarrierPoint {
        MERGE,
        NEXT_MERGE
    }
}
