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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.index.schema.BlockEntryMergerTestUtils.assertMergedPartStream;
import static org.neo4j.kernel.impl.index.schema.BlockEntryMergerTestUtils.buildParts;
import static org.neo4j.kernel.impl.index.schema.BlockStorage.Cancellation.NOT_CANCELLABLE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RawBytes;
import org.neo4j.index.internal.gbptree.SimpleByteArrayLayout;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith({RandomExtension.class, OtherThreadExtension.class})
class BlockEntryStreamMergerTest {
    private static final int QUEUE_SIZE = 5;
    private static final int BATCH_SIZE = 10;

    @Inject
    private RandomSupport random;

    @Inject
    private OtherThread t2;

    private final Layout<RawBytes, RawBytes> layout = new SimpleByteArrayLayout();
    private final List<BlockEntry<RawBytes, RawBytes>> allData = new ArrayList<>();

    @Test
    void shouldMergePartsIntoOneWithoutSampling() throws Exception {
        // given
        List<BlockEntryCursor<RawBytes, RawBytes>> parts = buildParts(random, layout, allData);

        // when
        try (BlockEntryStreamMerger<RawBytes, RawBytes> merger =
                new BlockEntryStreamMerger<>(parts, layout, null, NOT_CANCELLABLE, BATCH_SIZE, QUEUE_SIZE)) {
            t2.execute(merger);

            // then
            assertMergedPartStream(allData, merger);
        }
    }

    @Test
    void shouldMergePartsIntoOneWithSampling() throws Exception {
        // given
        List<BlockEntryCursor<RawBytes, RawBytes>> parts = buildParts(random, layout, allData);

        // when
        try (BlockEntryStreamMerger<RawBytes, RawBytes> merger =
                new BlockEntryStreamMerger<>(parts, layout, layout, NOT_CANCELLABLE, BATCH_SIZE, QUEUE_SIZE)) {
            Future<Void> t2Future = t2.execute(merger);

            // then
            assertMergedPartStream(allData, merger);
            t2Future.get();
            IndexSample sample = merger.buildIndexSample();
            assertThat(sample.sampleSize()).isEqualTo(allData.size());
            assertThat(sample.indexSize()).isEqualTo(allData.size());
            assertThat(sample.uniqueValues()).isEqualTo(countUniqueKeys(allData));
        }
    }

    @Test
    void shouldStopMergingWhenHalted() throws Exception {
        // given
        List<BlockEntryCursor<RawBytes, RawBytes>> parts =
                buildParts(random, layout, allData, 4, rng -> QUEUE_SIZE * BATCH_SIZE);

        // when
        try (BlockEntryStreamMerger<RawBytes, RawBytes> merger =
                new BlockEntryStreamMerger<>(parts, layout, null, NOT_CANCELLABLE, BATCH_SIZE, QUEUE_SIZE)) {
            // start the merge and wait for it to fill up the queue to the brim before halting it
            Future<Void> invocation = t2.execute(merger);
            t2.get().waitUntilWaiting(wait -> wait.isAt(BlockEntryStreamMerger.class, "call"));
            merger.halt();
            invocation.get();

            // then we know how many items we should have gotten, and no more after that
            assertThat(countEntries(merger)).isEqualTo(QUEUE_SIZE * BATCH_SIZE);
        }
    }

    @Test
    void shouldStopMergingWhenCancelled() throws Exception {
        // given
        List<BlockEntryCursor<RawBytes, RawBytes>> parts =
                buildParts(random, layout, allData, 4, rng -> QUEUE_SIZE * BATCH_SIZE);

        // when
        AtomicBoolean cancelled = new AtomicBoolean();
        try (BlockEntryStreamMerger<RawBytes, RawBytes> merger =
                new BlockEntryStreamMerger<>(parts, layout, null, cancelled::get, BATCH_SIZE, QUEUE_SIZE)) {
            // start the merge and wait for it to fill up the queue to the brim before halting it
            Future<Void> invocation = t2.execute(merger);
            t2.get().waitUntilWaiting(wait -> wait.isAt(BlockEntryStreamMerger.class, "call"));
            cancelled.set(true);
            invocation.get();

            // then we know how many items we should have gotten, and no more after that
            assertThat(countEntries(merger)).isEqualTo(QUEUE_SIZE * BATCH_SIZE);
        }
    }

    @Test
    void shouldStopReaderFromAwaitingMoreBatchesWhenHalted() throws Exception {
        // given
        List<BlockEntryCursor<RawBytes, RawBytes>> parts =
                buildParts(random, layout, allData, 4, rng -> QUEUE_SIZE * BATCH_SIZE);

        // when
        try (BlockEntryStreamMerger<RawBytes, RawBytes> merger =
                new BlockEntryStreamMerger<>(parts, layout, null, NOT_CANCELLABLE, BATCH_SIZE, QUEUE_SIZE)) {
            Future<Boolean> firstRead = t2.execute(merger::next);
            t2.get().waitUntilWaiting(wait -> wait.isAt(BlockEntryStreamMerger.class, "next"));
            merger.halt();

            // then
            assertThat(firstRead.get()).isFalse();
        }
    }

    @Test
    void shouldStopReaderFromAwaitingMoreBatchesWhenCancelled() throws Exception {
        // given
        List<BlockEntryCursor<RawBytes, RawBytes>> parts =
                buildParts(random, layout, allData, 4, rng -> QUEUE_SIZE * BATCH_SIZE);

        // when
        try (BlockEntryStreamMerger<RawBytes, RawBytes> merger =
                new BlockEntryStreamMerger<>(parts, layout, null, NOT_CANCELLABLE, BATCH_SIZE, QUEUE_SIZE)) {
            Future<Boolean> firstRead = t2.execute(merger::next);
            t2.get().waitUntilWaiting(wait -> wait.isAt(BlockEntryStreamMerger.class, "next"));
            merger.halt();

            // then
            assertThat(firstRead.get()).isFalse();
        }
    }

    private static int countEntries(BlockEntryStreamMerger<RawBytes, RawBytes> merger) throws IOException {
        int numMergedEntries = 0;
        while (merger.next()) {
            numMergedEntries++;
        }
        return numMergedEntries;
    }

    private long countUniqueKeys(List<BlockEntry<RawBytes, RawBytes>> entries) {
        TreeSet<RawBytes> set = new TreeSet<>(layout);
        entries.forEach(e -> set.add(e.key()));
        return set.size();
    }
}
