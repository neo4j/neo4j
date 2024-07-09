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
package org.neo4j.internal.batchimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.SimpleStageControl;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.Race;

class RecordProcessorStepTest {
    @Test
    void shouldMergeResultsWhenCompleted() throws Exception {
        // given
        int numThreads = 4;
        Configuration config = Configuration.DEFAULT;
        MutableLong result = new MutableLong();
        AtomicInteger doneCalls = new AtomicInteger();
        AtomicInteger closeCalls = new AtomicInteger();
        try (RecordProcessorStep<NodeRecord> step = new RecordProcessorStep<>(
                new SimpleStageControl(),
                "test",
                config,
                () -> new TestProcessor(result, doneCalls, closeCalls),
                true,
                numThreads,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                any -> StoreCursors.NULL)) {
            // when
            step.start(0);
            int batchesPerThread = 100;
            AtomicLong nextId = new AtomicLong();
            Race race = new Race();
            race.addContestants(
                    numThreads,
                    () -> {
                        long startId = nextId.getAndAdd(config.batchSize());
                        NodeRecord[] batch = new NodeRecord[config.batchSize()];
                        for (int r = 0; r < batch.length; r++) {
                            batch[r] = new NodeRecord(startId + r);
                            batch[r].setInUse(true);
                        }
                        step.process(batch, null, CursorContext.NULL_CONTEXT);
                    },
                    batchesPerThread);
            race.goUnchecked();

            // then
            step.done();
            assertThat(result.longValue()).isEqualTo(numThreads * config.batchSize() * batchesPerThread);
        }

        // then also
        assertThat(doneCalls.longValue()).isEqualTo(1);
        assertThat(closeCalls.longValue()).isEqualTo(numThreads);
    }

    private static class TestProcessor implements RecordProcessor<NodeRecord> {
        private final MutableLong result;
        private final AtomicInteger doneCalls;
        private final AtomicInteger closeCalls;
        private int counter;
        private boolean done;
        private boolean closed;

        TestProcessor(MutableLong result, AtomicInteger doneCalls, AtomicInteger closeCalls) {
            this.result = result;
            this.doneCalls = doneCalls;
            this.closeCalls = closeCalls;
        }

        @Override
        public boolean process(NodeRecord item, StoreCursors storeCursors) {
            counter++;
            return false;
        }

        @Override
        public void mergeResultsFrom(RecordProcessor<NodeRecord> other) {
            assertThat(done).isFalse();
            counter += ((TestProcessor) other).counter;
        }

        @Override
        public void done() {
            // Only one of the instances will get this call
            assertThat(closed).isFalse();
            done = true;
            assertThat(doneCalls.getAndIncrement()).isEqualTo(0);
            result.setValue(counter);
        }

        @Override
        public void close() {
            assertThat(closed).isFalse();
            closed = true;
            closeCalls.incrementAndGet();
        }
    }
}
