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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.index.internal.gbptree.DataTree.W_ESCALATING_COORDINATION;
import static org.neo4j.index.internal.gbptree.DataTree.W_SPLIT_KEEP_ALL_LEFT;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.consistencyCheckStrict;
import static org.neo4j.io.async.AsyncBlockAccessor.EMPTY_ASYNC_BLOCK_ACCESSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.test.utils.PageCacheConfig.config;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
abstract class GBPTreeEscalationCoordinationIT<KEY, VALUE> {
    private static final int PAGE_SIZE = 256;

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fileSystem;

    private PageCache pageCache;
    private TestLayout<KEY, VALUE> layout;

    abstract TestLayout<KEY, VALUE> getLayout();

    int loadedEvens() {
        return 250;
    }

    @BeforeEach
    void start() {
        pageCache = PageCacheSupportExtension.getPageCache(
                fileSystem, config().withPageSize(PAGE_SIZE).withAccessChecks(true));
        layout = getLayout();
    }

    @AfterEach
    void stop() {
        pageCache.close();
    }

    @Test
    void shouldDeepEscalateAndStayConsistentAcrossForcedResets() throws IOException {
        var monitor = new CountingMonitor(null);
        try (var tree = openTree(directory.file("index"), monitor)) {
            bulkLoadPackedEvens(tree, loadedEvens());
            assertThat(inspect(tree).lastLevel())
                    .as("tree must be deep enough (>=4 levels) to allow deep escalation")
                    .isGreaterThanOrEqualTo(3);

            try (var writer = tree.writer(W_ESCALATING_COORDINATION, NULL_CONTEXT)) {
                for (int i = 0; i < loadedEvens(); i++) {
                    long seed = 2L * i + 1;
                    writer.put(layout.key(seed), layout.value(seed));
                }
            }

            assertThat(monitor.deepEscalations.get())
                    .as("at least one insert must have deep-escalated (absorbed above the immediate parent)")
                    .isGreaterThanOrEqualTo(1);

            checkpoint(tree);
            assertThat(consistencyCheckStrict(tree)).isTrue();
            assertContainsExactlyRange(tree, 2L * loadedEvens());
        }
    }

    @Test
    void shouldBailToPessimisticWhenAbsorbingAncestorIsContended() throws Exception {
        long seed = executeControlInsertion();

        var pessimisticSignal = new CountDownLatch(1);
        var contendedMonitor = new CountingMonitor(pessimisticSignal);
        try (var executor = Executors.newSingleThreadExecutor();
                var contended = openTree(directory.file("contended"), contendedMonitor)) {
            bulkLoadPackedEvens(contended, loadedEvens());
            var inspection = inspect(contended);
            assertThat(inspection.lastLevel()).isEqualTo(3);

            var latchService = contended.rootLayerSupport.latchService();
            var held = new ArrayList<LongSpinLatch>();
            for (long rootChild : inspection.nodesPerLevel().get(1).toArray()) {
                var latch = latchService.latch(rootChild);
                latch.acquireRead();
                held.add(latch);
            }

            long contendedSeed = seed;
            Future<?> insert;
            try {
                insert = executor.submit(() -> put(contended, contendedSeed));
                assertThat(pessimisticSignal.await(60, TimeUnit.SECONDS))
                        .as("escalating writer should flip to pessimistic because the ancestor is contended")
                        .isTrue();
            } finally {
                for (var latch : held) {
                    latch.releaseRead();
                    latch.deref();
                }
            }
            insert.get(60, TimeUnit.SECONDS);

            assertThat(contendedMonitor.pessimisticFallbacks.get()).isGreaterThanOrEqualTo(1);
            assertThat(contendedMonitor.deepEscalations.get())
                    .as("contention must have prevented escalation")
                    .isZero();

            checkpoint(contended);
            assertThat(consistencyCheckStrict(contended)).isTrue();
            assertThat(contains(contended, seed))
                    .as("seed %d must be present after the pessimistic fallback completed", seed)
                    .isTrue();
        }
    }

    private long executeControlInsertion() throws IOException {
        long seed;
        var controlMonitor = new CountingMonitor(null);
        try (var control = openTree(directory.file("control"), controlMonitor)) {
            bulkLoadPackedEvens(control, loadedEvens());
            var inspection = inspect(control);
            assertThat(inspection.lastLevel())
                    .as("scenario requires exactly 4 levels")
                    .isEqualTo(3);
            seed = deepEscalationSeed(inspection);
            put(control, seed);
            assertThat(controlMonitor.deepEscalations.get())
                    .as("control insert of seed %d should deep-escalate", seed)
                    .isEqualTo(1);
            assertThat(controlMonitor.pessimisticFallbacks.get()).isZero();
        }
        return seed;
    }

    private GBPTree<KEY, VALUE> openTree(Path file, MultiRootGBPTree.Monitor monitor) {
        return new GBPTreeBuilder<>(pageCache, fileSystem, file, layout)
                .with(monitor)
                .build();
    }

    private void bulkLoadPackedEvens(GBPTree<KEY, VALUE> tree, int count) throws IOException {
        try (var writer = tree.writer(W_BATCHED_SINGLE_THREADED | W_SPLIT_KEEP_ALL_LEFT, NULL_CONTEXT)) {
            for (int i = 0; i < count; i++) {
                long seed = 2L * i;
                writer.put(layout.key(seed), layout.value(seed));
            }
        }
    }

    /**
     * A seed that lands in a full leaf resulting in split that get absorbed by grandparent
     */
    private long deepEscalationSeed(GBPTreeInspection.Tree inspection) {
        int leafCapacity = maxKeyCount(inspection, inspection.leafNodes().toArray());
        int internalFanout = maxKeyCount(inspection, inspection.internalNodes().toArray()) + 1;
        int rootChildren = inspection.nodesPerLevel().get(1).size();
        long firstLeafUnderRightmostRootChild = (long) (rootChildren - 1) * internalFanout * internalFanout;
        long firstEvenSeedInThatLeaf = 2L * firstLeafUnderRightmostRootChild * leafCapacity;
        return firstEvenSeedInThatLeaf + 1;
    }

    private static int maxKeyCount(GBPTreeInspection.Tree inspection, long[] nodes) {
        int max = 0;
        for (long node : nodes) {
            max = Math.max(max, inspection.keyCounts().get(node));
        }
        return max;
    }

    private Void put(GBPTree<KEY, VALUE> tree, long seed) throws IOException {
        try (var writer = tree.writer(W_ESCALATING_COORDINATION, NULL_CONTEXT)) {
            writer.put(layout.key(seed), layout.value(seed));
        }
        return null;
    }

    private GBPTreeInspection.Tree inspect(GBPTree<KEY, VALUE> tree) throws IOException {
        return tree.visit(new InspectingVisitor<>(), NULL_CONTEXT).get().single();
    }

    private void checkpoint(GBPTree<KEY, VALUE> tree) throws IOException {
        tree.checkpoint(
                Header.CARRY_OVER_PREVIOUS_HEADER, FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT, true);
    }

    private boolean contains(GBPTree<KEY, VALUE> tree, long seed) throws IOException {
        try (var seek = tree.seek(layout.key(seed), layout.key(seed), NULL_CONTEXT)) {
            return seek.next();
        }
    }

    private void assertContainsExactlyRange(GBPTree<KEY, VALUE> tree, long size) throws IOException {
        KEY low = layout.newKey();
        KEY high = layout.newKey();
        layout.initializeAsLowest(low);
        layout.initializeAsHighest(high);
        long expected = 0;
        try (var seek = tree.seek(low, high, NULL_CONTEXT)) {
            while (seek.next()) {
                assertThat(layout.keySeed(seek.key())).isEqualTo(expected);
                assertThat(layout.valueSeed(seek.value())).isEqualTo(expected);
                expected++;
            }
        }
        assertThat(expected)
                .as("all seeds in [0, %d) must be present exactly once", size)
                .isEqualTo(size);
    }

    private static class CountingMonitor extends MultiRootGBPTree.Monitor.Adaptor {
        final AtomicInteger deepEscalations = new AtomicInteger();
        final AtomicInteger shallowEscalations = new AtomicInteger();
        final AtomicInteger pessimisticFallbacks = new AtomicInteger();
        private final CountDownLatch pessimisticSignal;

        CountingMonitor(CountDownLatch pessimisticSignal) {
            this.pessimisticSignal = pessimisticSignal;
        }

        @Override
        public void treeWriterEscalated(boolean deep) {
            (deep ? deepEscalations : shallowEscalations).incrementAndGet();
        }

        @Override
        public void treeWriterFlippedToPessimistic() {
            pessimisticFallbacks.incrementAndGet();
            if (pessimisticSignal != null) {
                pessimisticSignal.countDown();
            }
        }
    }
}
