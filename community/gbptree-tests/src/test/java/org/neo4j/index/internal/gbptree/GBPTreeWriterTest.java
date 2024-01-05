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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.index.internal.gbptree.DataTree.W_SPLIT_KEEP_ALL_LEFT;
import static org.neo4j.index.internal.gbptree.DataTree.W_SPLIT_KEEP_ALL_RIGHT;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.PageCacheConfig;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class GBPTreeWriterTest {
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension =
            new PageCacheSupportExtension(PageCacheConfig.config().withPageSize(512));

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    private final SimpleLongLayout layout =
            SimpleLongLayout.longLayout().withFixedSize(true).build();

    @Test
    void shouldReInitializeTreeLogicWithSameSplitRatioAsInitiallySet0() throws IOException {
        TreeHeightTracker treeHeightTracker = new TreeHeightTracker();
        try (GBPTree<MutableLong, MutableLong> gbpTree = new GBPTreeBuilder<>(
                        pageCache, fileSystem, directory.file("index"), layout)
                .with(treeHeightTracker)
                .build()) {
            try (var writer = gbpTree.writer(W_SPLIT_KEEP_ALL_RIGHT | W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                MutableLong dontCare = layout.value(0);

                long keySeed = 10_000;
                while (treeHeightTracker.treeHeight < 5) {
                    MutableLong key = layout.key(keySeed--);
                    writer.put(key, dontCare);
                }
            }
            // We now have a tree with height 6.
            // The leftmost node on all levels should have only a single key.
            KeyCountingVisitor keyCountingVisitor = new KeyCountingVisitor();
            gbpTree.visit(keyCountingVisitor, NULL_CONTEXT);
            for (Integer leftmostKeyCount : keyCountingVisitor.keyCountOnLeftmostPerLevel) {
                assertEquals(1, leftmostKeyCount.intValue());
            }
        }
    }

    @Test
    void shouldReInitializeTreeLogicWithSameSplitRatioAsInitiallySet1() throws IOException {
        TreeHeightTracker treeHeightTracker = new TreeHeightTracker();
        try (GBPTree<MutableLong, MutableLong> gbpTree = new GBPTreeBuilder<>(
                        pageCache, fileSystem, directory.file("index"), layout)
                .with(treeHeightTracker)
                .build()) {
            try (var writer = gbpTree.writer(W_SPLIT_KEEP_ALL_LEFT | W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                MutableLong dontCare = layout.value(0);

                long keySeed = 0;
                while (treeHeightTracker.treeHeight < 5) {
                    MutableLong key = layout.key(keySeed++);
                    writer.put(key, dontCare);
                }
            }
            // We now have a tree with height 6.
            // The rightmost node on all levels should have either one or zero key (zero for internal nodes).
            KeyCountingVisitor keyCountingVisitor = new KeyCountingVisitor();
            gbpTree.visit(keyCountingVisitor, NULL_CONTEXT);
            for (Integer rightmostKeyCount : keyCountingVisitor.keyCountOnRightmostPerLevel) {
                assertTrue(rightmostKeyCount == 0 || rightmostKeyCount == 1);
            }
        }
    }

    @Test
    void trackPageCacheAccessOnMerge() throws IOException {
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        var cursorContext = contextFactory.create("trackPageCacheAccessOnMerge");

        assertZeroCursor(cursorContext);

        try (var gbpTree = new GBPTreeBuilder<>(pageCache, fileSystem, directory.file("index"), layout).build();
                var treeWriter = gbpTree.writer(W_SPLIT_KEEP_ALL_RIGHT, cursorContext)) {
            treeWriter.merge(new MutableLong(0), new MutableLong(1), ValueMergers.overwrite());
        }

        var cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isEqualTo(1);
        assertThat(cursorTracer.unpins()).isEqualTo(1);
        assertThat(cursorTracer.hits()).isEqualTo(1);
        assertThat(cursorTracer.faults()).isEqualTo(0);
    }

    @Test
    void trackPageCacheAccessOnPut() throws IOException {
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        var cursorContext = contextFactory.create("trackPageCacheAccessOnPut");

        assertZeroCursor(cursorContext);

        try (var gbpTree = new GBPTreeBuilder<>(pageCache, fileSystem, directory.file("index"), layout).build();
                var treeWriter = gbpTree.writer(W_SPLIT_KEEP_ALL_RIGHT, cursorContext)) {
            treeWriter.put(new MutableLong(0), new MutableLong(1));
        }

        var cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isEqualTo(1);
        assertThat(cursorTracer.unpins()).isEqualTo(1);
        assertThat(cursorTracer.hits()).isEqualTo(1);
        assertThat(cursorTracer.faults()).isEqualTo(0);
    }

    @Test
    void trackPageCacheAccessOnRemove() throws IOException {
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        var cursorContext = contextFactory.create("trackPageCacheAccessOnRemove");

        try (var gbpTree = new GBPTreeBuilder<>(pageCache, fileSystem, directory.file("index"), layout).build();
                var treeWriter = gbpTree.writer(W_SPLIT_KEEP_ALL_RIGHT, cursorContext)) {
            treeWriter.put(new MutableLong(0), new MutableLong(0));
            treeWriter.remove(new MutableLong(0));
        }

        var cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isEqualTo(2);
        assertThat(cursorTracer.unpins()).isEqualTo(2);
        assertThat(cursorTracer.hits()).isEqualTo(2);
        assertThat(cursorTracer.faults()).isZero();
    }

    @Test
    void trackPageCacheAccessOnRemoveWhenNothingToRemove() throws IOException {
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        var cursorContext = contextFactory.create("trackPageCacheAccessOnRemoveWhenNothingToRemove");

        assertZeroCursor(cursorContext);

        try (var gbpTree = new GBPTreeBuilder<>(pageCache, fileSystem, directory.file("index"), layout).build();
                var treeWriter = gbpTree.writer(W_SPLIT_KEEP_ALL_RIGHT, cursorContext)) {
            treeWriter.remove(new MutableLong(0));
        }

        var cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isEqualTo(1);
        assertThat(cursorTracer.hits()).isEqualTo(1);
        assertThat(cursorTracer.unpins()).isEqualTo(1);
        assertThat(cursorTracer.faults()).isEqualTo(0);
    }

    @Test
    void shouldYieldParallelWriterSoThatOthersCanProgressUnhindered() throws Exception {
        // given
        try (var tree = new GBPTreeBuilder<>(pageCache, fileSystem, directory.file("index"), layout).build();
                var t2 = new OtherThreadExecutor("T2")) {
            int numInitialWrites = 10;
            int numAdditionalWrites = 1_000;
            int numFinalWrites = 5;
            try (var writer1 = tree.writer(NULL_CONTEXT)) {
                for (int i = 0; i < numInitialWrites; i++) {
                    writer1.put(new MutableLong(i), new MutableLong(i));
                }
                writer1.yield();

                // when writer in another thread adds data, enough to for sure require pessimistic mode (split root)
                t2.execute(() -> {
                    try (var writer2 = tree.writer(NULL_CONTEXT)) {
                        for (int i = 0; i < numAdditionalWrites; i++) {
                            int value = numInitialWrites + i;
                            writer2.put(new MutableLong(value), new MutableLong(value));
                        }
                    }
                    return null;
                });

                // and when using the first writer to add even more data
                for (int i = 0; i < numFinalWrites; i++) {
                    int value = numInitialWrites + numAdditionalWrites + i;
                    writer1.put(new MutableLong(value), new MutableLong(value));
                }
            }

            // then all writes should complete and all data should be there
            int numWrites = numInitialWrites + numAdditionalWrites + numFinalWrites;
            try (var seek = tree.seek(new MutableLong(0), new MutableLong(numWrites), NULL_CONTEXT)) {
                for (long expected = 0; expected < numWrites; expected++) {
                    assertThat(seek.next()).isTrue();
                    assertThat(seek.key().longValue()).isEqualTo(expected);
                    assertThat(seek.value().longValue()).isEqualTo(expected);
                }
                assertThat(seek.next()).isFalse();
            }
        }
    }

    private static void assertZeroCursor(CursorContext cursorContext) {
        var cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isZero();
        assertThat(cursorTracer.unpins()).isZero();
        assertThat(cursorTracer.hits()).isZero();
        assertThat(cursorTracer.faults()).isZero();
    }

    private static class KeyCountingVisitor extends GBPTreeVisitor.Adaptor<SingleRoot, MutableLong, MutableLong> {
        private boolean newLevel;
        private final List<Integer> keyCountOnLeftmostPerLevel = new ArrayList<>();
        private final List<Integer> keyCountOnRightmostPerLevel = new ArrayList<>();
        private int rightmostKeyCountOnLevelSoFar;

        @Override
        public void beginLevel(int level) {
            newLevel = true;
            rightmostKeyCountOnLevelSoFar = -1;
        }

        @Override
        public void endLevel(int level) {
            keyCountOnRightmostPerLevel.add(rightmostKeyCountOnLevelSoFar);
        }

        @Override
        public void beginNode(long pageId, boolean isLeaf, long generation, int keyCount) {
            if (newLevel) {
                newLevel = false;
                keyCountOnLeftmostPerLevel.add(keyCount);
            }
            rightmostKeyCountOnLevelSoFar = keyCount;
        }
    }
}
