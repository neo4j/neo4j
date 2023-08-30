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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.DATA_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.ROOT_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.setKeyCount;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.test.utils.PageCacheConfig.config;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
@ExtendWith(RandomExtension.class)
class CrashGenerationCleanerTest {
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private RandomSupport randomRule;

    private static final String FILE_NAME = "index";
    private static final String DATABASE_NAME = "neo4j";
    private static final int PAGE_SIZE = 256;

    private PagedFile pagedFile;
    private PageCache pageCache;
    private final Layout<MutableLong, MutableLong> dataLayout = longLayout().build();
    private final LeafNodeFixedSize<MutableLong, MutableLong> dataLeafNode =
            new LeafNodeFixedSize<>(PAGE_SIZE, dataLayout);
    private final InternalNodeFixedSize<MutableLong> dataInternalNode =
            new InternalNodeFixedSize<>(PAGE_SIZE, dataLayout);
    private final Layout<RawBytes, RawBytes> rootLayout = new SimpleByteArrayLayout();
    private final LeafNodeDynamicSize<RawBytes, RawBytes> rootLeafNode =
            new LeafNodeDynamicSize<>(PAGE_SIZE, rootLayout, null);
    private final InternalNodeDynamicSize<RawBytes> rootInternalNode =
            new InternalNodeDynamicSize<>(PAGE_SIZE, rootLayout, null);
    private static ExecutorService executorService;
    private static CleanupJob.Executor executor;
    private final TreeState checkpointedTreeState = new TreeState(0, 9, 10, 0, 0, 0, 0, 0, 0, 0, true, true);
    private final TreeState unstableTreeState = new TreeState(0, 10, 12, 0, 0, 0, 0, 0, 0, 0, true, true);
    private final List<GBPTreeCorruption.PageCorruption> possibleCorruptionsInInternal = Arrays.asList(
            GBPTreeCorruption.crashed(GBPTreePointerType.leftSibling()),
            GBPTreeCorruption.crashed(GBPTreePointerType.rightSibling()),
            GBPTreeCorruption.crashed(GBPTreePointerType.successor()),
            GBPTreeCorruption.crashed(GBPTreePointerType.child(0)));
    private final List<GBPTreeCorruption.PageCorruption> possibleCorruptionsInLeaf = Arrays.asList(
            GBPTreeCorruption.crashed(GBPTreePointerType.leftSibling()),
            GBPTreeCorruption.crashed(GBPTreePointerType.rightSibling()),
            GBPTreeCorruption.crashed(GBPTreePointerType.successor()));

    @BeforeAll
    static void setUp() {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        executor = new CleanupJob.Executor() {
            @Override
            public <T> CleanupJob.JobResult<T> submit(String jobDescription, Callable<T> job) {
                var future = executorService.submit(job);
                return future::get;
            }
        };
    }

    @AfterAll
    static void tearDown() {
        executorService.shutdown();
    }

    @BeforeEach
    void setupPagedFile() throws IOException {
        pageCache = PageCacheSupportExtension.getPageCache(
                fileSystem, config().withPageSize(PAGE_SIZE).withAccessChecks(true));
        pagedFile = pageCache.map(
                testDirectory.file(FILE_NAME), PAGE_SIZE, DATABASE_NAME, immutable.of(CREATE, DELETE_ON_CLOSE));
    }

    @AfterEach
    void teardownPagedFile() {
        pagedFile.close();
        pageCache.close();
    }

    @Test
    void shouldNotCrashOnEmptyFile() throws Exception {
        // GIVEN
        Page[] pages = with();
        initializeFile(pagedFile, pages);

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner(pagedFile, 0, pages.length, monitor).clean(executor);

        // THEN
        assertPagesVisited(monitor, pages.length);
        assertTreeNodes(monitor, pages.length);
        assertCleanedCrashPointers(monitor, 0);
    }

    @Test
    void shouldNotReportErrorsOnCleanPages() throws Exception {
        // GIVEN
        Page[] pages = with(dataLeafWith(), dataInternalWith());
        initializeFile(pagedFile, pages);

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner(pagedFile, 0, pages.length, monitor).clean(executor);

        // THEN
        assertPagesVisited(monitor, pages.length);
        assertTreeNodes(monitor, pages.length);
        assertCleanedCrashPointers(monitor, 0);
    }

    @Test
    void shouldCleanOneCrashPerPage() throws Exception {
        // GIVEN
        Page[] pages = with(
                // === root ===
                /* left sibling */
                rootLeafWith(GBPTreeCorruption.crashed(GBPTreePointerType.leftSibling())),
                rootInternalWith(GBPTreeCorruption.crashed(GBPTreePointerType.leftSibling())),

                /* right sibling */
                rootLeafWith(GBPTreeCorruption.crashed(GBPTreePointerType.rightSibling())),
                rootInternalWith(GBPTreeCorruption.crashed(GBPTreePointerType.rightSibling())),

                /* successor */
                rootLeafWith(GBPTreeCorruption.crashed(GBPTreePointerType.successor())),
                rootInternalWith(GBPTreeCorruption.crashed(GBPTreePointerType.successor())),

                /* child */
                rootInternalWith(GBPTreeCorruption.crashed(GBPTreePointerType.child(0))),

                // === data ===
                /* left sibling */
                dataLeafWith(GBPTreeCorruption.crashed(GBPTreePointerType.leftSibling())),
                dataInternalWith(GBPTreeCorruption.crashed(GBPTreePointerType.leftSibling())),

                /* right sibling */
                dataLeafWith(GBPTreeCorruption.crashed(GBPTreePointerType.rightSibling())),
                dataInternalWith(GBPTreeCorruption.crashed(GBPTreePointerType.rightSibling())),

                /* successor */
                dataLeafWith(GBPTreeCorruption.crashed(GBPTreePointerType.successor())),
                dataInternalWith(GBPTreeCorruption.crashed(GBPTreePointerType.successor())),

                /* child */
                dataInternalWith(GBPTreeCorruption.crashed(GBPTreePointerType.child(0))));
        initializeFile(pagedFile, pages);

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner(pagedFile, 0, pages.length, monitor).clean(executor);

        // THEN
        assertPagesVisited(monitor, pages.length);
        assertTreeNodes(monitor, pages.length);
        assertCleanedCrashPointers(monitor, 14);
    }

    @Test
    void shouldCleanMultipleCrashPerPage() throws Exception {
        // GIVEN
        Page[] pages = with(
                dataLeafWith(
                        GBPTreeCorruption.crashed(GBPTreePointerType.leftSibling()),
                        GBPTreeCorruption.crashed(GBPTreePointerType.rightSibling()),
                        GBPTreeCorruption.crashed(GBPTreePointerType.successor())),
                dataInternalWith(
                        GBPTreeCorruption.crashed(GBPTreePointerType.leftSibling()),
                        GBPTreeCorruption.crashed(GBPTreePointerType.rightSibling()),
                        GBPTreeCorruption.crashed(GBPTreePointerType.successor()),
                        GBPTreeCorruption.crashed(GBPTreePointerType.child(0))));
        initializeFile(pagedFile, pages);

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner(pagedFile, 0, pages.length, monitor).clean(executor);

        // THEN
        assertPagesVisited(monitor, pages.length);
        assertTreeNodes(monitor, pages.length);
        assertCleanedCrashPointers(monitor, 7);
    }

    @Test
    void shouldNotCleanOffloadOrFreelistPages() throws IOException {
        // GIVEN
        Page[] pages = with(offload(), freelist());
        initializeFile(pagedFile, pages);

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner(pagedFile, 0, pages.length, monitor).clean(executor);

        // THEN
        assertPagesVisited(monitor, 2);
        assertTreeNodes(monitor, 0);
        assertCleanedCrashPointers(monitor, 0);
    }

    @Test
    void shouldCleanLargeFile() throws Exception {
        // GIVEN
        int numberOfPages = randomRule.intBetween(1_000, 10_000);
        int corruptionPercent = randomRule.nextInt(90);
        MutableInt totalNumberOfCorruptions = new MutableInt(0);

        Page[] pages = new Page[numberOfPages];
        for (int i = 0; i < numberOfPages; i++) {
            Page page = randomPage(corruptionPercent, totalNumberOfCorruptions);
            pages[i] = page;
        }
        initializeFile(pagedFile, pages);

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner(pagedFile, 0, numberOfPages, monitor).clean(executor);

        // THEN
        assertPagesVisited(monitor, numberOfPages);
        assertTreeNodes(monitor, numberOfPages);
        assertCleanedCrashPointers(monitor, totalNumberOfCorruptions.getValue());
    }

    @Test
    void tracePageCacheAccessInCleaners() throws IOException {
        int numberOfPages = randomRule.intBetween(100, 1000);
        Page[] pages = new Page[numberOfPages];
        for (int i = 0; i < numberOfPages; i++) {
            Page page = randomPage(0, new MutableInt());
            pages[i] = page;
        }
        initializeFile(pagedFile, pages);
        var cacheTracer = new DefaultPageCacheTracer();

        assertThat(cacheTracer.pins()).isZero();
        assertThat(cacheTracer.unpins()).isZero();
        assertThat(cacheTracer.hits()).isZero();

        var cursorContextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);

        var cleaner = new CrashGenerationCleaner(
                pagedFile,
                null,
                dataInternalNode,
                0,
                pages.length,
                unstableTreeState.stableGeneration(),
                unstableTreeState.unstableGeneration(),
                NO_MONITOR,
                cursorContextFactory,
                "test tree");
        cleaner.clean(executor);

        assertThat(cacheTracer.pins()).isEqualTo(pages.length);
        assertThat(cacheTracer.unpins()).isEqualTo(pages.length);
        assertThat(cacheTracer.hits()).isEqualTo(pages.length);
    }

    private CrashGenerationCleaner crashGenerationCleaner(
            PagedFile pagedFile, int lowTreeNodeId, int highTreeNodeId, SimpleCleanupMonitor monitor) {
        return new CrashGenerationCleaner(
                pagedFile,
                rootInternalNode,
                dataInternalNode,
                lowTreeNodeId,
                highTreeNodeId,
                unstableTreeState.stableGeneration(),
                unstableTreeState.unstableGeneration(),
                monitor,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                "test tree");
    }

    private void initializeFile(PagedFile pagedFile, Page... pages) throws IOException {
        try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, CursorContext.NULL_CONTEXT)) {
            for (Page page : pages) {
                cursor.next();
                var data = page.type.isData;
                var leafNode = data ? dataLeafNode : rootLeafNode;
                var internalNode = data ? dataInternalNode : rootInternalNode;
                var layout = data ? dataLayout : rootLayout;
                page.write(cursor, leafNode, internalNode, layout, checkpointedTreeState, unstableTreeState);
            }
        }
    }

    /* Assertions */
    private static void assertCleanedCrashPointers(
            SimpleCleanupMonitor monitor, int expectedNumberOfCleanedCrashPointers) {
        assertEquals(
                expectedNumberOfCleanedCrashPointers,
                monitor.numberOfCleanedCrashPointers,
                "Expected number of cleaned crash pointers to be " + expectedNumberOfCleanedCrashPointers + " but was "
                        + monitor.numberOfCleanedCrashPointers);
    }

    private static void assertPagesVisited(SimpleCleanupMonitor monitor, int expectedNumberOfPagesVisited) {
        assertEquals(
                expectedNumberOfPagesVisited,
                monitor.numberOfPagesVisited,
                "Expected number of visited pages to be " + expectedNumberOfPagesVisited + " but was "
                        + monitor.numberOfPagesVisited);
    }

    private static void assertTreeNodes(SimpleCleanupMonitor monitor, int expectedNumberOfTreeNodes) {
        assertEquals(
                expectedNumberOfTreeNodes,
                monitor.numberOfTreeNodes,
                "Expected number of TreeNodes to be " + expectedNumberOfTreeNodes + " but was "
                        + monitor.numberOfTreeNodes);
    }

    /* Random page */
    private Page randomPage(int corruptionPercent, MutableInt totalNumberOfCorruptions) {
        int numberOfCorruptions = 0;
        boolean internal = randomRule.nextBoolean();
        if (randomRule.nextInt(100) < corruptionPercent) {
            int maxCorruptions = internal ? possibleCorruptionsInInternal.size() : possibleCorruptionsInLeaf.size();
            numberOfCorruptions = randomRule.intBetween(1, maxCorruptions);
            totalNumberOfCorruptions.add(numberOfCorruptions);
        }
        return internal ? randomInternal(numberOfCorruptions) : randomLeaf(numberOfCorruptions);
    }

    private Page randomLeaf(int numberOfCorruptions) {
        Collections.shuffle(possibleCorruptionsInLeaf);
        GBPTreeCorruption.PageCorruption[] corruptions = new GBPTreeCorruption.PageCorruption[numberOfCorruptions];
        for (int i = 0; i < numberOfCorruptions; i++) {
            corruptions[i] = possibleCorruptionsInLeaf.get(i);
        }
        return dataLeafWith(corruptions);
    }

    private Page randomInternal(int numberOfCorruptions) {
        Collections.shuffle(possibleCorruptionsInInternal);
        GBPTreeCorruption.PageCorruption[] corruptions = new GBPTreeCorruption.PageCorruption[numberOfCorruptions];
        for (int i = 0; i < numberOfCorruptions; i++) {
            corruptions[i] = possibleCorruptionsInInternal.get(i);
        }
        return dataInternalWith(corruptions);
    }

    /* Page */
    private static Page[] with(Page... pages) {
        return pages;
    }

    private static Page dataLeafWith(GBPTreeCorruption.PageCorruption<MutableLong, MutableLong>... pageCorruptions) {
        return new Page(PageType.DATA_LEAF, pageCorruptions);
    }

    private static Page dataInternalWith(
            GBPTreeCorruption.PageCorruption<MutableLong, MutableLong>... pageCorruptions) {
        return new Page(PageType.DATA_INTERNAL, pageCorruptions);
    }

    private static Page rootLeafWith(GBPTreeCorruption.PageCorruption<MutableLong, MutableLong>... pageCorruptions) {
        return new Page(PageType.ROOT_LEAF, pageCorruptions);
    }

    private static Page rootInternalWith(
            GBPTreeCorruption.PageCorruption<MutableLong, MutableLong>... pageCorruptions) {
        return new Page(PageType.ROOT_INTERNAL, pageCorruptions);
    }

    private static Page offload() {
        return new Page(PageType.OFFLOAD);
    }

    private static Page freelist() {
        return new Page(PageType.FREELIST);
    }

    private static class Page {
        private final PageType type;
        private final GBPTreeCorruption.PageCorruption<MutableLong, MutableLong>[] pageCorruptions;

        private Page(PageType type, GBPTreeCorruption.PageCorruption<MutableLong, MutableLong>... pageCorruptions) {
            this.type = type;
            this.pageCorruptions = pageCorruptions;
        }

        private void write(
                PageCursor cursor,
                LeafNodeBehaviour leafNode,
                InternalNodeBehaviour internalNode,
                Layout layout,
                TreeState checkpointedTreeState,
                TreeState unstableTreeState)
                throws IOException {
            type.write(cursor, leafNode, internalNode, layout, checkpointedTreeState);
            for (GBPTreeCorruption.PageCorruption<MutableLong, MutableLong> pc : pageCorruptions) {
                pc.corrupt(cursor, layout, leafNode, internalNode, unstableTreeState);
            }
        }
    }

    enum PageType {
        DATA_LEAF(true) {
            @Override
            <KEY> void write(
                    PageCursor cursor,
                    LeafNodeBehaviour<KEY, ?> leafNode,
                    InternalNodeBehaviour<KEY> internalNode,
                    Layout<KEY, ?> layout,
                    TreeState treeState) {
                long stableGeneration = treeState.stableGeneration();
                long unstableGeneration = treeState.unstableGeneration();
                leafNode.initialize(cursor, DATA_LAYER_FLAG, stableGeneration, unstableGeneration);
            }
        },
        DATA_INTERNAL(true) {
            @Override
            <KEY> void write(
                    PageCursor cursor,
                    LeafNodeBehaviour<KEY, ?> leafNode,
                    InternalNodeBehaviour<KEY> internalNode,
                    Layout<KEY, ?> layout,
                    TreeState treeState) {
                writeInternal(cursor, DATA_LAYER_FLAG, internalNode, layout, treeState);
            }
        },
        ROOT_LEAF(false) {
            @Override
            <KEY> void write(
                    PageCursor cursor,
                    LeafNodeBehaviour<KEY, ?> leafNode,
                    InternalNodeBehaviour<KEY> internalNode,
                    Layout<KEY, ?> layout,
                    TreeState treeState) {
                long stableGeneration = treeState.stableGeneration();
                long unstableGeneration = treeState.unstableGeneration();
                leafNode.initialize(cursor, ROOT_LAYER_FLAG, stableGeneration, unstableGeneration);
            }
        },
        ROOT_INTERNAL(false) {
            @Override
            <KEY> void write(
                    PageCursor cursor,
                    LeafNodeBehaviour<KEY, ?> leafNode,
                    InternalNodeBehaviour<KEY> internalNode,
                    Layout<KEY, ?> layout,
                    TreeState treeState) {
                writeInternal(cursor, ROOT_LAYER_FLAG, internalNode, layout, treeState);
            }
        },
        OFFLOAD(true) {
            @Override
            <KEY> void write(
                    PageCursor cursor,
                    LeafNodeBehaviour<KEY, ?> leafNode,
                    InternalNodeBehaviour<KEY> internalNode,
                    Layout<KEY, ?> layout,
                    TreeState treeState) {
                OffloadStoreImpl.writeHeader(cursor);
            }
        },
        FREELIST(true) {
            @Override
            <KEY> void write(
                    PageCursor cursor,
                    LeafNodeBehaviour<KEY, ?> leafNode,
                    InternalNodeBehaviour<KEY> internalNode,
                    Layout<KEY, ?> layout,
                    TreeState treeState) {
                FreelistNode.initialize(cursor);
            }
        };

        private final boolean isData;

        PageType(boolean isData) {
            this.isData = isData;
        }

        abstract <KEY> void write(
                PageCursor cursor,
                LeafNodeBehaviour<KEY, ?> leafNode,
                InternalNodeBehaviour<KEY> internalNode,
                Layout<KEY, ?> layout,
                TreeState treeState);

        <KEY> void writeInternal(
                PageCursor cursor,
                byte layerType,
                InternalNodeBehaviour<KEY> internalNode,
                Layout<KEY, ?> layout,
                TreeState treeState) {
            long stableGeneration = treeState.stableGeneration();
            long unstableGeneration = treeState.unstableGeneration();
            internalNode.initialize(cursor, layerType, stableGeneration, unstableGeneration);
            long base = IdSpace.MIN_TREE_NODE_ID;
            int keyCount;
            KEY newKey = layout.newKey();
            for (keyCount = 0; internalNode.overflow(cursor, keyCount, newKey) == Overflow.NO; keyCount++) {
                long child = base + keyCount;
                long stableGeneration1 = treeState.stableGeneration();
                long unstableGeneration1 = treeState.unstableGeneration();
                internalNode.setChildAt(cursor, child, keyCount, stableGeneration1, unstableGeneration1);
            }
            setKeyCount(cursor, keyCount);
        }
    }
}
