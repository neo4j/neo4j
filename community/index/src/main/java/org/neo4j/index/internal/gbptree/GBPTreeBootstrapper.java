/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.reserved_page_header_bytes;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.ignore;
import static org.neo4j.io.mem.MemoryAllocator.createAllocator;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.config;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.LayoutBootstrapper.Layouts;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;

public class GBPTreeBootstrapper implements Closeable {
    private static final int MAX_PAGE_PAYLOAD = (int) ByteUnit.mebiBytes(4);
    private final FileSystemAbstraction fs;
    private final JobScheduler jobScheduler;
    private final LayoutBootstrapper layoutBootstrapper;
    private final DatabaseReadOnlyChecker readOnlyChecker;
    private final CursorContextFactory contextFactory;
    private PageCache pageCache;

    public GBPTreeBootstrapper(
            FileSystemAbstraction fs,
            JobScheduler jobScheduler,
            LayoutBootstrapper layoutBootstrapper,
            DatabaseReadOnlyChecker readOnlyChecker,
            CursorContextFactory contextFactory) {
        this.fs = fs;
        this.jobScheduler = jobScheduler;
        this.layoutBootstrapper = layoutBootstrapper;
        this.readOnlyChecker = readOnlyChecker;
        this.contextFactory = contextFactory;
    }

    public Bootstrap bootstrapTree(Path file, OpenOption... additionalOptions) {
        try {
            instantiatePageCache(fs, jobScheduler, PageCache.PAGE_SIZE);
            var openOptions = immutable.of(additionalOptions);
            // Get meta information about the tree
            MetaVisitor<?, ?, ?> metaVisitor = visitMeta(file, openOptions);
            Meta meta = metaVisitor.meta;
            if (!isReasonablePageSize(meta.getPayloadSize())) {
                throw new MetadataMismatchException("Unexpected page size " + meta.getPayloadSize());
            }
            if (meta.getPayloadSize() != pageCache.payloadSize()) {
                // GBPTree was created with a different page size, re-instantiate page cache and re-read meta.
                instantiatePageCache(fs, jobScheduler, pageCachePageForPayload(meta.getPayloadSize()));
                metaVisitor = visitMeta(file, openOptions);
                meta = metaVisitor.meta;
            }
            StateVisitor<?, ?, ?> stateVisitor = visitState(file, openOptions);
            Pair<TreeState, TreeState> statePair = stateVisitor.statePair;
            TreeState state = TreeStatePair.selectNewestValidState(statePair);

            // Create layout and treeNode from meta
            Layouts layouts = layoutBootstrapper.bootstrap(meta);
            MultiRootGBPTree<?, ?, ?> tree = new MultiRootGBPTree<>(
                    pageCache,
                    file,
                    layouts.dataLayout(),
                    NO_MONITOR,
                    NO_HEADER_READER,
                    NO_HEADER_WRITER,
                    ignore(),
                    readOnlyChecker,
                    openOptions,
                    DEFAULT_DATABASE_NAME,
                    file.getFileName().toString(),
                    contextFactory,
                    layouts.rootLayerConfiguration());
            return new SuccessfulBootstrap(tree, layouts, state, meta);
        } catch (Exception e) {
            return new FailedBootstrap(e);
        }
    }

    private int pageCachePageForPayload(int payload) {
        return payload + pageCache.pageReservedBytes();
    }

    @Override
    public void close() throws IOException {
        closePageCache();
    }

    private MetaVisitor<?, ?, ?> visitMeta(Path file, ImmutableSet<OpenOption> openOptions) throws IOException {
        MetaVisitor<?, ?, ?> metaVisitor = new MetaVisitor<>();
        try (var cursorContext = contextFactory.create("TreeBootstrap")) {
            GBPTreeStructure.visitMeta(
                    pageCache, file, metaVisitor, file.getFileName().toString(), cursorContext, openOptions);
        }
        return metaVisitor;
    }

    private StateVisitor<?, ?, ?> visitState(Path file, ImmutableSet<OpenOption> openOptions) throws IOException {
        StateVisitor<?, ?, ?> stateVisitor = new StateVisitor<>();
        try (var cursorContext = contextFactory.create("TreeBootstrap")) {
            GBPTreeStructure.visitState(
                    pageCache, file, stateVisitor, file.getFileName().toString(), cursorContext, openOptions);
        }
        return stateVisitor;
    }

    private void instantiatePageCache(FileSystemAbstraction fs, JobScheduler jobScheduler, int pageSize) {
        if (pageCache != null && pageCache.pageSize() == pageSize) {
            return;
        }
        closePageCache();
        var swapper = new SingleFilePageSwapperFactory(fs, PageCacheTracer.NULL, EmptyMemoryTracker.INSTANCE);
        long expectedMemory = Math.max(MuninnPageCache.memoryRequiredForPages(100), 3L * pageSize);
        pageCache = new MuninnPageCache(
                swapper,
                jobScheduler,
                config(createAllocator(expectedMemory, EmptyMemoryTracker.INSTANCE))
                        .pageSize(pageSize)
                        .reservedPageBytes(reserved_page_header_bytes.defaultValue()));
    }

    private void closePageCache() {
        if (pageCache != null) {
            pageCache.close();
            pageCache = null;
        }
    }

    private static boolean isReasonablePageSize(int number) {
        return isReasonableSize(number);
    }

    private static boolean isReasonableSize(int payloadSize) {
        return payloadSize <= MAX_PAGE_PAYLOAD;
    }

    public interface Bootstrap {
        boolean isTree();

        MultiRootGBPTree<?, ?, ?> tree();

        Layouts layouts();

        TreeState state();

        Meta meta();
    }

    private static class FailedBootstrap implements Bootstrap {
        private final Throwable cause;

        FailedBootstrap(Throwable cause) {
            this.cause = cause;
        }

        @Override
        public boolean isTree() {
            return false;
        }

        @Override
        public MultiRootGBPTree<?, ?, ?> tree() {
            throw new IllegalStateException("Bootstrap failed", cause);
        }

        @Override
        public Layouts layouts() {
            throw new IllegalStateException("Bootstrap failed", cause);
        }

        @Override
        public TreeState state() {
            throw new IllegalStateException("Bootstrap failed", cause);
        }

        @Override
        public Meta meta() {
            throw new IllegalStateException("Bootstrap failed", cause);
        }
    }

    private record SuccessfulBootstrap(MultiRootGBPTree<?, ?, ?> tree, Layouts layouts, TreeState state, Meta meta)
            implements Bootstrap {
        @Override
        public boolean isTree() {
            return true;
        }
    }

    private static class MetaVisitor<ROOT_KEY, KEY, VALUE> extends GBPTreeVisitor.Adaptor<ROOT_KEY, KEY, VALUE> {
        private Meta meta;

        @Override
        public void meta(Meta meta) {
            this.meta = meta;
        }
    }

    private static class StateVisitor<ROOT_KEY, KEY, VALUE> extends GBPTreeVisitor.Adaptor<ROOT_KEY, KEY, VALUE> {
        private Pair<TreeState, TreeState> statePair;

        @Override
        public void treeState(Pair<TreeState, TreeState> statePair) {
            this.statePair = statePair;
        }
    }
}
