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

import static org.neo4j.index.internal.gbptree.InternalTreeLogic.DEFAULT_SPLIT_RATIO;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.DATA_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.NODE_TYPE_TREE_NODE;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.ROOT_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.generation;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.goTo;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.nodeType;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.successor;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

import java.io.IOException;
import java.util.BitSet;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.version.FileTruncateEvent;
import org.neo4j.time.Stopwatch;
import org.neo4j.util.VisibleForTesting;

/**
 * The upper layer in a GBPTree that manages mappings from root keys to data tree.
 *
 * <pre>
 *             .-~~~-.
 *      .- ~ ~-(       )_ _
 *     /                     ~ -.
 *    |        Root layer         \
 *     \                         .'
 *       ~- . _____________ . -~
 *       /          |        \
 *  data tree   data tree   data tree
 * </pre>
 *
 * Data trees are trees that hold the actual data of a GBPTree and the root layer holds mappings from a user-provided root key to a data tree.
 * Data trees can be explicitly {@link #create(Object, CursorContext) created} {@link #delete(Object, CursorContext) deleted} and
 * {@link #access(Object) accessed} from this layer.
 *
 * @param <ROOT_KEY> layout of keys for the root key -> data tree mappings.
 * @param <DATA_KEY> layout of the data keys.
 * @param <DATA_VALUE> layout of the data values.
 */
abstract class RootLayer<ROOT_KEY, DATA_KEY, DATA_VALUE> implements TreeRootExchange {
    private static final int SMALL_TREE_COMPACTION_THRESHOLD = 10;

    protected final RootLayerSupport support;
    protected final TreeNodeSelector treeNodeSelector;
    protected volatile Root root;
    // Kept and ref:ed for performance. Just keeping it ref:ed doesn't actually latch it, just keeps it
    // in the map of active latches to avoid that overhead.
    private volatile LongSpinLatch rootLatch;

    RootLayer(RootLayerSupport support, TreeNodeSelector treeNodeSelector) {
        this.support = support;
        this.treeNodeSelector = treeNodeSelector;
    }

    /**
     * Called on first startup when the GBPTree file gets created. Typically initializes the top-level root.
     *
     * @param firstRoot The first {@link Root} of the tree
     * @param cursorContext the {@link CursorContext}.
     * @throws IOException on I/O error.
     */
    abstract void initializeAfterCreation(Root firstRoot, CursorContext cursorContext) throws IOException;

    /**
     * Called on startup when the GBPTree file already exists. Typically, verify metadata.
     *
     * @param root new root from now on.
     * @param cursorContext the {@link CursorContext}.
     * @throws IOException on I/O error.
     */
    abstract void initialize(Root root, CursorContext cursorContext) throws IOException;
    /**
     * Creates a root mapping from the given {@code rootKey} to a new data tree and initializes the data tree.
     * @param rootKey key which maps to the created data tree.
     * @param cursorContext the {@link CursorContext}.
     * @throws IOException on I/O error.
     * @throws DataTreeAlreadyExistsException if there's already a mapping for the given {@code rootKey}.
     */
    abstract void create(ROOT_KEY rootKey, CursorContext cursorContext) throws IOException;

    /**
     * Deletes a root mapping for the given {@code rootKey} along with the empty data tree.
     * The data tree that this key maps to must be empty when performing this operation.
     * @param rootKey key which maps to the data tree
     * @param cursorContext the {@link CursorContext}.
     * @throws IOException on I/O error.
     * @throws DataTreeNotEmptyException if the data tree is not empty.
     * @throws DataTreeNotFoundException if no root mapping for the given {@code rootKey} exists.
     */
    abstract void delete(ROOT_KEY rootKey, CursorContext cursorContext) throws IOException;

    /**
     * Accesses the data tree that the given {@code rootKey} maps to. This method does not explicitly check that the root mapping exists
     * since all actual interaction with the {@link DataTree} will do this.
     * @param rootKey key which maps to the data tree.
     * @return the {@link DataTree} for the given {@code rootKey}.
     */
    abstract DataTree<DATA_KEY, DATA_VALUE> access(ROOT_KEY rootKey);

    /**
     * Current root of this root layer (id and generation where it was assigned). In the rare event of creating a new root
     * a new {@link Root} instance will be created and assigned to this variable.
     *
     * For reading id and generation atomically a reader can first grab a local reference to this variable
     * and then call {@link Root#id()} and {@link Root#generation()}, or use {@link Root#goTo(PageCursor)}
     * directly, which moves the page cursor to the id and returns the generation.
     */
    @Override
    public Root getRoot(CursorContext context) {
        return root;
    }

    @Override
    public void setRoot(Root root, CursorContext context) {
        if (rootLatch != null) {
            rootLatch.deref();
            rootLatch = null;
        }
        this.root = root;
        rootLatch = support.latchService().latch(root.id());
    }

    /**
     * Visits all types of content found in a {@link MultiRootGBPTree}, like metadata, root keys and actual data entries (key/value pairs).
     * @param visitor observing the content.
     * @param cursorContext the {@link CursorContext}.
     * @throws IOException on I/O error.
     */
    abstract void visit(GBPTreeVisitor<ROOT_KEY, DATA_KEY, DATA_VALUE> visitor, CursorContext cursorContext)
            throws IOException;

    /**
     * Checks consistency of the root layer as well as all data trees.
     * @param state structure to keep internal state while checking all trees.
     * @param visitor gets notified about potential inconsistencies.
     * @param reportDirty whether to report (to the visitor) about GBPTree being dirty, i.e. not cleanly shut down before checking.
     * @param contextFactory for creating cursor contexts.
     * @param numThreads number of threads to use for the check.
     * @throws IOException on I/O error.
     */
    abstract void consistencyCheck(
            GBPTreeConsistencyChecker.ConsistencyCheckState state,
            GBPTreeConsistencyCheckVisitor visitor,
            boolean reportDirty,
            CursorContextFactory contextFactory,
            int numThreads)
            throws IOException;

    /**
     * @return max possible key+value size of one entry in any data tree.
     */
    abstract int keyValueSizeCap();

    /**
     * @return max possible key+value size of one inlined entry in any data tree.
     */
    abstract int inlineKeyValueSizeCap();

    /**
     * @return max number of keys that can fit into single data leaf node
     */
    abstract int leafNodeMaxKeys();

    /**
     * Visits keys for all existing root mappings.
     *
     * @param cursorContext the {@link CursorContext}.
     * @param visitor       called for each existing root mapping.
     * @throws IOException on I/O error.
     */
    abstract void visitAllDataTreeRoots(CursorContext cursorContext, TreeRootsVisitor<ROOT_KEY> visitor)
            throws IOException;

    /**
     * Performs unsafe operations directly on the underlying pages.
     * @param unsafe the unsafe operation to perform.
     * @param dataTree {@code true} if the operation is on a data tree, otherwise it's on a root tree.
     * @param cursorContext the {@link CursorContext}.
     * @throws IOException on I/O error.
     */
    abstract <K, V> void unsafe(GBPTreeUnsafe<K, V> unsafe, boolean dataTree, CursorContext cursorContext)
            throws IOException;

    /**
     * Creates a {@link CrashGenerationCleaner} for running as part of starting a GBPTree which hasn't been cleanly check-pointed and shut down.
     * @param contextFactory the {@link CursorContextFactory}.
     * @return CrashGenerationCleaner for cleaning crash-pointers.
     */
    abstract CrashGenerationCleaner createCrashGenerationCleaner(CursorContextFactory contextFactory);

    /**
     * Prints the tree node, in textual form, that the given {@code cursor} is currently at.
     * @param cursor the {@link PageCursor} to read data from. This cursor should be placed at the tree node to be printed.
     * @param cursorContext the {@link CursorContext}.
     */
    abstract void printNode(PageCursor cursor, CursorContext cursorContext);

    abstract <K, V> GBPTreeWriter<K, V> writer(byte layerType);

    /**
     * @param flags flags, typically provided by the user to the writer.
     * @return the split ratio to be used, given the flags.
     */
    static double splitRatio(int flags) {
        if ((flags & DataTree.W_SPLIT_KEEP_ALL_LEFT) != 0) {
            return 1;
        } else if ((flags & DataTree.W_SPLIT_KEEP_ALL_RIGHT) != 0) {
            return 0;
        }
        return DEFAULT_SPLIT_RATIO;
    }

    abstract void visitDataTreeRoots(
            CursorContext cursorContext,
            TreeRootsVisitor<ROOT_KEY> visitor,
            ROOT_KEY fromInclusiveKey,
            ROOT_KEY toExclusiveKey)
            throws IOException;

    @VisibleForTesting
    public abstract void clearCache();

    CompactionReport runCompaction(
            int maxNumHighPages, long stableGeneration, long unstableGeneration, CursorContext cursorContext)
            throws IOException {
        // Look at the high pages in this file
        try (IdProvider.ExclusiveAccessMode exclusiveAccess =
                support.idProvider().exclusiveAccess()) {
            Stopwatch stopwatch = Stopwatch.start();
            long highId = support.idProvider().lastId() + 1;
            long numAvailableFreeIds = countAvailableFreeIds(stableGeneration, cursorContext);
            int numHighPages = (int) Math.min(maxNumHighPages, numAvailableFreeIds * 0.5);
            long baseId = Math.max(highId - numHighPages, IdSpace.MIN_TREE_NODE_ID + SMALL_TREE_COMPACTION_THRESHOLD);
            numHighPages = Math.toIntExact(Math.max(0, highId - baseId));

            BitSet actuallyFree = new BitSet(numHighPages);
            BitSet candidatesToMove;
            CompactionFreelistVisitor visitor = new CompactionFreelistVisitor(baseId, stableGeneration, actuallyFree);

            // The problem with data-layer tree nodes is that we don't know which data root to start from when
            // finding them (for forcing successor creations).
            try (PageCursor cursor = support.pagedFile().io(0, PF_SHARED_READ_LOCK, cursorContext)) {
                // Strategy: go through the freelist
                support.idProvider().visitFreelist(visitor, CursorCreator.bind(cursor));

                // Strategy: scan through the pages
                candidatesToMove = findCompactionCandidatesToMove(
                        numHighPages, stableGeneration, unstableGeneration, actuallyFree, baseId, cursor);
            }

            if (visitor.freelistIsHighUp) {
                rewriteFreelist(
                        exclusiveAccess, stableGeneration, unstableGeneration, cursorContext, baseId, actuallyFree);
            }

            // If there are some candidate pages to move in this segment then try to move them down
            int numSuccessorsCreated = 0;
            int numUnnecessarySuccessorsCreated = 0;
            byte writerLayerType = this instanceof MultiRootLayer ? ROOT_LAYER_FLAG : DATA_LAYER_FLAG;
            if (!candidatesToMove.isEmpty()) {
                int i = -1;
                while ((i = candidatesToMove.nextSetBit(i + 1)) != -1) {
                    long pageId = baseId + i;
                    // Can we just poke this one to create a successor, and hope the new successor will get a lower ID?
                    try (GBPTreeWriter<?, ?> writer = writer(writerLayerType)) {
                        writer.initialize(InternalTreeLogic.DEFAULT_SPLIT_RATIO, cursorContext);
                        OptionalLong newSuccessor = writer.forceCreateSuccessor(pageId);
                        if (newSuccessor.isPresent()) {
                            numSuccessorsCreated++;
                            actuallyFree.set(i);
                            long newSuccessorId = newSuccessor.getAsLong();
                            if (newSuccessorId >= baseId) {
                                // Oops, a treenode moved, but ended up in the high range
                                int occupiedBitIndex = Math.toIntExact(newSuccessorId - baseId);
                                actuallyFree.clear(occupiedBitIndex);
                                numUnnecessarySuccessorsCreated++;
                            }
                        }
                    }
                }
            }

            support.idProvider()
                    .flush(
                            stableGeneration,
                            unstableGeneration,
                            CursorCreator.bind(support.pagedFile(), PF_SHARED_WRITE_LOCK, cursorContext),
                            acquiredId -> {
                                if (acquiredId >= baseId) {
                                    actuallyFree.clear(Math.toIntExact(acquiredId - baseId));
                                }
                            });

            // Either all of these "high pages" are now free, or at least the top N of them. Figure out N.
            int highestNotFreeBitIndex = actuallyFree.previousClearBit(numHighPages - 1);
            int numTrimmedPages;
            if (support.idProvider().lastId() + 1 == highId) {
                numTrimmedPages =
                        highestNotFreeBitIndex == -1 ? numHighPages : numHighPages - (highestNotFreeBitIndex + 1);
                if (numTrimmedPages > 0) {
                    exclusiveAccess.shrink(numTrimmedPages);
                }
            } // else for some reason the act of moving these pages increased the high ID, so we can't trim then.'

            return new CompactionReport(
                    support.pagedFile().path(),
                    highId,
                    support.idProvider().lastId() + 1,
                    visitor.freelistIsHighUp,
                    numHighPages,
                    actuallyFree.cardinality(),
                    numSuccessorsCreated,
                    numAvailableFreeIds,
                    visitor.numAvailableIdsBelowBaseId,
                    numUnnecessarySuccessorsCreated,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private void rewriteFreelist(
            IdProvider.ExclusiveAccessMode exclusiveAccess,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext,
            long baseId,
            BitSet actuallyFree)
            throws IOException {
        IdProvider.RewriteResult freelistRewrite = exclusiveAccess.rewrite(
                CursorCreator.bind(support.pagedFile(), PagedFile.PF_SHARED_WRITE_LOCK, cursorContext),
                baseId,
                stableGeneration,
                unstableGeneration,
                cursorContext);
        if (freelistRewrite != null) {
            for (long freedFreelistPageId : freelistRewrite.idsBefore()) {
                if (freedFreelistPageId >= baseId) {
                    actuallyFree.set(Math.toIntExact(freedFreelistPageId - baseId));
                }
            }
            for (long newFreelistPageId : freelistRewrite.idsAfter()) {
                if (newFreelistPageId >= baseId) {
                    actuallyFree.clear(Math.toIntExact(newFreelistPageId - baseId));
                }
            }
        }
    }

    private BitSet findCompactionCandidatesToMove(
            int numHighPages,
            long stableGeneration,
            long unstableGeneration,
            BitSet actuallyFree,
            long baseId,
            PageCursor cursor)
            throws IOException {
        BitSet candidatesToMove = new BitSet(numHighPages);
        boolean isMultiRoot = this instanceof MultiRootLayer;
        Predicate<Byte> layerTypeFilter = isMultiRoot ? t -> t == ROOT_LAYER_FLAG : t -> true;

        int i = -1;
        while ((i = actuallyFree.nextClearBit(i + 1)) != -1) {
            if (i >= numHighPages) {
                break;
            }
            long pageId = baseId + i;
            byte nodeType;
            byte layerType;
            PointerWithGeneration successor;
            long generation;
            goTo(cursor, "compaction check", pageId);
            do {
                nodeType = nodeType(cursor);
                layerType = TreeNodeUtil.layerType(cursor);
                successor = successor(cursor, stableGeneration, unstableGeneration);
                generation = generation(cursor);
            } while (cursor.shouldRetry());

            if (nodeType == NODE_TYPE_TREE_NODE
                    && successor.isNull()
                    && generation <= stableGeneration
                    && layerTypeFilter.test(layerType)) {
                candidatesToMove.set(i);
            }
        }
        return candidatesToMove;
    }

    private long countAvailableFreeIds(long stableGeneration, CursorContext cursorContext) throws IOException {
        try (PageCursor cursor = support.pagedFile().io(0, PF_SHARED_READ_LOCK, cursorContext)) {
            MutableLong count = new MutableLong();
            support.idProvider()
                    .visitFreelist(
                            new IdProvider.IdProviderVisitor.Adaptor() {
                                @Override
                                public void freelistEntry(long pageId, long generation, int pos) {
                                    if (generation <= stableGeneration) {
                                        count.increment();
                                    }
                                }
                            },
                            CursorCreator.bind(cursor));
            return count.longValue();
        }
    }

    void postCompaction(CompactionReport compactionReport) throws IOException {
        if (compactionReport.numTrimmedPages() > 0) {
            support.pagedFile().truncate(compactionReport.newHighId(), FileTruncateEvent.NULL);
        }
    }

    @FunctionalInterface
    public interface TreeRootsVisitor<ROOT_KEY> {
        /**
         *
         * @param root the root of the tree to visit
         * @return <code>true</code> to terminate the iteration, <code>false</code> to continue.
         */
        boolean accept(ROOT_KEY root);
    }

    private static class CompactionFreelistVisitor extends IdProvider.IdProviderVisitor.Adaptor {
        private final long baseId;
        private final long stableGeneration;
        private final BitSet actuallyFree;

        private boolean freelistIsHighUp;
        private long numAvailableIdsBelowBaseId;
        private long numFreelistPages;

        CompactionFreelistVisitor(long baseId, long stableGeneration, BitSet actuallyFree) {
            this.baseId = baseId;
            this.stableGeneration = stableGeneration;
            this.actuallyFree = actuallyFree;
        }

        @Override
        public void freelistEntry(long pageId, long generation, int pos) {
            if (generation > stableGeneration) {
                return;
            }
            if (pageId < baseId) {
                numAvailableIdsBelowBaseId++;
                return;
            }
            actuallyFree.set(Math.toIntExact(pageId - baseId));
        }

        @Override
        public void beginFreelistPage(long pageId) {
            numFreelistPages++;
            if (pageId >= baseId) {
                freelistIsHighUp = true;
            }
        }
    }
}
