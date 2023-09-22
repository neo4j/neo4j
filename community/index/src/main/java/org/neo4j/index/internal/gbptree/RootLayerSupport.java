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

import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.LatchCrabbingCoordination.DEFAULT_RESET_FREQUENCY;
import static org.neo4j.index.internal.gbptree.PointerChecking.checkOutOfBounds;
import static org.neo4j.index.internal.gbptree.SeekCursor.DEFAULT_MAX_READ_AHEAD;
import static org.neo4j.index.internal.gbptree.SeekCursor.LEAF_LEVEL;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.util.Preconditions.checkArgument;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.function.ThrowingAction;
import org.neo4j.index.internal.gbptree.SeekCursor.Monitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;

class RootLayerSupport {
    private final PagedFile pagedFile;
    private final LongSupplier generationSupplier;
    private final Consumer<Throwable> exceptionDecorator;
    private final TreeNodeLatchService latchService;
    private final FreeListIdProvider freeList;
    private final MultiRootGBPTree.Monitor monitor;
    private final ThrowingAction<IOException> cleanCheck;
    private final ReadWriteLock checkpointLock;
    private final ReadWriteLock writerLock;
    private final AtomicBoolean changesSinceLastCheckpoint;
    private final int payloadSize;
    private final String treeName;
    private final boolean readOnly;
    private final BooleanSupplier writersMustEagerlyFlushSupplier;
    private final StructureWriteLog structureWriteLog;

    RootLayerSupport(
            PagedFile pagedFile,
            LongSupplier generationSupplier,
            Consumer<Throwable> exceptionDecorator,
            TreeNodeLatchService latchService,
            FreeListIdProvider freeList,
            MultiRootGBPTree.Monitor monitor,
            ThrowingAction<IOException> cleanCheck,
            ReadWriteLock checkpointLock,
            ReadWriteLock writerLock,
            AtomicBoolean changesSinceLastCheckpoint,
            String treeName,
            boolean readOnly,
            BooleanSupplier writersMustEagerlyFlushSupplier,
            StructureWriteLog structureWriteLog) {
        this.pagedFile = pagedFile;
        this.generationSupplier = generationSupplier;
        this.exceptionDecorator = exceptionDecorator;
        this.latchService = latchService;
        this.freeList = freeList;
        this.monitor = monitor;
        this.cleanCheck = cleanCheck;
        this.checkpointLock = checkpointLock;
        this.writerLock = writerLock;
        this.changesSinceLastCheckpoint = changesSinceLastCheckpoint;
        this.payloadSize = pagedFile.payloadSize();
        this.treeName = treeName;
        this.readOnly = readOnly;
        this.writersMustEagerlyFlushSupplier = writersMustEagerlyFlushSupplier;
        this.structureWriteLog = structureWriteLog;
    }

    <K, V> SeekCursor<K, V> internalAllocateSeeker(
            Layout<K, V> layout,
            CursorContext cursorContext,
            LeafNodeBehaviour<K, V> leafNode,
            InternalNodeBehaviour<K> internalNode)
            throws IOException {
        PageCursor cursor = pagedFile.io(0L /*ignored*/, PF_SHARED_READ_LOCK, cursorContext);
        return new SeekCursor<>(
                cursor, layout, leafNode, internalNode, generationSupplier, exceptionDecorator, cursorContext);
    }

    <K, V> Seeker<K, V> initializeSeeker(
            Seeker<K, V> seeker,
            RootSupplier rootSupplier,
            K fromInclusive,
            K toExclusive,
            int readAheadLength,
            int searchLevel,
            Monitor monitor)
            throws IOException {
        return ((SeekCursor<K, V>) seeker)
                .initialize(
                        (cursor, context) -> rootSupplier.getRoot(context).goTo(cursor),
                        new TripCountingRootCatchup(rootSupplier),
                        fromInclusive,
                        toExclusive,
                        readAheadLength,
                        searchLevel,
                        monitor);
    }

    /**
     * We want to create a given number of partitions of the range given by <code>fromInclusive</code> and <code>toExclusive</code>.
     * We want the number of entries in each partition to be as equal as possible. We let the number of leaves in each partition
     * be an estimate for the number of entries, assuming that one leaf will contain a comparable number of entries as another.
     * Each subtree on level X is divided by splitter keys on the same level or on any of the levels above. Example:
     * <pre>
     * Level 0:                  [10,                           50]
     * Level 1:     [3,    7]     ^          [13,      25]                [70,      90]
     * Level 2: [1,2] [3,6]^[8,9]   [10,11,12]  [14,20]  [25,43]   [50, 55]  [71,85]  [109,200]
     *                ===== =====   ==========
     * </pre>
     * All keys on level 0 and 1 are called splitter keys (or internal keys) because they split subtrees from each other.
     * In this tree [3,6] and [8,9] on level 2 is separated by splitter key 7 on level 1. But looking at [8,9] and [10,11,12]
     * on level 2 we see that they are separated by splitter key 10 from level 0. Similarly, each subtree on level 2 (where each
     * subtree is a single leaf node) is separated from the others by a splitter key in one of the levels above. Noting that,
     * we can begin to form a strategy for how to create our partitions.
     * <p>
     * We want to create our partitions as high up in the tree as possible, simply to terminate the partitioning step as soon as possible.
     * If we want to create three partitions in the tree above for the range [0,300) we can use the three subtrees seen from the root
     * and be done, we would the form the three key-ranges [0,10), [10,50) and [50,300) and that is our partitioning. Note that we don't
     * strictly rely on those key-ranges to correspond perfectly to separate sub-trees since concurrent updates might change the structure of
     * the tree while we are reading. We have simply used the structure of the tree, at the time of forming the partitions, as a way to
     * construct key-ranges that are estimated to contain a similar number of entries.
     * <p>
     * If we want a more fine grained partitioning we need to go to the lower parts of the tree.
     * This is what we do: We start at level 0 and collect all keys within our target range, let's say we find N keys [K1, K2,... KN].
     * In between each key and on both sides of the range is a subtree which means we now have a way to create N+1 partitions of
     * estimated equal size. The outer boundaries will be given by fromInclusive and toExclusive. If <code>N+1 < desiredNumberOfPartitions</code>
     * we need to go one level further down and include all of the keys within our range from that level as well. If we still don't
     * have enough splitter keys in our range we continue down the tree until we either have enough keys or we reach the leaf level.
     * If we reach the leaf level it means that each partition will be only a single leaf node and we do not partition any further.
     * <p>
     * If concurrent updates causes changes higher up in the tree while searching in lower levels, some splitter keys can be missed or
     * extra splitter keys may be included. This can lead to partitions being more unevenly sized but it will not affect correctness.
     *
     * @param leafNode
     * @param internalNode
     * @param fromInclusive             lower bound of the target range to seek (inclusive).
     * @param toExclusive               higher bound of the target range to seek (exclusive).
     * @param desiredNumberOfPartitions number of partitions desired by the caller. If the tree is small a lower number of partitions may be returned.
     *                                  The number of partitions will never be higher than the provided {@code desiredNumberOfPartitions}.
     * @param cursorContext             underlying page cursor cursorContext for the thread doing the partitioning.
     * @return sorted {@link List} of {@code KEY}s, corresponding to the edges of each partition. Collectively they
     * seek across the whole provided range.
     * The number of partitions is given by the size of the collection.
     * @throws IOException on error accessing the index.
     */
    <K, V> List<K> internalPartitionedSeek(
            Layout<K, V> layout,
            LeafNodeBehaviour<K, V> leafNode,
            InternalNodeBehaviour<K> internalNode,
            K fromInclusive,
            K toExclusive,
            int desiredNumberOfPartitions,
            RootSupplier rootSupplier,
            CursorContext cursorContext)
            throws IOException {
        checkArgument(
                layout.compare(fromInclusive, toExclusive) <= 0,
                "Partitioned seek only supports forward seeking for the time being");

        // Read enough splitter keys from root and downwards to create enough partitions.
        final var splitterKeysInRange = new TreeSet<>(layout);
        int numberOfSubtrees;
        int searchLevel = 0;
        do {
            final var depthMonitor = new SeekDepthMonitor();
            final var localFrom = layout.copyKey(fromInclusive, layout.newKey());
            final var localTo = layout.copyKey(toExclusive, layout.newKey());
            try (var seek = initializeSeeker(
                    internalAllocateSeeker(layout, cursorContext, leafNode, internalNode),
                    rootSupplier,
                    localFrom,
                    localTo,
                    DEFAULT_MAX_READ_AHEAD,
                    searchLevel,
                    depthMonitor)) {
                if (depthMonitor.reachedLeafLevel) {
                    // Don't partition any further if we've reached leaf level.
                    break;
                }
                while (seek.next()) {
                    splitterKeysInRange.add(layout.copyKey(seek.key(), layout.newKey()));
                }
            }
            searchLevel++;
            numberOfSubtrees = splitterKeysInRange.size() + 1;
        } while (numberOfSubtrees < desiredNumberOfPartitions);

        // From the set of splitter keys, create sorted list of partition edges
        return new KeyPartitioning<>(layout)
                .partition(splitterKeysInRange, fromInclusive, toExclusive, desiredNumberOfPartitions);
    }

    <K, V> Writer<K, V> internalParallelWriter(
            Layout<K, V> layout,
            LeafNodeBehaviour<K, V> leafNode,
            InternalNodeBehaviour<K> internalNode,
            double ratioToKeepInLeftOnSplit,
            CursorContext cursorContext,
            TreeRootExchange rootChangeMonitor,
            byte layerType)
            throws IOException {
        TreeWriterCoordination traversalMonitor =
                new LatchCrabbingCoordination(latchService, leafNode.underflowThreshold(), DEFAULT_RESET_FREQUENCY);
        GBPTreeWriter<K, V> writer =
                newWriter(layout, rootChangeMonitor, leafNode, internalNode, traversalMonitor, true, layerType);
        return initializeWriter(writer, ratioToKeepInLeftOnSplit, cursorContext);
    }

    <K, V> GBPTreeWriter<K, V> newWriter(
            Layout<K, V> layout,
            TreeRootExchange rootChangeMonitor,
            LeafNodeBehaviour<K, V> leafNode,
            InternalNodeBehaviour<K> internalNode,
            TreeWriterCoordination traversalMonitor,
            boolean parallel,
            byte layerType) {
        return new GBPTreeWriter<>(
                layout,
                pagedFile,
                traversalMonitor,
                new InternalTreeLogic<>(freeList, leafNode, internalNode, layout, monitor, traversalMonitor, layerType),
                leafNode,
                internalNode,
                parallel,
                rootChangeMonitor,
                checkpointLock,
                writerLock,
                freeList,
                monitor,
                exceptionDecorator,
                generationSupplier,
                writersMustEagerlyFlushSupplier,
                structureWriteLog.newSession());
    }

    <K, V> GBPTreeWriter<K, V> initializeWriter(
            GBPTreeWriter<K, V> writer, double ratioToKeepInLeftOnSplit, CursorContext cursorContext)
            throws IOException {
        if (readOnly) {
            throw new IllegalStateException(String.format("'%s' is read-only", pagedFile.path()));
        }
        cleanCheck.apply();
        writer.initialize(ratioToKeepInLeftOnSplit, cursorContext);
        changesSinceLastCheckpoint.set(true);
        return writer;
    }

    <K, V> OffloadStoreImpl<K, V> buildOffload(Layout<K, V> layout) {
        OffloadIdValidator idValidator = id -> id >= IdSpace.MIN_TREE_NODE_ID && id <= pagedFile.getLastPageId();
        return new OffloadStoreImpl<>(layout, freeList, pagedFile::io, idValidator, payloadSize);
    }

    private static PageCursor openMetaPageCursor(PagedFile pagedFile, int pfFlags, CursorContext cursorContext)
            throws IOException {
        PageCursor metaCursor = pagedFile.io(IdSpace.META_PAGE_ID, pfFlags, cursorContext);
        PageCursorUtil.goTo(metaCursor, "meta page", IdSpace.META_PAGE_ID);
        return metaCursor;
    }

    Meta readMeta(CursorContext cursorContext) throws IOException {
        try (PageCursor metaCursor = openMetaPageCursor(pagedFile, PF_SHARED_READ_LOCK, cursorContext)) {
            return Meta.read(metaCursor);
        }
    }

    static Meta readMeta(PagedFile pagedFile, CursorContext cursorContext) throws IOException {
        try (PageCursor metaCursor = openMetaPageCursor(pagedFile, PF_SHARED_READ_LOCK, cursorContext)) {
            return Meta.read(metaCursor);
        }
    }

    void writeMeta(
            Layout<?, ?> rootLayout,
            Layout<?, ?> dataLayout,
            CursorContext cursorContext,
            TreeNodeSelector treeNodeSelector)
            throws IOException {
        Meta meta = Meta.from(payloadSize, dataLayout, rootLayout, treeNodeSelector);
        try (PageCursor metaCursor = openMetaPageCursor(pagedFile, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext)) {
            meta.write(metaCursor);
        }
    }

    int payloadSize() {
        return payloadSize;
    }

    /**
     * Utility for {@link PagedFile#io(long, int, CursorContext) acquiring} a new {@link PageCursor},
     * placed at the current root id and which have had its {@link PageCursor#next()} called-
     *
     * @param pfFlags flags sent into {@link PagedFile#io(long, int, CursorContext)}.
     * @return {@link PageCursor} result from call to {@link PagedFile#io(long, int, CursorContext)} after it has been
     * placed at the current root and has had {@link PageCursor#next()} called.
     * @throws IOException on {@link PageCursor} error.
     */
    PageCursor openRootCursor(Root root, int pfFlags, CursorContext cursorContext) throws IOException {
        PageCursor cursor = pagedFile.io(0L /*Ignored*/, pfFlags, cursorContext);
        root.goTo(cursor);
        return cursor;
    }

    PageCursor openCursor(int pfFlags, CursorContext cursorContext) throws IOException {
        return pagedFile.io(0L /*Ignored*/, pfFlags, cursorContext);
    }

    long generation() {
        return generationSupplier.getAsLong();
    }

    <K, V> void initializeNewRoot(
            Root root, LeafNodeBehaviour<K, V> leafNode, byte layerType, CursorContext cursorContext)
            throws IOException {
        try (PageCursor cursor = openRootCursor(root, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext)) {
            long generation = generationSupplier.getAsLong();
            long stableGeneration = stableGeneration(generation);
            long unstableGeneration = unstableGeneration(generation);
            leafNode.initialize(cursor, layerType, stableGeneration, unstableGeneration);
            changesSinceLastCheckpoint.set(true);
            checkOutOfBounds(cursor);
        }
    }

    IdProvider idProvider() {
        return freeList;
    }

    <K, V> void unsafe(
            GBPTreeUnsafe<K, V> unsafe,
            Layout<K, V> layout,
            LeafNodeBehaviour<K, V> leafNode,
            InternalNodeBehaviour<K> internalNode,
            CursorContext cursorContext)
            throws IOException {
        TreeState state;
        try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext)) {
            // todo find better way of getting TreeState?
            Pair<TreeState, TreeState> states =
                    TreeStatePair.readStatePages(cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B);
            state = TreeStatePair.selectNewestValidOrFirst(states);
        }
        unsafe.access(pagedFile, layout, leafNode, internalNode, state);
    }

    CrashGenerationCleaner createCrashGenerationCleaner(
            InternalNodeBehaviour<?> rootTreeNode,
            InternalNodeBehaviour<?> dataTreeNode,
            CursorContextFactory contextFactory) {
        long generation = generation();
        long stableGeneration = stableGeneration(generation);
        long unstableGeneration = unstableGeneration(generation);
        long highTreeNodeId = freeList.lastId() + 1;

        return new CrashGenerationCleaner(
                pagedFile,
                rootTreeNode,
                dataTreeNode,
                IdSpace.MIN_TREE_NODE_ID,
                highTreeNodeId,
                stableGeneration,
                unstableGeneration,
                monitor,
                contextFactory,
                treeName);
    }

    TreeNodeLatchService latchService() {
        return latchService;
    }

    <K, V> long estimateNumberOfEntriesInTree(
            Layout<K, V> layout,
            final LeafNodeBehaviour<K, V> leafNode,
            final InternalNodeBehaviour<K> internalNode,
            RootSupplier rootSupplier,
            CursorContext cursorContext)
            throws IOException {
        K low = layout.newKey();
        layout.initializeAsLowest(low);
        K high = layout.newKey();
        layout.initializeAsHighest(high);
        int sampleSize = 100;
        SizeEstimationMonitor monitor = new SizeEstimationMonitor();
        do {
            monitor.clear();
            Seeker.Factory<K, V> monitoredSeeks = new Seeker.Factory<>() {
                @Override
                public Seeker<K, V> allocateSeeker(CursorContext cursorContext) throws IOException {
                    return internalAllocateSeeker(layout, cursorContext, leafNode, internalNode);
                }

                @Override
                public Seeker<K, V> seek(Seeker<K, V> seeker, K fromInclusive, K toExclusive) throws IOException {
                    return initializeSeeker(seeker, rootSupplier, fromInclusive, toExclusive, 1, LEAF_LEVEL, monitor);
                }

                @Override
                public List<K> partitionedSeek(
                        K fromInclusive, K toExclusive, int numberOfPartitions, CursorContext cursorContext)
                        throws IOException {
                    return internalPartitionedSeek(
                            layout,
                            leafNode,
                            internalNode,
                            fromInclusive,
                            toExclusive,
                            numberOfPartitions,
                            rootSupplier,
                            cursorContext);
                }
            };
            List<K> partitionEdges = internalPartitionedSeek(
                    layout, leafNode, internalNode, low, high, sampleSize, rootSupplier, cursorContext);
            for (int i = 0; i < partitionEdges.size() - 1; i++) {
                // Simply make sure the first one is found so that the supplied monitor have been notified about the
                // path down to it
                try (Seeker<K, V> partition =
                        monitoredSeeks.seek(partitionEdges.get(i), partitionEdges.get(i + 1), cursorContext)) {
                    partition.next();
                }
            }
        } while (!monitor.isConsistent());
        return monitor.estimateNumberOfKeys();
    }

    PagedFile pagedFile() {
        return pagedFile;
    }

    StructureWriteLog structureWriteLog() {
        return structureWriteLog;
    }
}
