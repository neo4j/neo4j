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

import static java.lang.Integer.max;
import static java.lang.Math.abs;
import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.InternalTreeLogic.DEFAULT_SPLIT_RATIO;
import static org.neo4j.index.internal.gbptree.SeekCursor.DEFAULT_MAX_READ_AHEAD;
import static org.neo4j.index.internal.gbptree.SeekCursor.LEAF_LEVEL;
import static org.neo4j.index.internal.gbptree.TreeNode.DATA_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.TreeNode.ROOT_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.index.internal.gbptree.RootMappingLayout.RootMappingValue;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.util.Preconditions;

/**
 * A {@link RootLayer} that has support for multiple data trees, instead of the otherwise normal scenario of having a single data tree.
 * The global root is used to keep mappings from user-defined root key to the actual data tree root ID.
 * The data trees has the provided data layout of its entries. The root layer uses the provided root key layout and an internal "root mapping value".
 *
 * @param <ROOT_KEY> keys that make up the root mappings to the data roots.
 * @param <DATA_KEY> keys used in the data entries in the data roots.
 * @param <DATA_VALUE> values used in the data entries in the data roots.
 */
class MultiRootLayer<ROOT_KEY, DATA_KEY, DATA_VALUE> extends RootLayer<ROOT_KEY, DATA_KEY, DATA_VALUE> {
    private static final int BYTE_SIZE_PER_CACHED_EXTERNAL_ROOT =
            16 /*obj.overhead*/ + 16 /*obj.fields*/ + 16 /*inner root instance*/;

    private final RootLayerSupport support;
    private final CursorContextFactory contextFactory;
    private final Layout<ROOT_KEY, RootMappingValue> rootLayout;
    private final TreeNode<ROOT_KEY, RootMappingValue> singleRootTreeNode;
    private final OffloadStoreImpl<ROOT_KEY, RootMappingValue> rootOffloadStore;
    private final Supplier<TreeNode<ROOT_KEY, RootMappingValue>> rootTreeNodeFactory;
    private final AtomicReferenceArray<DataTreeRoot<ROOT_KEY>> rootMappingCache;
    private final TreeNodeLatchService rootMappingCacheLatches = new TreeNodeLatchService();
    private final ValueMerger<ROOT_KEY, RootMappingValue> DONT_ALLOW_CREATE_EXISTING_ROOT =
            (existingKey, newKey, existingValue, newValue) -> {
                throw new DataTreeAlreadyExistsException(existingKey);
            };

    private final Layout<DATA_KEY, DATA_VALUE> dataLayout;
    private final TreeNode<DATA_KEY, DATA_VALUE> singleDataTreeNode;
    private final OffloadStoreImpl<DATA_KEY, DATA_VALUE> dataOffloadStore;
    private final Supplier<TreeNode<DATA_KEY, DATA_VALUE>> dataTreeNodeFactory;

    private volatile Root root;

    MultiRootLayer(
            RootLayerSupport support,
            Layout<ROOT_KEY, RootMappingValue> rootLayout,
            Layout<DATA_KEY, DATA_VALUE> dataLayout,
            int rootCacheSizeInBytes,
            boolean created,
            CursorContext cursorContext,
            CursorContextFactory contextFactory)
            throws IOException {
        Preconditions.checkState(
                hashCodeSeemsImplemented(rootLayout), "Root layout doesn't seem to have a hashCode() implementation");

        this.support = support;
        this.rootLayout = rootLayout;
        this.dataLayout = dataLayout;
        this.contextFactory = contextFactory;
        int numCachedRoots = rootCacheSizeInBytes / BYTE_SIZE_PER_CACHED_EXTERNAL_ROOT;
        this.rootMappingCache = new AtomicReferenceArray<>(max(numCachedRoots, 10));

        if (created) {
            support.writeMeta(this.rootLayout, dataLayout, cursorContext);
        } else {
            support.readMeta(cursorContext).verify(dataLayout, this.rootLayout);
        }

        TreeNodeSelector.Factory rootMappingFormat = TreeNodeSelector.selectByLayout(this.rootLayout);
        TreeNodeSelector.Factory format = TreeNodeSelector.selectByLayout(dataLayout);
        this.rootOffloadStore = support.buildOffload(this.rootLayout);
        this.dataOffloadStore = support.buildOffload(dataLayout);
        this.rootTreeNodeFactory =
                () -> rootMappingFormat.create(support.payloadSize(), this.rootLayout, rootOffloadStore);
        this.dataTreeNodeFactory = () -> format.create(support.payloadSize(), dataLayout, dataOffloadStore);
        this.singleRootTreeNode = rootTreeNodeFactory.get();
        this.singleDataTreeNode = dataTreeNodeFactory.get();
    }

    private boolean hashCodeSeemsImplemented(Layout<ROOT_KEY, RootMappingValue> rootLayout) {
        var key1 = rootLayout.newKey();
        var key2 = rootLayout.newKey();
        rootLayout.initializeAsHighest(key1);
        rootLayout.initializeAsHighest(key2);
        return key1.hashCode() == key2.hashCode();
    }

    @Override
    public void setRoot(Root root) throws IOException {
        this.root = root;
    }

    @Override
    public Root getRoot() {
        return root;
    }

    @Override
    void initializeAfterCreation(CursorContext cursorContext) throws IOException {
        support.initializeNewRoot(root, singleRootTreeNode, ROOT_LAYER_FLAG, cursorContext);
    }

    @Override
    void create(ROOT_KEY dataRootKey, CursorContext cursorContext) throws IOException {
        dataRootKey = rootLayout.copyKey(dataRootKey);
        try (Writer<ROOT_KEY, RootMappingValue> rootMappingWriter = support.internalParallelWriter(
                rootLayout, rootTreeNodeFactory, DEFAULT_SPLIT_RATIO, cursorContext, this, ROOT_LAYER_FLAG)) {
            long generation = support.generation();
            long stableGeneration = stableGeneration(generation);
            long unstableGeneration = unstableGeneration(generation);
            long rootId = support.idProvider().acquireNewId(stableGeneration, unstableGeneration, cursorContext);
            Root dataRoot = new Root(rootId, unstableGeneration);
            support.initializeNewRoot(dataRoot, singleDataTreeNode, DATA_LAYER_FLAG, cursorContext);
            try {
                // Write it to the root mapping tree
                rootMappingWriter.merge(
                        dataRootKey, new RootMappingValue().initialize(dataRoot), DONT_ALLOW_CREATE_EXISTING_ROOT);
                // Cache the created root
                cache(new DataTreeRoot<>(dataRootKey, dataRoot));
            } catch (DataTreeAlreadyExistsException e) {
                support.idProvider().releaseId(stableGeneration, unstableGeneration, rootId, cursorContext);
                throw e;
            }
        }
    }

    @Override
    void delete(ROOT_KEY dataRootKey, CursorContext cursorContext) throws IOException {
        int cacheIndex = cacheIndex(dataRootKey);
        try (Writer<ROOT_KEY, RootMappingValue> rootMappingWriter = support.internalParallelWriter(
                rootLayout, rootTreeNodeFactory, DEFAULT_SPLIT_RATIO, cursorContext, this, ROOT_LAYER_FLAG)) {
            while (true) {
                MutableLong rootIdToRelease = new MutableLong();
                ValueMerger<ROOT_KEY, RootMappingValue> rootMappingMerger =
                        (existingKey, newKey, existingValue, newValue) -> {
                            // Here we have the latch on the root mapping and want to acquire a latch on the data root
                            // There could be another writer having the latch on the data root, and as part of
                            // split/shrink/successor,
                            // wants to setRoot which means that it wants to acquire the latch on the root mapping ->
                            // deadlock

                            LongSpinLatch rootLatch = support.latchService().tryAcquireWrite(existingValue.rootId);
                            if (rootLatch == null) {
                                // Someone else is just now writing to the contents of this data tree. Back out and try
                                // again
                                rootIdToRelease.setValue(-1);
                                return ValueMerger.MergeResult.UNCHANGED;
                            }
                            try (PageCursor cursor = support.openRootCursor(
                                    existingValue.asRoot(), PF_SHARED_WRITE_LOCK, cursorContext)) {
                                if (TreeNode.keyCount(cursor) != 0) {
                                    throw new DataTreeNotEmptyException(dataRootKey);
                                }
                                rootIdToRelease.setValue(existingValue.rootId);

                                // Remove it from the cache if it's present
                                DataTreeRoot<ROOT_KEY> cachedRoot = rootMappingCache.get(cacheIndex);
                                if (cachedRoot != null && rootLayout.compare(cachedRoot.key, dataRootKey) == 0) {
                                    rootMappingCache.compareAndSet(cacheIndex, cachedRoot, null);
                                }

                                return ValueMerger.MergeResult.REMOVED;
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            } finally {
                                rootLatch.releaseWrite();
                            }
                        };

                rootMappingWriter.mergeIfExists(
                        dataRootKey, new RootMappingValue().initialize(new Root(-1, -1)), rootMappingMerger);
                if (rootIdToRelease.longValue() == 0) {
                    throw new DataTreeNotFoundException(dataRootKey);
                }
                if (rootIdToRelease.longValue() != -1) {
                    long generation = support.generation();
                    support.idProvider()
                            .releaseId(
                                    stableGeneration(generation),
                                    unstableGeneration(generation),
                                    rootIdToRelease.longValue(),
                                    cursorContext);
                    break;
                }
            }
        }
    }

    private static long cacheIndexAsTreeNodeId(int cacheIndex) {
        return (cacheIndex & 0xFFFFFFFFL) + 1;
    }

    @Override
    DataTree<DATA_KEY, DATA_VALUE> access(ROOT_KEY dataRootKey) {
        return new MultiDataTree(dataRootKey);
    }

    @Override
    void visit(GBPTreeVisitor visitor, CursorContext cursorContext) throws IOException {
        // Root mappings
        long generation = support.generation();
        var structure = new GBPTreeStructure<>(
                singleRootTreeNode,
                rootLayout,
                singleDataTreeNode,
                dataLayout,
                stableGeneration(generation),
                unstableGeneration(generation));
        try (PageCursor cursor = support.openRootCursor(root, PF_SHARED_READ_LOCK, cursorContext)) {
            structure.visitTree(cursor, visitor, cursorContext);
            support.idProvider().visitFreelist(visitor, cursorContext);
        }

        try (Seeker<ROOT_KEY, RootMappingValue> allRootsSeek = allRootsSeek(cursorContext)) {
            while (allRootsSeek.next()) {
                // Data
                try (PageCursor cursor =
                        support.openRootCursor(allRootsSeek.value().asRoot(), PF_SHARED_READ_LOCK, cursorContext)) {
                    structure.visitTree(cursor, visitor, cursorContext);
                    support.idProvider().visitFreelist(visitor, cursorContext);
                }
            }
        }
    }

    @Override
    void consistencyCheck(
            GBPTreeConsistencyChecker.ConsistencyCheckState state,
            GBPTreeConsistencyCheckVisitor visitor,
            boolean reportDirty,
            PageCursor cursor,
            CursorContext cursorContext,
            Path file)
            throws IOException {
        // Check the root mapping tree
        long generation = support.generation();
        long stableGeneration = stableGeneration(generation);
        long unstableGeneration = unstableGeneration(generation);
        new GBPTreeConsistencyChecker<>(
                        singleRootTreeNode, rootLayout, state, stableGeneration, unstableGeneration, reportDirty)
                .check(file, cursor, root, visitor, cursorContext);

        // Check each root
        try (Seeker<ROOT_KEY, RootMappingValue> seek = allRootsSeek(cursorContext)) {
            while (seek.next()) {
                Root dataRoot = seek.value().asRoot();
                new GBPTreeConsistencyChecker<>(
                                singleDataTreeNode,
                                dataLayout,
                                state,
                                stableGeneration,
                                unstableGeneration,
                                reportDirty)
                        .check(file, cursor, dataRoot, visitor, cursorContext);
            }
        }
    }

    private Seeker<ROOT_KEY, RootMappingValue> allRootsSeek(CursorContext cursorContext) throws IOException {
        ROOT_KEY low = rootLayout.newKey();
        ROOT_KEY high = rootLayout.newKey();
        rootLayout.initializeAsLowest(low);
        rootLayout.initializeAsHighest(high);
        return support.initializeSeeker(
                support.internalAllocateSeeker(rootLayout, singleRootTreeNode, cursorContext, SeekCursor.NO_MONITOR),
                this,
                low,
                high,
                DEFAULT_MAX_READ_AHEAD,
                LEAF_LEVEL);
    }

    @Override
    int keyValueSizeCap() {
        return singleDataTreeNode.keyValueSizeCap();
    }

    @Override
    int inlineKeyValueSizeCap() {
        return singleDataTreeNode.inlineKeyValueSizeCap();
    }

    @Override
    void unsafe(GBPTreeUnsafe unsafe, boolean dataTree, CursorContext cursorContext) throws IOException {
        if (dataTree) {
            support.unsafe(unsafe, dataLayout, singleDataTreeNode, cursorContext);
        } else {
            support.unsafe(unsafe, rootLayout, singleRootTreeNode, cursorContext);
        }
    }

    @Override
    CrashGenerationCleaner createCrashGenerationCleaner(CursorContextFactory contextFactory) {
        return support.createCrashGenerationCleaner(singleRootTreeNode, singleDataTreeNode, contextFactory);
    }

    @Override
    void printNode(PageCursor cursor, CursorContext cursorContext) {
        byte layerType = TreeNode.layerType(cursor);
        var treeNode = layerType == DATA_LAYER_FLAG ? singleDataTreeNode : singleRootTreeNode;
        long generation = support.generation();
        treeNode.printNode(
                cursor, false, true, stableGeneration(generation), unstableGeneration(generation), cursorContext);
    }

    private void cache(DataTreeRoot<ROOT_KEY> dataRoot) {
        rootMappingCache.set(cacheIndex(dataRoot.key), dataRoot);
    }

    private int cacheIndex(ROOT_KEY dataRootKey) {
        return abs(dataRootKey.hashCode()) % rootMappingCache.length();
    }

    void visitAllDataTreeRoots(Consumer<ROOT_KEY> visitor, CursorContext cursorContext) throws IOException {
        try (Seeker<ROOT_KEY, RootMappingValue> seek = allRootsSeek(cursorContext)) {
            while (seek.next()) {
                visitor.accept(rootLayout.copyKey(seek.key()));
            }
        }
    }

    private class RootMappingInteraction implements TreeRootExchange {
        private final ROOT_KEY dataRootKey;
        private final int cacheIndex;

        RootMappingInteraction(ROOT_KEY dataRootKey) {
            this.dataRootKey = dataRootKey;
            this.cacheIndex = cacheIndex(dataRootKey);
        }

        @Override
        public Root getRoot() {
            DataTreeRoot<ROOT_KEY> dataRoot = rootMappingCache.get(cacheIndex);
            if (dataRoot != null && rootLayout.compare(dataRoot.key, dataRootKey) == 0) {
                return dataRoot.root;
            }

            // Acquire a read latch for this cache slot, which will act as a guard for this scenario:
            // - Existing root R1
            // - Reader (we) searches and finds R1
            // - A new root for this data tree is set to R2 and placed in the cache
            // - Another root enters, with the same cache index, and places its root on this slot
            // - Reader (we) put the old R1 into cache
            LongSpinLatch rootMappingLatch = rootMappingCacheLatches.acquireRead(cacheIndexAsTreeNodeId(cacheIndex));
            try (CursorContext cursorContext = contextFactory.create("Update root mapping");
                    Seeker<ROOT_KEY, RootMappingValue> seek = support.initializeSeeker(
                            support.internalAllocateSeeker(
                                    rootLayout, singleRootTreeNode, cursorContext, SeekCursor.NO_MONITOR),
                            () -> root,
                            dataRootKey,
                            dataRootKey,
                            DEFAULT_MAX_READ_AHEAD,
                            LEAF_LEVEL)) {
                if (seek.next()) {
                    Root root = seek.value().asRoot();
                    cacheReadRoot(root);
                    return root;
                }
                throw new DataTreeNotFoundException(dataRootKey);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                rootMappingLatch.releaseRead();
            }
        }

        private void cacheReadRoot(Root root) {
            DataTreeRoot<ROOT_KEY> from;
            DataTreeRoot<ROOT_KEY> to = new DataTreeRoot<>(dataRootKey, root);
            do {
                from = rootMappingCache.get(cacheIndex);
                if (from != null && rootLayout.compare(from.key, dataRootKey) == 0) {
                    // If there's already a cached entry for this key then don't update it - it's up to the writer to do
                    // this,
                    // otherwise this "lookup" will race with a writer changing the root ID of this data root -
                    // potentially
                    // overwriting that cache slot with the old root
                    break;
                }
            } while (!rootMappingCache.compareAndSet(cacheIndex, from, to));
        }

        @Override
        public void setRoot(Root newRoot) throws IOException {
            LongSpinLatch rootMappingLatch = rootMappingCacheLatches.acquireWrite(cacheIndexAsTreeNodeId(cacheIndex));
            try (CursorContext cursorContext = contextFactory.create("Update root mapping");
                    Writer<ROOT_KEY, RootMappingValue> rootMappingWriter = support.internalParallelWriter(
                            rootLayout,
                            rootTreeNodeFactory,
                            DEFAULT_SPLIT_RATIO,
                            cursorContext,
                            MultiRootLayer.this,
                            DATA_LAYER_FLAG)) {
                cache(new DataTreeRoot<>(dataRootKey, newRoot));
                TrackingValueMerger<ROOT_KEY, RootMappingValue> merger = new TrackingValueMerger<>(overwrite());
                rootMappingWriter.mergeIfExists(dataRootKey, new RootMappingValue().initialize(newRoot), merger);
                if (!merger.wasMerged()) {
                    throw new DataTreeNotFoundException(dataRootKey);
                }
            } finally {
                rootMappingLatch.releaseWrite();
            }
        }
    }

    private class MultiDataTree implements DataTree<DATA_KEY, DATA_VALUE> {
        private final RootMappingInteraction rootMappingInteraction;

        MultiDataTree(ROOT_KEY dataRootKey) {
            this.rootMappingInteraction = new RootMappingInteraction(dataRootKey);
        }

        @Override
        public Writer<DATA_KEY, DATA_VALUE> writer(int flags, CursorContext cursorContext) throws IOException {
            return support.internalParallelWriter(
                    dataLayout,
                    dataTreeNodeFactory,
                    splitRatio(flags),
                    cursorContext,
                    rootMappingInteraction,
                    DATA_LAYER_FLAG);
        }

        @Override
        public Seeker<DATA_KEY, DATA_VALUE> allocateSeeker(CursorContext cursorContext) throws IOException {
            return support.internalAllocateSeeker(dataLayout, singleDataTreeNode, cursorContext, SeekCursor.NO_MONITOR);
        }

        @Override
        public Seeker<DATA_KEY, DATA_VALUE> seek(
                Seeker<DATA_KEY, DATA_VALUE> seeker, DATA_KEY fromInclusive, DATA_KEY toExclusive) throws IOException {
            return support.initializeSeeker(
                    seeker, rootMappingInteraction, fromInclusive, toExclusive, DEFAULT_MAX_READ_AHEAD, LEAF_LEVEL);
        }

        @Override
        public List<DATA_KEY> partitionedSeek(
                DATA_KEY fromInclusive, DATA_KEY toExclusive, int numberOfPartitions, CursorContext cursorContext)
                throws IOException {
            return support.internalPartitionedSeek(
                    dataLayout,
                    singleDataTreeNode,
                    fromInclusive,
                    toExclusive,
                    numberOfPartitions,
                    rootMappingInteraction,
                    cursorContext);
        }

        @Override
        public long estimateNumberOfEntriesInTree(CursorContext cursorContext) throws IOException {
            return support.estimateNumberOfEntriesInTree(
                    dataLayout, singleDataTreeNode, rootMappingInteraction, cursorContext);
        }
    }

    private record DataTreeRoot<DATA_ROOT_KEY>(DATA_ROOT_KEY key, Root root) {}
}
