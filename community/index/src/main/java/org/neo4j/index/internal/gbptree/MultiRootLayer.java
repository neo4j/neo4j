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

import static org.neo4j.index.internal.gbptree.CursorCreator.bind;
import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.InternalTreeLogic.DEFAULT_SPLIT_RATIO;
import static org.neo4j.index.internal.gbptree.SeekCursor.DEFAULT_MAX_READ_AHEAD;
import static org.neo4j.index.internal.gbptree.SeekCursor.LEAF_LEVEL;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.DATA_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.ROOT_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.neo4j.common.DependencyResolver;
import org.neo4j.index.internal.gbptree.RootMappingLayout.RootMappingValue;
import org.neo4j.internal.helpers.collection.LfuCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.util.Preconditions;
import org.neo4j.util.concurrent.Futures;

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
            16 /*obj.overhead*/ + 16 /*obj.fields*/ + 16 /*inner root instance*/ + 8 /* cache references*/;
    private static final long NULL_ROOT_ID = -1;
    private static final long NOT_FOUND_ROOT_ID = 0;
    private static final Root NULL_ROOT = new Root(NULL_ROOT_ID, -1);
    private final Layout<ROOT_KEY, RootMappingValue> rootLayout;
    private final LeafNodeBehaviour<ROOT_KEY, RootMappingValue> rootLeafNode;
    private final InternalNodeBehaviour<ROOT_KEY> rootInternalNode;
    private final LfuCache<ROOT_KEY, DataTreeRoot<ROOT_KEY>> rootMappingCache;
    private final ValueMerger<ROOT_KEY, RootMappingValue> DONT_ALLOW_CREATE_EXISTING_ROOT =
            (existingKey, newKey, existingValue, newValue) -> {
                throw new DataTreeAlreadyExistsException(existingKey);
            };

    private final Layout<DATA_KEY, DATA_VALUE> dataLayout;
    private final LeafNodeBehaviour<DATA_KEY, DATA_VALUE> dataLeafNode;
    private final InternalNodeBehaviour<DATA_KEY> dataInternalNode;

    MultiRootLayer(
            RootLayerSupport support,
            Layout<ROOT_KEY, RootMappingValue> rootLayout,
            Layout<DATA_KEY, DATA_VALUE> dataLayout,
            int rootCacheSizeInBytes,
            TreeNodeSelector treeNodeSelector,
            DependencyResolver dependencyResolver) {
        super(support, treeNodeSelector);
        Preconditions.checkState(
                hashCodeSeemsImplemented(rootLayout), "Root layout doesn't seem to have a hashCode() implementation");

        this.rootLayout = rootLayout;
        this.dataLayout = dataLayout;
        this.rootMappingCache = new LfuCache<>(
                "Root mapping cache", Math.max(100, rootCacheSizeInBytes / BYTE_SIZE_PER_CACHED_EXTERNAL_ROOT));
        var rootMappingFormat = treeNodeSelector.selectByLayout(this.rootLayout);
        var format = treeNodeSelector.selectByLayout(dataLayout);
        OffloadStoreImpl<ROOT_KEY, RootMappingValue> rootOffloadStore = support.buildOffload(this.rootLayout);
        OffloadStoreImpl<DATA_KEY, DATA_VALUE> dataOffloadStore = support.buildOffload(dataLayout);
        this.rootLeafNode = rootMappingFormat.createLeafBehaviour(
                support.payloadSize(), this.rootLayout, rootOffloadStore, dependencyResolver);
        this.rootInternalNode = rootMappingFormat.createInternalBehaviour(
                support.payloadSize(), this.rootLayout, rootOffloadStore, dependencyResolver);
        this.dataLeafNode =
                format.createLeafBehaviour(support.payloadSize(), dataLayout, dataOffloadStore, dependencyResolver);
        this.dataInternalNode =
                format.createInternalBehaviour(support.payloadSize(), dataLayout, dataOffloadStore, dependencyResolver);
    }

    private boolean hashCodeSeemsImplemented(Layout<ROOT_KEY, RootMappingValue> rootLayout) {
        var key1 = rootLayout.newKey();
        var key2 = rootLayout.newKey();
        rootLayout.initializeAsHighest(key1);
        rootLayout.initializeAsHighest(key2);
        return key1.hashCode() == key2.hashCode();
    }

    @Override
    void initializeAfterCreation(Root firstRoot, CursorContext cursorContext) throws IOException {
        setRoot(firstRoot, cursorContext);
        support.writeMeta(rootLayout, dataLayout, cursorContext, treeNodeSelector);
        support.initializeNewRoot(root, rootLeafNode, ROOT_LAYER_FLAG, cursorContext);
    }

    @Override
    void initialize(Root root, CursorContext cursorContext) throws IOException {
        setRoot(root, cursorContext);
        support.readMeta(cursorContext).verify(dataLayout, rootLayout, treeNodeSelector);
    }

    @Override
    void create(ROOT_KEY dataRootKey, CursorContext cursorContext) throws IOException {
        var rootUnderCreation = new DataTreeRoot<ROOT_KEY>(null, NULL_ROOT);
        rootMappingCache.putIfAbsent(dataRootKey, rootUnderCreation);
        try {
            var cursorCreator = bind(support, PF_SHARED_WRITE_LOCK, cursorContext);
            Root dataRoot;
            try (Writer<ROOT_KEY, RootMappingValue> rootMappingWriter = support.internalParallelWriter(
                    rootLayout,
                    rootLeafNode,
                    rootInternalNode,
                    DEFAULT_SPLIT_RATIO,
                    cursorContext,
                    this,
                    ROOT_LAYER_FLAG)) {
                dataRootKey = rootLayout.copyKey(dataRootKey);
                long generation = support.generation();
                long stableGeneration = stableGeneration(generation);
                long unstableGeneration = unstableGeneration(generation);
                long rootId = support.idProvider().acquireNewId(stableGeneration, unstableGeneration, cursorCreator);
                try {
                    dataRoot = new Root(rootId, unstableGeneration);
                    support.initializeNewRoot(dataRoot, dataLeafNode, DATA_LAYER_FLAG, cursorContext);
                    // Write it to the root mapping tree
                    rootMappingWriter.merge(
                            dataRootKey, new RootMappingValue().initialize(dataRoot), DONT_ALLOW_CREATE_EXISTING_ROOT);
                    support.structureWriteLog().createRoot(unstableGeneration, rootId);
                } catch (DataTreeAlreadyExistsException e) {
                    support.idProvider().releaseId(stableGeneration, unstableGeneration, rootId, cursorCreator);
                    throw e;
                }
            }
            // Success! Let's cache it after we close the writer, as long as no one has touched this cache key
            // concurrently!
            rootMappingCache.compute(
                    dataRootKey, (key, curr) -> curr == rootUnderCreation ? new DataTreeRoot<>(key, dataRoot) : curr);
        } catch (Throwable e) {
            // Something went wrong, lets clean up the cache if it's not cleaned already!
            rootMappingCache.compute(dataRootKey, (key, curr) -> curr == rootUnderCreation ? null : curr);
            throw e;
        }
    }

    @Override
    void delete(ROOT_KEY dataRootKey, CursorContext cursorContext) throws IOException {
        try (var rootMappingMerger = new RootDeleteValueMerger(cursorContext, dataRootKey)) {
            try (Writer<ROOT_KEY, RootMappingValue> rootMappingWriter = support.internalParallelWriter(
                    rootLayout,
                    rootLeafNode,
                    rootInternalNode,
                    DEFAULT_SPLIT_RATIO,
                    cursorContext,
                    this,
                    ROOT_LAYER_FLAG)) {
                dataRootKey = rootLayout.copyKey(dataRootKey);
                while (true) {
                    rootMappingMerger.reset();
                    rootMappingWriter.mergeIfExists(
                            dataRootKey, new RootMappingValue().initialize(NULL_ROOT), rootMappingMerger);
                    var rootId = rootMappingMerger.rootIdToRelease;
                    if (rootId == NOT_FOUND_ROOT_ID) {
                        throw new DataTreeNotFoundException(dataRootKey);
                    }
                    if (rootId != NULL_ROOT_ID) {
                        long generation = support.generation();
                        var unstableGeneration = unstableGeneration(generation);
                        support.structureWriteLog().deleteRoot(unstableGeneration, rootId);
                        support.idProvider()
                                .releaseId(
                                        stableGeneration(generation),
                                        unstableGeneration,
                                        rootId,
                                        bind(support, PF_SHARED_WRITE_LOCK, cursorContext));
                        break;
                    } else {
                        rootMappingWriter.yield();
                    }
                }
            }
            rootMappingCache.remove(dataRootKey);
        }
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
                rootLayout,
                rootLeafNode,
                rootInternalNode,
                dataLayout,
                dataLeafNode,
                dataInternalNode,
                stableGeneration(generation),
                unstableGeneration(generation));
        var cursorCreator = bind(support, PF_SHARED_READ_LOCK, cursorContext);
        try (PageCursor cursor = support.openRootCursor(root, PF_SHARED_READ_LOCK, cursorContext)) {
            structure.visitTree(cursor, visitor, cursorContext);
            support.idProvider().visitFreelist(visitor, cursorCreator);
        }

        try (Seeker<ROOT_KEY, RootMappingValue> allRootsSeek = allRootsSeek(cursorContext)) {
            while (allRootsSeek.next()) {
                // Data
                try (PageCursor cursor =
                        support.openRootCursor(allRootsSeek.value().asRoot(), PF_SHARED_READ_LOCK, cursorContext)) {
                    structure.visitTree(cursor, visitor, cursorContext);
                    support.idProvider().visitFreelist(visitor, cursorCreator);
                }
            }
        }
    }

    @Override
    void consistencyCheck(
            GBPTreeConsistencyChecker.ConsistencyCheckState state,
            GBPTreeConsistencyCheckVisitor visitor,
            boolean reportDirty,
            CursorContextFactory contextFactory,
            int numThreads)
            throws IOException {
        // Check the root mapping tree using numThreads
        long generation = support.generation();
        long stableGeneration = stableGeneration(generation);
        long unstableGeneration = unstableGeneration(generation);
        var pagedFile = support.pagedFile();
        var isRootTreeClean = new CleanTrackingConsistencyCheckVisitor(visitor);
        var dataRootCount = new LongAdder();
        new GBPTreeConsistencyChecker<>(
                        rootLeafNode,
                        rootInternalNode,
                        rootLayout,
                        state,
                        numThreads,
                        stableGeneration,
                        unstableGeneration,
                        reportDirty,
                        pagedFile.path(),
                        ctx -> pagedFile.io(0, PF_SHARED_READ_LOCK, ctx),
                        root,
                        contextFactory)
                .check(isRootTreeClean, state.progress, dataRootCount::add);

        if (!isRootTreeClean.isConsistent()) {
            // The root tree has inconsistencies, we essentially cannot trust to read it in order to get
            // to the data trees
            return;
        }

        // Check batches of N data trees in parallel, each individual data tree is checked using a single thread
        var futures = new LinkedList<Future<Void>>();
        var batchSize = 100;
        var batchSizeSwitchThreshold = dataRootCount.sum() * 0.9;
        try (var context = contextFactory.create("allRootsSeek")) {
            var dataTreeRootBatch = new ArrayList<Root>();
            try (var rootSeeker = allRootsSeek(context)) {
                var numBatchesAdded = 0;
                for (long numRootsSeen = 0; rootSeeker.next(); numRootsSeen++) {
                    dataTreeRootBatch.add(rootSeeker.value().asRoot());
                    if (dataTreeRootBatch.size() == batchSize) {
                        futures.add(submitDataTreeRootBatch(
                                dataTreeRootBatch,
                                state,
                                stableGeneration,
                                unstableGeneration,
                                reportDirty,
                                pagedFile,
                                visitor,
                                contextFactory));
                        if (++numBatchesAdded % 100 == 0) {
                            // Check now and then to keep the number of futures down a bit in the list
                            while (!futures.isEmpty() && futures.peekFirst().isDone()) {
                                futures.removeFirst().get();
                            }
                        }
                        // Try to distribute smaller batches towards the end so that we don't end up with a few
                        // threads doing the majority of the work.
                        if (batchSize > 1 && numRootsSeen >= batchSizeSwitchThreshold) {
                            batchSize = 1;
                        }
                    }
                }
                if (!dataTreeRootBatch.isEmpty()) {
                    futures.add(submitDataTreeRootBatch(
                            dataTreeRootBatch,
                            state,
                            stableGeneration,
                            unstableGeneration,
                            reportDirty,
                            pagedFile,
                            visitor,
                            contextFactory));
                }
            }
            Futures.getAll(futures);
        } catch (ExecutionException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    private Future<Void> submitDataTreeRootBatch(
            List<Root> dataTreeRootBatch,
            GBPTreeConsistencyChecker.ConsistencyCheckState state,
            long stableGeneration,
            long unstableGeneration,
            boolean reportDirty,
            PagedFile pagedFile,
            GBPTreeConsistencyCheckVisitor visitor,
            CursorContextFactory contextFactory) {
        var batch = dataTreeRootBatch.toArray(new Root[0]);
        dataTreeRootBatch.clear();
        return state.executor.submit(() -> {
            var low = dataLayout.newKey();
            var high = dataLayout.newKey();
            dataLayout.initializeAsLowest(low);
            dataLayout.initializeAsHighest(high);
            // Temporarily disable concurrency as there is a bug causing the checker to hang
            try (var partitionProgress = state.progress.threadLocalReporter();
            /*var seeker = support.internalAllocateSeeker(dataLayout, dataTreeNode, CursorContext.NULL_CONTEXT)*/ ) {
                for (var root : batch) {
                    // int treeDepth = depthOf(root, seeker, low, high);
                    new GBPTreeConsistencyChecker<>(
                                    dataLeafNode,
                                    dataInternalNode,
                                    dataLayout,
                                    state,
                                    /*treeDepth >= 2 ? state.numThreads : 1*/ 1,
                                    stableGeneration,
                                    unstableGeneration,
                                    reportDirty,
                                    pagedFile.path(),
                                    ctx -> pagedFile.io(0, PF_SHARED_READ_LOCK, ctx),
                                    root,
                                    contextFactory)
                            .check(visitor, partitionProgress, GBPTreeConsistencyChecker.NO_MONITOR);
                }
            }
            return null;
        });
    }

    /**
     * @return depth of the tree or -1 if it cannot be decided due to tree being inconsistent.
     */
    private int depthOf(Root root, SeekCursor<DATA_KEY, DATA_VALUE> seeker, DATA_KEY low, DATA_KEY high)
            throws IOException {
        try {
            var depthMonitor = new SeekDepthMonitor();
            support.initializeSeeker(seeker, c -> root, low, high, 1, LEAF_LEVEL, depthMonitor);
            return depthMonitor.treeDepth;
        } catch (TreeInconsistencyException e) {
            return -1;
        }
    }

    private Seeker<ROOT_KEY, RootMappingValue> allRootsSeek(CursorContext cursorContext) throws IOException {
        ROOT_KEY low = rootLayout.newKey();
        ROOT_KEY high = rootLayout.newKey();
        rootLayout.initializeAsLowest(low);
        rootLayout.initializeAsHighest(high);
        return support.initializeSeeker(
                support.internalAllocateSeeker(rootLayout, cursorContext, rootLeafNode, rootInternalNode),
                this,
                low,
                high,
                DEFAULT_MAX_READ_AHEAD,
                LEAF_LEVEL,
                SeekCursor.NO_MONITOR);
    }

    @Override
    int keyValueSizeCap() {
        return dataLeafNode.keyValueSizeCap();
    }

    @Override
    int inlineKeyValueSizeCap() {
        return dataLeafNode.inlineKeyValueSizeCap();
    }

    @Override
    int leafNodeMaxKeys() {
        return dataLeafNode.maxKeyCount();
    }

    @Override
    void unsafe(GBPTreeUnsafe unsafe, boolean dataTree, CursorContext cursorContext) throws IOException {
        if (dataTree) {
            support.unsafe(unsafe, dataLayout, dataLeafNode, dataInternalNode, cursorContext);
        } else {
            support.unsafe(unsafe, rootLayout, rootLeafNode, rootInternalNode, cursorContext);
        }
    }

    @Override
    CrashGenerationCleaner createCrashGenerationCleaner(CursorContextFactory contextFactory) {
        return support.createCrashGenerationCleaner(rootInternalNode, dataInternalNode, contextFactory);
    }

    @Override
    void printNode(PageCursor cursor, CursorContext cursorContext) {
        try {
            long generation = support.generation();
            long stableGeneration = stableGeneration(generation);
            long unstableGeneration = unstableGeneration(generation);
            boolean isDataNode = TreeNodeUtil.layerType(cursor) == DATA_LAYER_FLAG;
            if (isDataNode) {
                new GBPTreeStructure<>(
                                null,
                                null,
                                null,
                                dataLayout,
                                dataLeafNode,
                                dataInternalNode,
                                stableGeneration,
                                unstableGeneration)
                        .visitTreeNode(cursor, new PrintingGBPTreeVisitor<>(PrintConfig.defaults()), cursorContext);
            } else {
                new GBPTreeStructure<>(
                                rootLayout,
                                rootLeafNode,
                                rootInternalNode,
                                null,
                                null,
                                null,
                                stableGeneration,
                                unstableGeneration)
                        .visitTreeNode(cursor, new PrintingGBPTreeVisitor<>(PrintConfig.defaults()), cursorContext);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    void visitAllDataTreeRoots(CursorContext cursorContext, TreeRootsVisitor<ROOT_KEY> visitor) throws IOException {
        try (Seeker<ROOT_KEY, RootMappingValue> seek = allRootsSeek(cursorContext)) {
            while (seek.next()) {
                if (visitor.accept(rootLayout.copyKey(seek.key()))) {
                    break;
                }
            }
        }
    }

    private class RootMappingInteraction implements TreeRootExchange {
        private final ROOT_KEY dataRootKey;

        RootMappingInteraction(ROOT_KEY dataRootKey) {
            this.dataRootKey = dataRootKey;
        }

        @Override
        public Root getRoot(CursorContext context) {
            var dataRoot = rootMappingCache.computeIfAbsent(
                    dataRootKey,
                    key -> getFromTree(key, context, () -> {
                        throw new DataTreeNotFoundException(dataRootKey);
                    }));
            if (dataRoot.root() == NULL_ROOT) {
                // Let's read from the tree to see if there is something here and not just a stale cache value!
                return getFromTree(dataRootKey, context, () -> {
                            throw new DataTreeNotFoundException(dataRootKey);
                        })
                        .root();
            }
            return dataRoot.root();
        }

        private DataTreeRoot<ROOT_KEY> getFromTree(
                ROOT_KEY key, CursorContext context, Supplier<DataTreeRoot<ROOT_KEY>> orElse) {
            try (Seeker<ROOT_KEY, RootMappingValue> seek = support.initializeSeeker(
                    support.internalAllocateSeeker(rootLayout, context, rootLeafNode, rootInternalNode),
                    c -> root,
                    key,
                    key,
                    DEFAULT_MAX_READ_AHEAD,
                    LEAF_LEVEL,
                    SeekCursor.NO_MONITOR)) {
                if (!seek.next()) {
                    return orElse.get();
                }
                return new DataTreeRoot<>(key, seek.value().asRoot());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        boolean exists(CursorContext context) {
            return rootMappingCache.computeIfAbsent(dataRootKey, key -> getFromTree(key, context, () -> null)) != null;
        }

        @Override
        public void setRoot(Root newRoot, CursorContext context) {
            rootMappingCache.compute(dataRootKey, (key, ignored) -> {
                try (Writer<ROOT_KEY, RootMappingValue> rootMappingWriter = support.internalParallelWriter(
                        rootLayout,
                        rootLeafNode,
                        rootInternalNode,
                        DEFAULT_SPLIT_RATIO,
                        context,
                        MultiRootLayer.this,
                        DATA_LAYER_FLAG)) {
                    TrackingValueMerger<ROOT_KEY, RootMappingValue> merger = new TrackingValueMerger<>(overwrite());
                    rootMappingWriter.mergeIfExists(key, new RootMappingValue().initialize(newRoot), merger);
                    if (!merger.wasMerged()) {
                        throw new DataTreeNotFoundException(key);
                    }
                    return new DataTreeRoot<>(key, newRoot);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
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
                    dataLeafNode,
                    dataInternalNode,
                    splitRatio(flags),
                    cursorContext,
                    rootMappingInteraction,
                    DATA_LAYER_FLAG);
        }

        @Override
        public Seeker<DATA_KEY, DATA_VALUE> allocateSeeker(CursorContext cursorContext) throws IOException {
            return support.internalAllocateSeeker(dataLayout, cursorContext, dataLeafNode, dataInternalNode);
        }

        @Override
        public Seeker<DATA_KEY, DATA_VALUE> seek(
                Seeker<DATA_KEY, DATA_VALUE> seeker, DATA_KEY fromInclusive, DATA_KEY toExclusive) throws IOException {
            return support.initializeSeeker(
                    seeker,
                    rootMappingInteraction,
                    fromInclusive,
                    toExclusive,
                    DEFAULT_MAX_READ_AHEAD,
                    LEAF_LEVEL,
                    SeekCursor.NO_MONITOR);
        }

        @Override
        public List<DATA_KEY> partitionedSeek(
                DATA_KEY fromInclusive, DATA_KEY toExclusive, int numberOfPartitions, CursorContext cursorContext)
                throws IOException {
            return support.internalPartitionedSeek(
                    dataLayout,
                    dataLeafNode,
                    dataInternalNode,
                    fromInclusive,
                    toExclusive,
                    numberOfPartitions,
                    rootMappingInteraction,
                    cursorContext);
        }

        @Override
        public long estimateNumberOfEntriesInTree(CursorContext cursorContext) throws IOException {
            return support.estimateNumberOfEntriesInTree(
                    dataLayout, dataLeafNode, dataInternalNode, rootMappingInteraction, cursorContext);
        }

        @Override
        public boolean exists(CursorContext cursorContext) {
            return rootMappingInteraction.exists(cursorContext);
        }
    }

    private record DataTreeRoot<DATA_ROOT_KEY>(DATA_ROOT_KEY key, Root root) {}

    private class RootDeleteValueMerger implements ValueMerger<ROOT_KEY, RootMappingValue>, AutoCloseable {
        private final CursorContext cursorContext;
        private final ROOT_KEY dataRootKey;
        private long rootIdToRelease;
        private LongSpinLatch rootLatch;
        private boolean hasWriteLatch;

        public RootDeleteValueMerger(CursorContext cursorContext, ROOT_KEY dataRootKey) {
            this.cursorContext = cursorContext;
            this.dataRootKey = dataRootKey;
        }

        @Override
        public MergeResult merge(
                ROOT_KEY existingKey, ROOT_KEY newKey, RootMappingValue existingValue, RootMappingValue newValue) {
            // Here we have the latch on the root mapping and want to acquire a latch on the data root
            // There could be another writer having the latch on the data root, and as part of
            // split/shrink/successor,
            // wants to setRoot which means that it wants to acquire the latch on the root mapping ->
            // deadlock

            rootLatch = support.latchService().latch(existingValue.rootId);
            if (!rootLatch.tryAcquireWrite()) {
                // Someone else is just now writing to the contents of this data tree.
                // Back out and try again
                rootIdToRelease = NULL_ROOT_ID;
                return MergeResult.UNCHANGED;
            }
            hasWriteLatch = true;
            try (PageCursor cursor =
                    support.openRootCursor(existingValue.asRoot(), PF_SHARED_WRITE_LOCK, cursorContext)) {
                if (TreeNodeUtil.keyCount(cursor) != 0) {
                    throw new DataTreeNotEmptyException(dataRootKey);
                }
                rootIdToRelease = existingValue.rootId;
                return MergeResult.REMOVED;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void reset() {
            rootIdToRelease = NOT_FOUND_ROOT_ID;
            closeLatch();
        }

        private void closeLatch() {
            if (rootLatch != null) {
                try {
                    if (hasWriteLatch) {
                        rootLatch.releaseWrite();
                        hasWriteLatch = false;
                    }
                } finally {
                    rootLatch.deref();
                    rootLatch = null;
                }
            }
        }

        @Override
        public void close() {
            closeLatch();
        }
    }
}
