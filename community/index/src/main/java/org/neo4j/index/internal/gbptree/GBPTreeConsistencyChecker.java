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

import static java.lang.Math.toIntExact;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.IdSpace.MIN_TREE_NODE_ID;
import static org.neo4j.index.internal.gbptree.PointerChecking.checkOutOfBounds;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.NO_OFFLOAD_ID;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.goTo;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.isNode;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.NamedThreadFactory;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.util.concurrent.Futures;

/**
 * <ul>
 * Checks:
 * <li>order of keys in isolated nodes
 * <li>keys fit inside range given by parent node
 * <li>sibling pointers match
 * <li>GSPP
 * </ul>
 */
class GBPTreeConsistencyChecker<KEY> {
    private static final String TAG_CHECK = "check gbptree consistency";

    private final LeafNodeBehaviour<KEY, ?> leafNode;
    private final InternalNodeBehaviour<KEY> internalNode;
    private final Comparator<KEY> comparator;
    private final Layout<KEY, ?> layout;
    private final ConsistencyCheckState state;
    private final long stableGeneration;
    private final long unstableGeneration;
    private final boolean reportDirty;
    private final Path file;
    private final ThrowingFunction<CursorContext, PageCursor, IOException> cursorFactory;
    private final Root root;
    private final CursorContextFactory contextFactory;
    private final int numThreads;

    GBPTreeConsistencyChecker(
            LeafNodeBehaviour<KEY, ?> leafNode,
            InternalNodeBehaviour<KEY> internalNode,
            Layout<KEY, ?> layout,
            ConsistencyCheckState state,
            int numThreads,
            long stableGeneration,
            long unstableGeneration,
            boolean reportDirty,
            Path file,
            ThrowingFunction<CursorContext, PageCursor, IOException> cursorFactory,
            Root root,
            CursorContextFactory contextFactory) {
        this.leafNode = leafNode;
        this.internalNode = internalNode;
        this.comparator = layout;
        this.layout = layout;
        this.state = state;
        this.numThreads = numThreads;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
        this.reportDirty = reportDirty;
        this.file = file;
        this.cursorFactory = cursorFactory;
        this.root = root;
        this.contextFactory = contextFactory;
    }

    /**
     * Checks so that all pages between {@link IdSpace#MIN_TREE_NODE_ID} and highest allocated id
     * are either in use in the tree, on the free-list or free-list nodes, and that their interlinks are correct.
     *
     * NOTE that not all task are necessarily finished when this one returns, some can still be running in the
     * executor. Use state.awaitAllSubtasks to wait them out when ready.
     * Not done here to avoid wasting a thread on just waiting.
     *
     * @param visitor {@link GBPTreeConsistencyCheckVisitor} visitor to report inconsistencies to.
     * @throws IOException on {@link PageCursor} error.
     */
    void check(GBPTreeConsistencyCheckVisitor visitor, ProgressListener progress, Monitor monitor) throws IOException {
        try (var context = contextFactory.create(TAG_CHECK);
                var cursor = cursorFactory.apply(context)) {
            long rootGeneration = root.goTo(cursor);
            KeyRange<KEY> openRange = new KeyRange<>(-1, -1, comparator, null, null, layout, null);
            var rightmostPerLevel = new RightmostInChainShard(file, true);
            checkSubtree(
                    cursor,
                    openRange,
                    -1,
                    rootGeneration,
                    GBPTreePointerType.noPointer(),
                    0,
                    visitor,
                    state.threadLocalSeenIds(),
                    context,
                    rightmostPerLevel,
                    progress,
                    monitor,
                    () -> rightmostPerLevel.assertLast(visitor));
        }
    }

    private static void addToSeenList(
            Path file, BitSet target, long id, long lastId, GBPTreeConsistencyCheckVisitor visitor) {
        int index = toIntExact(id);
        if (target.get(index)) {
            visitor.pageIdSeenMultipleTimes(id, file);
        }
        if (id > lastId) {
            visitor.pageIdExceedLastId(lastId, id, file);
        }
        target.set(index);
    }

    private void checkSubtree(
            PageCursor cursor,
            KeyRange<KEY> range,
            long parentNode,
            long pointerGeneration,
            GBPTreePointerType parentPointerType,
            int level,
            GBPTreeConsistencyCheckVisitor visitor,
            BitSet seenIds,
            CursorContext cursorContext,
            RightmostInChainShard rightmostPerLevel,
            ProgressListener progress,
            Monitor monitor,
            Runnable taskToRunAsLastCheck)
            throws IOException {
        long pageId = cursor.getCurrentPageId();
        addToSeenList(file, seenIds, pageId, state.lastId, visitor);
        progress.add(1);
        if (range.hasPageIdInStack(pageId)) {
            visitor.childNodeFoundAmongParentNodes(range, level, pageId, file);
            return;
        }
        byte nodeType;
        byte treeNodeType;
        int keyCount;
        long successor;

        long leftSiblingPointer;
        long rightSiblingPointer;
        long leftSiblingPointerGeneration;
        long rightSiblingPointerGeneration;
        long currentNodeGeneration;
        var generationTarget = new GenerationKeeper();

        do {
            // for assertSiblings
            leftSiblingPointer =
                    TreeNodeUtil.leftSibling(cursor, stableGeneration, unstableGeneration, generationTarget);
            leftSiblingPointerGeneration = generationTarget.generation;
            rightSiblingPointer =
                    TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration, generationTarget);
            rightSiblingPointerGeneration = generationTarget.generation;
            leftSiblingPointer = pointer(leftSiblingPointer);
            rightSiblingPointer = pointer(rightSiblingPointer);
            currentNodeGeneration = TreeNodeUtil.generation(cursor);

            successor = TreeNodeUtil.successor(cursor, stableGeneration, unstableGeneration, generationTarget);

            keyCount = TreeNodeUtil.keyCount(cursor);
            nodeType = TreeNodeUtil.nodeType(cursor);
            treeNodeType = TreeNodeUtil.treeNodeType(cursor);
        } while (cursor.shouldRetry());
        checkAfterShouldRetry(cursor);

        if (nodeType != TreeNodeUtil.NODE_TYPE_TREE_NODE) {
            visitor.notATreeNode(pageId, file);
            return;
        }

        boolean isLeaf = treeNodeType == TreeNodeUtil.LEAF_FLAG;
        boolean isInternal = treeNodeType == TreeNodeUtil.INTERNAL_FLAG;
        if (!isInternal && !isLeaf) {
            visitor.unknownTreeNodeType(pageId, treeNodeType, file);
            return;
        }

        // check header pointers
        assertNoCrashOrBrokenPointerInGSPP(
                file,
                cursor,
                stableGeneration,
                unstableGeneration,
                GBPTreePointerType.leftSibling(),
                TreeNodeUtil.BYTE_POS_LEFTSIBLING,
                visitor,
                reportDirty);
        assertNoCrashOrBrokenPointerInGSPP(
                file,
                cursor,
                stableGeneration,
                unstableGeneration,
                GBPTreePointerType.rightSibling(),
                TreeNodeUtil.BYTE_POS_RIGHTSIBLING,
                visitor,
                reportDirty);
        assertNoCrashOrBrokenPointerInGSPP(
                file,
                cursor,
                stableGeneration,
                unstableGeneration,
                GBPTreePointerType.successor(),
                TreeNodeUtil.BYTE_POS_SUCCESSOR,
                visitor,
                reportDirty);

        boolean reasonableKeyCount =
                isLeaf ? leafNode.reasonableKeyCount(keyCount) : internalNode.reasonableKeyCount(keyCount);
        if (!reasonableKeyCount) {
            visitor.unreasonableKeyCount(pageId, keyCount, file);
        } else {
            var offloadIds =
                    assertKeyOrder(cursor, range, keyCount, isLeaf ? leafNode : internalNode, visitor, cursorContext);
            offloadIds.forEach(id -> addToSeenList(file, seenIds, id, state.lastId, visitor));
        }

        String nodeMetaReport;
        do {
            if (isLeaf) {
                nodeMetaReport = leafNode.checkMetaConsistency(cursor);
            } else {
                nodeMetaReport = internalNode.checkMetaConsistency(cursor);
            }
        } while (cursor.shouldRetry());
        checkAfterShouldRetry(cursor);
        if (!nodeMetaReport.isEmpty()) {
            visitor.nodeMetaInconsistency(pageId, nodeMetaReport, file);
        }

        assertPointerGenerationMatchesGeneration(
                parentPointerType, parentNode, pageId, pointerGeneration, currentNodeGeneration, visitor);
        // Assumption: We traverse the tree from left to right on every level
        rightmostPerLevel
                .forLevel(level)
                .assertNext(
                        cursor,
                        currentNodeGeneration,
                        leftSiblingPointer,
                        leftSiblingPointerGeneration,
                        rightSiblingPointer,
                        rightSiblingPointerGeneration,
                        visitor);
        checkSuccessorPointerGeneration(cursor, successor, visitor);

        if (!isInternal || !reasonableKeyCount || !nodeMetaReport.isEmpty()) {
            if (isLeaf) {
                monitor.dataKeysSeen(keyCount);
            }
            return;
        }

        if (level == 0 && numThreads > 1) {
            // Let's parallelize checking the children in the root, one child is one task
            var futures = new ArrayList<Future<Void>>();
            AtomicInteger nbrRunning = new AtomicInteger();
            AtomicBoolean lastOneSeen = new AtomicBoolean();
            var rightmostPerLevelFromShards = new ArrayList<RightmostInChainShard>();
            visitChildren(
                    cursor,
                    range,
                    keyCount,
                    level,
                    visitor,
                    cursorContext,
                    generationTarget,
                    (pos, treeNodeId, generation, childRange, isLast) -> {
                        // Add the RightmostInChain in child order, i.e. when visiting and not when checking (which is
                        // done by another thread)
                        var shardRightmostPerLevel = new RightmostInChainShard(file, pos == 0);
                        rightmostPerLevelFromShards.add(shardRightmostPerLevel);
                        nbrRunning.incrementAndGet();
                        if (isLast) {
                            lastOneSeen.set(true);
                        }
                        futures.add(state.executor.submit(() -> {
                            try (var shardContext = contextFactory.create(TAG_CHECK);
                                    var shardCursor = cursorFactory.apply(shardContext);
                                    var shardProgress = progress.threadLocalReporter()) {
                                goTo(shardCursor, "child at pos " + pos, treeNodeId);
                                checkSubtree(
                                        shardCursor,
                                        childRange,
                                        pageId,
                                        generation,
                                        GBPTreePointerType.child(pos),
                                        level + 1,
                                        visitor,
                                        state.threadLocalSeenIds(),
                                        cursorContext,
                                        shardRightmostPerLevel,
                                        shardProgress,
                                        monitor,
                                        () -> {});
                                int running = nbrRunning.decrementAndGet();
                                if (lastOneSeen.get() && running == 0) {
                                    checkRightmostInChainSeams(visitor, rightmostPerLevelFromShards);
                                    // This is guaranteed to be last since we know checkSubtree doesn't add
                                    // any new tasks on level > 0.
                                    taskToRunAsLastCheck.run();
                                }
                                return null;
                            }
                        }));
                    });
            removeAlreadyFinishedTasks(futures);
            state.trackSubtasks(futures);
        } else {
            visitChildren(
                    cursor,
                    range,
                    keyCount,
                    level,
                    visitor,
                    cursorContext,
                    generationTarget,
                    (pos, treeNodeId, generation, childRange, isLast) -> {
                        goTo(cursor, "child at pos " + pos, treeNodeId);
                        checkSubtree(
                                cursor,
                                childRange,
                                pageId,
                                generation,
                                GBPTreePointerType.child(pos),
                                level + 1,
                                visitor,
                                seenIds,
                                cursorContext,
                                rightmostPerLevel,
                                progress,
                                monitor,
                                () -> {});
                        goTo(cursor, "parent", pageId);
                    });
            // This is guaranteed to be last since we know checkSubtree doesn't add
            // any new tasks on level > 0
            taskToRunAsLastCheck.run();
        }
    }

    // Try to keep down the list of tasks to keep track of, by removing any that have already finished.
    // When checking a tree with many children on few threads it is likely that some didn't fit in the executor
    // queue and ran in the current thread
    private static void removeAlreadyFinishedTasks(ArrayList<Future<Void>> tasks) throws IOException {
        try {
            Iterator<Future<Void>> iterator = tasks.iterator();
            while (iterator.hasNext()) {
                Future<Void> next = iterator.next();
                if (next.isDone()) {
                    iterator.remove();
                    next.get();
                }
            }
        } catch (ExecutionException | InterruptedException e) {
            unwrapAndThrowException(e);
        }
    }

    private static void unwrapAndThrowException(Exception e) throws IOException {
        // There may be multiple layers of ExecutionException here, so unwrap those to get to the real cause
        var cause = Exceptions.findCauseOrSuppressed(e, t -> !(t instanceof ExecutionException))
                .orElse(e);
        Exceptions.throwIfInstanceOfOrUnchecked(cause, IOException.class, IOException::new);
    }

    private static void checkRightmostInChainSeams(
            GBPTreeConsistencyCheckVisitor visitor, List<RightmostInChainShard> rightmostPerLevelFromShards) {
        // No need to go to parent w/ the shardCursor, but we need to check the RightmostInChain
        // data and also check the seams between the shards.
        if (!rightmostPerLevelFromShards.isEmpty()) {
            var totalRightmost = rightmostPerLevelFromShards.get(0);
            for (var i = 1; i < rightmostPerLevelFromShards.size(); i++) {
                var shard = rightmostPerLevelFromShards.get(i);
                totalRightmost.assertAndMergeNext(shard, visitor);
            }
            totalRightmost.assertLast(visitor);
        }
    }

    private void assertPointerGenerationMatchesGeneration(
            GBPTreePointerType pointerType,
            long sourceNode,
            long pointer,
            long pointerGeneration,
            long targetNodeGeneration,
            GBPTreeConsistencyCheckVisitor visitor) {
        if (targetNodeGeneration > pointerGeneration) {
            visitor.pointerHasLowerGenerationThanNode(
                    pointerType, sourceNode, pointerGeneration, pointer, targetNodeGeneration, file);
        }
    }

    private void checkSuccessorPointerGeneration(
            PageCursor cursor, long successor, GBPTreeConsistencyCheckVisitor visitor) {
        if (isNode(successor)) {
            visitor.pointerToOldVersionOfTreeNode(cursor.getCurrentPageId(), pointer(successor), file);
        }
    }

    private void visitChildren(
            PageCursor cursor,
            KeyRange<KEY> range,
            int keyCount,
            int level,
            GBPTreeConsistencyCheckVisitor visitor,
            CursorContext cursorContext,
            GenerationKeeper generationTarget,
            ChildVisitor<KEY> childVisitor)
            throws IOException {
        long pageId = cursor.getCurrentPageId();
        KEY prev = layout.newKey();

        // Visit children, all except the last one
        int pos = 0;
        while (pos < keyCount) {
            KEY readKey = layout.newKey();
            KeyRange<KEY> childRange;
            long child;
            long childGeneration;
            assertNoCrashOrBrokenPointerInGSPP(
                    file,
                    cursor,
                    stableGeneration,
                    unstableGeneration,
                    GBPTreePointerType.child(pos),
                    internalNode.childOffset(pos),
                    visitor,
                    reportDirty);
            do {
                child = childAt(cursor, pos, generationTarget);
                childGeneration = generationTarget.generation;
                internalNode.keyAt(cursor, readKey, pos, cursorContext);
            } while (cursor.shouldRetry());
            checkAfterShouldRetry(cursor);

            childRange = range.newSubRange(level, pageId).restrictRight(readKey);
            if (pos > 0) {
                childRange = childRange.restrictLeft(prev);
            }

            childVisitor.accept(pos, child, childGeneration, childRange, false);
            layout.copyKey(readKey, prev);
            pos++;
        }

        // Check last child
        long child;
        long childGeneration;
        assertNoCrashOrBrokenPointerInGSPP(
                file,
                cursor,
                stableGeneration,
                unstableGeneration,
                GBPTreePointerType.child(pos),
                internalNode.childOffset(pos),
                visitor,
                reportDirty);
        do {
            child = childAt(cursor, pos, generationTarget);
            childGeneration = generationTarget.generation;
        } while (cursor.shouldRetry());
        checkAfterShouldRetry(cursor);
        var childRange = range.newSubRange(level, pageId).restrictLeft(prev);
        childVisitor.accept(pos, child, childGeneration, childRange, true);
    }

    private static void checkAfterShouldRetry(PageCursor cursor) throws CursorException {
        checkOutOfBounds(cursor);
        cursor.checkAndClearCursorException();
    }

    private long childAt(PageCursor cursor, int pos, GBPTreeGenerationTarget childGeneration) {
        return internalNode.childAt(cursor, pos, stableGeneration, unstableGeneration, childGeneration);
    }

    private LongList assertKeyOrder(
            PageCursor cursor,
            KeyRange<KEY> range,
            int keyCount,
            SharedNodeBehaviour<KEY> node,
            GBPTreeConsistencyCheckVisitor visitor,
            CursorContext cursorContext)
            throws IOException {
        DelayedVisitor delayedVisitor = new DelayedVisitor(file);
        var offloadIds = LongLists.mutable.empty();
        do {
            delayedVisitor.clear();
            offloadIds.clear();
            KEY prev = layout.newKey();
            KEY readKey = layout.newKey();
            boolean first = true;
            try {
                for (int pos = 0; pos < keyCount; pos++) {
                    node.keyAt(cursor, readKey, pos, cursorContext);
                    if (!range.inRange(readKey)) {
                        KEY keyCopy = layout.newKey();
                        layout.copyKey(readKey, keyCopy);
                        delayedVisitor.keysLocatedInWrongNode(
                                range, keyCopy, pos, keyCount, cursor.getCurrentPageId(), file);
                    }
                    if (!first) {
                        if (comparator.compare(prev, readKey) >= 0) {
                            delayedVisitor.keysOutOfOrderInNode(cursor.getCurrentPageId(), file);
                        }
                    } else {
                        first = false;
                    }
                    layout.copyKey(readKey, prev);
                    long offloadId = node.offloadIdAt(cursor, pos);
                    if (offloadId != NO_OFFLOAD_ID) {
                        offloadIds.add(offloadId);
                    }
                }
            } catch (Exception e) {
                cursor.setCursorException(e.toString());
            }
        } while (cursor.shouldRetry());
        checkAfterShouldRetry(cursor);
        delayedVisitor.report(visitor);
        return offloadIds;
    }

    static void assertNoCrashOrBrokenPointerInGSPP(
            Path file,
            PageCursor cursor,
            long stableGeneration,
            long unstableGeneration,
            GBPTreePointerType pointerType,
            int offset,
            GBPTreeConsistencyCheckVisitor visitor,
            boolean reportDirty)
            throws IOException {
        long currentNodeId = cursor.getCurrentPageId();

        long generationA;
        long readPointerA;
        long pointerA;
        short checksumA;
        boolean correctChecksumA;
        byte stateA;

        long generationB;
        long readPointerB;
        long pointerB;
        short checksumB;
        boolean correctChecksumB;
        byte stateB;
        do {
            cursor.setOffset(offset);
            // A
            generationA = GenerationSafePointer.readGeneration(cursor);
            readPointerA = GenerationSafePointer.readPointer(cursor);
            pointerA = pointer(readPointerA);
            checksumA = GenerationSafePointer.readChecksum(cursor);
            correctChecksumA = GenerationSafePointer.checksumOf(generationA, readPointerA) == checksumA;
            stateA = GenerationSafePointerPair.pointerState(
                    stableGeneration, unstableGeneration, generationA, readPointerA, correctChecksumA);

            // B
            generationB = GenerationSafePointer.readGeneration(cursor);
            readPointerB = GenerationSafePointer.readPointer(cursor);
            pointerB = pointer(readPointerA);
            checksumB = GenerationSafePointer.readChecksum(cursor);
            correctChecksumB = GenerationSafePointer.checksumOf(generationB, readPointerB) == checksumB;
            stateB = GenerationSafePointerPair.pointerState(
                    stableGeneration, unstableGeneration, generationB, readPointerB, correctChecksumB);
        } while (cursor.shouldRetry());

        if (reportDirty) {
            if (stateA == GenerationSafePointerPair.CRASH || stateB == GenerationSafePointerPair.CRASH) {
                visitor.crashedPointer(
                        currentNodeId,
                        pointerType,
                        generationA,
                        readPointerA,
                        pointerA,
                        stateA,
                        generationB,
                        readPointerB,
                        pointerB,
                        stateB,
                        file);
            }
        }
        if (stateA == GenerationSafePointerPair.BROKEN || stateB == GenerationSafePointerPair.BROKEN) {
            visitor.brokenPointer(
                    currentNodeId,
                    pointerType,
                    generationA,
                    readPointerA,
                    pointerA,
                    stateA,
                    generationB,
                    readPointerB,
                    pointerB,
                    stateB,
                    file);
        }
    }

    private static class DelayedVisitor extends GBPTreeConsistencyCheckVisitor.Adaptor {
        private final Path path;
        MutableLongList keysOutOfOrder = LongLists.mutable.empty();
        MutableList<KeyInWrongNode> keysLocatedInWrongNode = Lists.mutable.empty();

        DelayedVisitor(Path path) {
            this.path = path;
        }

        @Override
        public void keysOutOfOrderInNode(long pageId, Path file) {
            keysOutOfOrder.add(pageId);
        }

        @Override
        public void keysLocatedInWrongNode(
                KeyRange<?> range, Object key, int pos, int keyCount, long pageId, Path file) {
            keysLocatedInWrongNode.add(new KeyInWrongNode(pageId, range, key, pos, keyCount));
        }

        void clear() {
            keysOutOfOrder.clear();
            keysLocatedInWrongNode.clear();
        }

        void report(GBPTreeConsistencyCheckVisitor visitor) {
            if (keysOutOfOrder.notEmpty()) {
                keysOutOfOrder.forEach(pageId -> visitor.keysOutOfOrderInNode(pageId, path));
            }
            if (keysLocatedInWrongNode.notEmpty()) {
                keysLocatedInWrongNode.forEach(keyInWrongNode -> visitor.keysLocatedInWrongNode(
                        keyInWrongNode.range,
                        keyInWrongNode.key,
                        keyInWrongNode.pos,
                        keyInWrongNode.keyCount,
                        keyInWrongNode.pageId,
                        path));
            }
        }

        private record KeyInWrongNode(long pageId, KeyRange<?> range, Object key, int pos, int keyCount) {}
    }

    private static class FreelistSeenIdsVisitor implements IdProvider.IdProviderVisitor {
        private final Path path;
        private final BitSet seenIds;
        private final long lastId;
        private final GBPTreeConsistencyCheckVisitor visitor;
        private final ProgressListener progress;

        private FreelistSeenIdsVisitor(
                Path path,
                BitSet seenIds,
                long lastId,
                GBPTreeConsistencyCheckVisitor visitor,
                ProgressListener progress) {
            this.path = path;
            this.seenIds = seenIds;
            this.lastId = lastId;
            this.visitor = visitor;
            this.progress = progress;
        }

        @Override
        public void beginFreelistPage(long pageId) {
            addToSeenList(path, seenIds, pageId, lastId, visitor);
            progress.add(1);
        }

        @Override
        public void endFreelistPage(long pageId) {}

        @Override
        public void freelistEntry(long pageId, long generation, int pos) {
            addToSeenList(path, seenIds, pageId, lastId, visitor);
        }

        @Override
        public void freelistEntryFromReleaseCache(long pageId) {
            addToSeenList(path, seenIds, pageId, lastId, visitor);
        }
    }

    /**
     * Global state for a consistency check. This is useful for e.g. verifying that all IDs are accounted for, even when running consistency check
     * on a multi-root tree. All separate roots are checked with its own checker, but with the shared {@link ConsistencyCheckState} instance.
     */
    static class ConsistencyCheckState implements AutoCloseable {
        private final Path file;
        private final long lastId;
        private final GBPTreeConsistencyCheckVisitor visitor;
        private final List<BitSet> allThreadLocalSeenIds = Collections.synchronizedList(new ArrayList<>());
        private final ThreadLocal<BitSet> threadLocalSeenIds;
        private final BitSet mainSeenIds;
        final ExecutorService executor;
        final ProgressListener progress;
        final int numThreads;
        final LinkedList<Future<?>> subTasks;

        ConsistencyCheckState(
                Path file,
                IdProvider idProvider,
                GBPTreeConsistencyCheckVisitor visitor,
                CursorCreator cursorCreator,
                int numThreads,
                ProgressMonitorFactory progressMonitorFactory)
                throws IOException {
            this.file = file;
            this.lastId = idProvider.lastId();
            this.numThreads = numThreads;
            this.threadLocalSeenIds = ThreadLocal.withInitial(() -> {
                // TODO: limitation, can't run on an index larger than Integer.MAX_VALUE pages (which is fairly large)
                var seenIds = new BitSet(toIntExact(highId()));
                allThreadLocalSeenIds.add(seenIds);
                return seenIds;
            });
            this.mainSeenIds = threadLocalSeenIds.get();
            this.visitor = visitor;

            int numSpawnedThreads = Integer.max(1, numThreads - 1);
            this.executor = new ThreadPoolExecutor(
                    numSpawnedThreads,
                    numSpawnedThreads,
                    30,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(numSpawnedThreads * 2),
                    new NamedThreadFactory("GBPTreeConsistencyChecker"),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            this.progress = progressMonitorFactory.singlePart("Check GBPTree consistency", lastId);

            IdProvider.IdProviderVisitor freelistSeenIdsVisitor =
                    new FreelistSeenIdsVisitor(file, mainSeenIds, lastId, visitor, progress);
            idProvider.visitFreelist(freelistSeenIdsVisitor, cursorCreator);
            this.subTasks = new LinkedList<>();
        }

        BitSet threadLocalSeenIds() {
            return threadLocalSeenIds.get();
        }

        synchronized void trackSubtasks(List<Future<Void>> tasks) throws IOException {
            if (tasks != null) {
                subTasks.addAll(tasks);
                // While we're here let's try to keep the list size down a bit.
                try {
                    while (!subTasks.isEmpty() && subTasks.peekFirst().isDone()) {
                        subTasks.removeFirst().get();
                    }
                } catch (ExecutionException | InterruptedException e) {
                    unwrapAndThrowException(e);
                }
            }
        }

        synchronized void awaitAllSubtasks() throws IOException {
            try {
                Futures.getAll(subTasks);
            } catch (ExecutionException e) {
                unwrapAndThrowException(e);
            } finally {
                subTasks.clear();
            }
        }

        private long highId() {
            return lastId + 1;
        }

        @Override
        public void close() throws IOException {
            shutdownExecutor();

            // Should have already waited for the subtasks before getting here,
            // but let's make sure and check anyway.
            awaitAllSubtasks();

            for (var threadLocalSeenIds : allThreadLocalSeenIds) {
                if (threadLocalSeenIds != mainSeenIds) {
                    threadLocalSeenIds.stream().forEach(id -> addToSeenList(file, mainSeenIds, id, lastId, visitor));
                }
            }

            var index = (int) MIN_TREE_NODE_ID;
            final var highId = highId();
            while (index >= 0 && index < highId) {
                index = mainSeenIds.nextClearBit(index);
                if (index != -1 && index < highId) {
                    visitor.unusedPage(index, file);
                }
                index++;
            }

            progress.close();
        }

        private void shutdownExecutor() {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                // preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }

    interface ChildVisitor<KEY> {
        void accept(int pos, long treeNodeId, long generation, KeyRange<KEY> range, boolean isLast) throws IOException;
    }

    private static class RightmostInChainShard {
        private final List<RightmostInChain> rightmostPerLevel = new ArrayList<>();
        private final Path file;
        private final boolean leftmostShard;

        RightmostInChainShard(Path file, boolean leftmostShard) {
            this.file = file;
            this.leftmostShard = leftmostShard;
        }

        private RightmostInChain forLevel(int level) {
            // If this is the first time on this level, we will add a new entry
            for (int i = rightmostPerLevel.size(); i <= level; i++) {
                rightmostPerLevel.add(i, new RightmostInChain(file, leftmostShard));
            }
            return rightmostPerLevel.get(level);
        }

        private void assertLast(GBPTreeConsistencyCheckVisitor visitor) {
            rightmostPerLevel.forEach(rightmost -> rightmost.assertLast(visitor));
        }

        private void assertAndMergeNext(RightmostInChainShard shard, GBPTreeConsistencyCheckVisitor visitor) {
            for (var j = 0; j < shard.rightmostPerLevel.size() || j < rightmostPerLevel.size(); j++) {
                var left = j < rightmostPerLevel.size() ? rightmostPerLevel.get(j) : null;
                var right = j < shard.rightmostPerLevel.size() ? shard.rightmostPerLevel.get(j) : null;
                if (left != null && right != null) {
                    left.assertNext(right, visitor);
                }
                if (right != null) {
                    if (j >= rightmostPerLevel.size()) {
                        rightmostPerLevel.add(right);
                    } else {
                        rightmostPerLevel.set(j, right);
                    }
                }
            }
        }
    }

    @FunctionalInterface
    interface Monitor {
        void dataKeysSeen(int keyCount);
    }

    static Monitor NO_MONITOR = keyCount -> {};
}
