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

import static java.lang.String.format;
import static java.util.Arrays.sort;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCursor;

/**
 * Locks nodes as traversal goes down the tree. The locking scheme is a variant of what is known as "Better Latch Crabbing" and consists of
 * an optimistic and a pessimistic mode.
 * <p>
 * Optimistic mode uses {@link LongSpinLatch#acquireRead() read latches} all the way down to leaf.
 * Down at the leaf the latch is upgraded to write (if child pointers would have leaf/internal bit this step could be skipped).
 * If operation is unsafe (split/merge) then first an optimistic latch upgrade on parent is attempted - if successful the operation
 * can continue. Otherwise, as well as for failure to upgrade latches will result in releasing of the latches and flip to pessimistic mode.
 * <p>
 * Pessimistic mode uses {@link LongSpinLatch#acquireWrite() write latches} all the way down to leaf and performs the change.
 * Even split/merge can be done since write latches on parents are also acquired. In typical latch crabbing write latches on parents can be released
 * when traversing down if the operation on the lower level is considered safe, i.e. taking into consideration that a split could occur and
 * that the parent has space enough to hold one more key. In the case of dynamically sized keys, together with "minimal splitter", knowing the
 * size of the key to potentially insert into the parent is not known before actually doing the operation and therefore the parent latches cannot
 * be released when traversing down the tree.
 *
 * <pre>
 *     The locking scheme in picture:
 *
 *                              [  1  ]
 *                   ┌──────────┘ │ │ └───────────┐
 *                   │        ┌───┘ └───┐         │
 *                 [ 2 ]   [ 3 ]      [ 4 ]     [ 5 ]
 *
 *     Optimistic:
 *
 *     Example of non-contending, simple insertion:
 *     - Read latch on [1]
 *     - Read latch on [3]
 *     - Upgrade read latch on [3] to write
 *     - Insert into [3]
 *
 *     Example of non-contending, split insertion:
 *     - Read latch on [1]
 *     - Read latch on [3]
 *     - Upgrade read latch on [3] to write
 *     - Notice that [3] needs to split, ask to upgrade read latch on [1] to write
 *     - Split [3] and insert
 *     - Insert splitter key into [1]
 *
 *     Example of non-contending, split insertion where [1] cannot fit the bubble key:
 *     - Read latch on [1]
 *     - Read latch on [3]
 *     - Upgrade read latch on [3] to write
 *     - Notice that [3] needs to split, ask to upgrade read latch on [1] to write
 *     - Notice that [1] cannot fit the bubble key, abort and flip to pessimistic mode
 *
 *     Example of contending, simple insertion:
 *     - Read latch on [1]
 *     - Read latch on [3]
 *     - Fail upgrade read latch on [3] to write since another writer has read latch too
 *     - Flip to pessimistic mode
 *
 *     Example of contending, split insertion:
 *     - Read latch on [1]
 *     - Read latch on [3]
 *     - Notice that [3] needs to split, ask to upgrade read latch on [1] to write, but fails since another writer has read lock too
 *     - Flip to pessimistic mode
 * </pre>
 */
class LatchCrabbingCoordination implements TreeWriterCoordination {
    static final int DEFAULT_RESET_FREQUENCY = 20;

    private final TreeNodeLatchService latchService;
    private final int leafUnderflowThreshold;
    private final int resetFrequency;
    private DepthData[] dataByDepth = new DepthData[10];
    private int depth = -1;
    private boolean pessimistic;
    private int operationCounter;
    private PageCursor cursor;

    LatchCrabbingCoordination(TreeNodeLatchService latchService, int leafUnderflowThreshold, int resetFrequency) {
        this.latchService = latchService;
        this.leafUnderflowThreshold = leafUnderflowThreshold;
        this.resetFrequency = resetFrequency;
    }

    @Override
    public void initialize(PageCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    public boolean checkForceReset() {
        var result = pessimistic || operationCounter >= resetFrequency;
        if (result) {
            operationCounter = 0;
        }
        return result;
    }

    @Override
    public void beginOperation() {
        inc(Stat.TOTAL_OPERATIONS);
        this.pessimistic = false;
        this.operationCounter++;
    }

    @Override
    public void beforeTraversingToChild(long childTreeNodeId, int childPos) {
        // Remember information about the latch
        depth++;
        if (depth >= dataByDepth.length) {
            dataByDepth = Arrays.copyOf(dataByDepth, dataByDepth.length * 2);
        }
        DepthData depthData = dataByDepth[depth];
        if (depthData == null) {
            depthData = dataByDepth[depth] = new DepthData();
        }

        // Acquire latch on the child node
        depthData.refLatch(childTreeNodeId, latchService);
        depthData.acquireLatch(pessimistic);
        depthData.childPos = childPos;
    }

    @Override
    public boolean arrivedAtChild(boolean isInternal, int availableSpace, boolean isStable, int keyCount) {
        DepthData depthData = dataByDepth[depth];
        depthData.availableSpace = availableSpace;
        depthData.isStable = isStable;
        depthData.keyCount = keyCount;
        if (isInternal || pessimistic) {
            // Wait to make decision till we reach the leaf. Also if we're in pessimistic mode then bailing out isn't an
            // option.
            return true;
        }

        // If we have arrived at leaf in optimistic mode, upgrade to write latch
        boolean upgraded = tryUpgradeReadLatchToWrite(depth);
        if (!upgraded) {
            inc(Stat.FAIL_LEAF_UPGRADE);
            return false;
        }

        if (isStable) {
            if (depthData.positionedAtTheEdge()) {
                // If the leaf we're updating needs a successor and the position of this leaf in the parent is at the
                // edge
                // it means that one of its siblings sits in a neighbour parent, which isn't currently locked, so fall
                // back to pessimistic
                inc(Stat.FAIL_SUCCESSOR_SIBLING);
                return false;
            }
            return tryUpgradeUnstableParentReadLatchToWrite();
        }

        return true;
    }

    @Override
    public void updateChildInformation(int availableSpace, int keyCount) {
        var depthData = dataByDepth[depth];
        depthData.availableSpace = availableSpace;
        depthData.keyCount = keyCount;
    }

    @Override
    public boolean beforeSplittingLeaf(int bubbleEntrySize) {
        inc(Stat.LEAF_SPLITS);
        if (pessimistic) {
            return true;
        }

        // We have one chance to do a simple optimization here, which is that if we can see that adding this bubble key
        // to the parent
        // without this parent change propagating any further we can try to upgrade the parent read latch and continue
        // with the leaf split
        // in optimistic mode
        DepthData parent = depth > 0 ? dataByDepth[depth - 1] : null;
        boolean parentSafe = parent != null && parent.availableSpace - bubbleEntrySize >= 0;
        if (!parentSafe) {
            // This split will cause parent split too... it's getting complicated at this point so bail out to
            // pessimistic mode
            inc(Stat.FAIL_LEAF_SPLIT_PARENT_UNSAFE);
            return false;
        }
        return tryUpgradeUnstableParentReadLatchToWrite();
    }

    @Override
    public boolean beforeRemovalFromLeaf(int sizeOfLeafEntryToRemove) {
        if (pessimistic) {
            return true;
        }

        int availableSpaceAfterRemoval = dataByDepth[depth].availableSpace + sizeOfLeafEntryToRemove;
        boolean leafWillUnderflow = availableSpaceAfterRemoval > leafUnderflowThreshold;
        if (leafWillUnderflow) {
            inc(Stat.FAIL_LEAF_UNDERFLOW);
            return false;
        }
        return true;
    }

    @Override
    public boolean beforeAccessingRightSiblingLeaf(long siblingNodeId) {
        if (pessimistic) {
            return true;
        }
        inc(Stat.FAIL_NEED_UPDATE_SIBLING_LEAF);
        return false;
    }

    @Override
    public boolean pessimistic() {
        return pessimistic;
    }

    @Override
    public void beforeSplitInternal(long treeNodeId) {
        if (!pessimistic) {
            throw new IllegalStateException(
                    format("Unexpected split of internal node [%d] in optimistic mode", treeNodeId));
        }
    }

    @Override
    public void beforeUnderflowInLeaf(long treeNodeId) {
        if (!pessimistic) {
            throw new IllegalStateException(
                    format("Unexpected underflow of leaf node [%d] in optimistic mode", treeNodeId));
        }
    }

    @Override
    public void up() {
        releaseLatchAtDepth(depth--);
    }

    @Override
    public void reset() {
        while (depth >= 0) {
            up();
        }
        if (cursor != null) {
            cursor.unpin();
        }
    }

    @Override
    public void flipToPessimisticMode() {
        reset();
        pessimistic = true;
        inc(Stat.PESSIMISTIC);
    }

    @Override
    public void close() {
        depth = -1;
        IOUtils.closeAllUnchecked(dataByDepth);
    }

    /**
     * Tries to upgrade the parent read latch to write if the parent is unstable.
     * @return {@code true} if the parent is in unstable generation and latch was successfully upgraded from read to write, otherwise {@code false}.
     */
    private boolean tryUpgradeUnstableParentReadLatchToWrite() {
        if (depth == 0 || dataByDepth[depth - 1].isStable) {
            inc(Stat.FAIL_PARENT_NEEDS_SUCCESSOR);
            // If the parent needs to create successor it means that this will cause an update on grand parent.
            // Since this will only happen this first time this parent needs successor it's fine to bail out to
            // pessimistic.
            return false;
        }

        // Try to upgrade parent latch to write, otherwise bail out to pessimistic.
        boolean upgraded = tryUpgradeReadLatchToWrite(depth - 1);
        if (!upgraded) {
            inc(Stat.FAIL_PARENT_UPGRADE);
        }
        return upgraded;
    }

    private boolean tryUpgradeReadLatchToWrite(int depth) {
        if (depth < 0) {
            // We're doing something on the root node, which is a leaf a.t.m? Anyway flip to pessimistic.
            return false;
        }

        DepthData depthData = dataByDepth[depth];
        return depthData.tryUpgradeLatchToWrite();
    }

    private void releaseLatchAtDepth(int depth) {
        DepthData depthData = dataByDepth[depth];
        depthData.releaseLatch();
    }

    @Override
    public String toString() {
        StringBuilder builder =
                new StringBuilder(format("LATCHES %s depth:%d%n", pessimistic ? "PESSIMISTIC" : "OPTIMISTIC", depth));
        for (int i = 0; i <= depth; i++) {
            LongSpinLatch latch = dataByDepth[i].latch;
            builder.append(dataByDepth[i].latchTypeIsWrite ? "W" : "R")
                    .append(latch.toString())
                    .append(format("%n"));
        }
        return builder.toString();
    }

    private static class DepthData implements AutoCloseable {
        private LongSpinLatch latch;
        private boolean latchTypeIsWrite;
        private boolean latchIsAcquired;
        private int availableSpace;
        private int keyCount;
        private int childPos;
        private boolean isStable;

        boolean positionedAtTheEdge() {
            return childPos == 0 || childPos == keyCount;
        }

        private void refLatch(long childTreeNodeId, TreeNodeLatchService latchService) {
            if (latch != null) {
                if (latch.treeNodeId() == childTreeNodeId) {
                    // It's the same one as the last time we went down. We still have a ref on it so just use it
                    return;
                }
                derefLatch();
            }
            latch = latchService.latch(childTreeNodeId);
        }

        void derefLatch() {
            assert !latchIsAcquired;
            if (latch != null) {
                try {
                    latch.deref();
                } finally {
                    latch = null;
                }
            }
        }

        void acquireLatch(boolean write) {
            assert !latchIsAcquired;
            if (write) {
                latch.acquireWrite();
            } else {
                latch.acquireRead();
            }
            latchTypeIsWrite = write;
            latchIsAcquired = true;
        }

        void releaseLatch() {
            if (latchIsAcquired) {
                latchIsAcquired = false;
                if (latchTypeIsWrite) {
                    latch.releaseWrite();
                } else {
                    latch.releaseRead();
                }
            }
        }

        boolean tryUpgradeLatchToWrite() {
            assert latchIsAcquired;
            if (!latchTypeIsWrite) {
                if (!latch.tryUpgradeToWrite()) {
                    // To avoid deadlock.
                    return false;
                }
                latchTypeIsWrite = true;
            }
            return true;
        }

        @Override
        public void close() {
            try {
                releaseLatch();
            } finally {
                derefLatch();
            }
        }
    }

    // === STATS FOR LEARNING PURPOSES ===

    private static final boolean KEEP_STATS = false;

    private enum Stat {
        /**
         * Total number of operations where one operation is one merge or remove on the writer.
         */
        TOTAL_OPERATIONS,
        /**
         * Number of operations that had to flip to pessimistic mode.
         */
        PESSIMISTIC(TOTAL_OPERATIONS),
        /**
         * Number of operations that resulted in leaf split.
         */
        LEAF_SPLITS(TOTAL_OPERATIONS),
        /**
         * Number of "flip to pessimistic" caused by failure to upgrade read latch on the leaf to write.
         */
        FAIL_LEAF_UPGRADE(PESSIMISTIC),
        /**
         * Number of "flip to pessimistic" caused by parent not considered safe on leaf split.
         */
        FAIL_LEAF_SPLIT_PARENT_UNSAFE(PESSIMISTIC),
        /**
         * Number of "flip to pessimistic" caused by leaf underflow.
         */
        FAIL_LEAF_UNDERFLOW(PESSIMISTIC),
        /**
         * Number of "flip to pessimistic" caused by leaf needing successor and leaf's childPos in parent being either 0 or keyCount.
         */
        FAIL_SUCCESSOR_SIBLING(PESSIMISTIC),
        /**
         * Number of "flip to pessimistic" caused by parent (due to leaf split) needing successor.
         */
        FAIL_PARENT_NEEDS_SUCCESSOR(PESSIMISTIC),
        /**
         * Number of "flip to pessimistic" caused by failure to upgrade parent read latch to write.
         */
        FAIL_PARENT_UPGRADE(PESSIMISTIC),
        /**
         * Number of "flip to pessimistic" caused by operation needing to update sibling leaf.
         */
        FAIL_NEED_UPDATE_SIBLING_LEAF(PESSIMISTIC);

        private final Stat comparedTo;
        private final LongAdder count = new LongAdder();

        Stat() {
            this(null);
        }

        Stat(Stat comparedTo) {
            this.comparedTo = comparedTo;
        }
    }

    /**
     * Increments statistics about the effect this monitor has on traversals.
     * @param stat the specific statistic to increment.
     */
    private static void inc(Stat stat) {
        if (KEEP_STATS) {
            stat.count.add(1);
        }
    }

    static {
        if (KEEP_STATS) {
            Runtime.getRuntime().addShutdownHook(new Thread(LatchCrabbingCoordination::dumpStats));
        }
    }

    static void dumpStats() {
        System.out.println("Stats for GBPTree parallel writes locking:");
        Stat[] stats = Stat.values();
        sort(stats, (s1, s2) -> Long.compare(s2.count.sum(), s1.count.sum()));
        for (Stat stat : stats) {
            long sum = stat.count.sum();
            System.out.printf("  %s: %d", stat.name(), sum);
            if (stat.comparedTo != null) {
                long comparedToSum = stat.comparedTo.count.sum();
                double percentage = (100D * sum) / comparedToSum;
                System.out.printf(" (%.4f%% of %s)", percentage, stat.comparedTo.name());
            }
            System.out.println();
        }
    }
}
