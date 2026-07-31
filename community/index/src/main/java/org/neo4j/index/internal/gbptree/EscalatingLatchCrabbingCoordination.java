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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCursor;

/**
 * Variant of {@link LatchCrabbingCoordination} which generalizes its one-level parent latch upgrade into an
 * escalation that can climb several levels. Once the leaf split is requested, this implementation walks up the
 * already-held read latch path, finds the lowest ancestor with room to absorb the cascade, upgrades every latch
 * from that ancestor down to the leaf to write, and lets the splits propagate under those write latches.
 *
 * Escalation refuses a few situations and falls back to pessimistic mode instead:
 *
 * Any ancestor up to and including the absorbing one that is not in the unstable generation, because changing
 * it would create a successor whose child pointer update propagates above the absorbing level.
 *
 * A splitting internal node that is the rightmost child of its parent. Splitting writes the old right sibling's
 * left sibling pointer without holding a latch on it.
 *
 * A cascade that would split the root, since that changes the root id and is rare enough to not be worth
 * optimizing.
 *
 * After an escalation that reached beyond the immediate parent, i.e. after any cascade that split an internal
 * node, the operation forces a path reset. This is required for correctness, because {@link InternalTreeLogic}
 * caches key range bounds.
 */
public class EscalatingLatchCrabbingCoordination implements TreeWriterCoordination {
    private static final int RESET_FREQUENCY = 20;
    private static final int NO_TARGET = -1;

    @FunctionalInterface
    interface EntrySizeLookup {
        int maxEntrySizeBound(CursorCreator cursorCreator, long treeNodeId, int keyCount) throws IOException;
    }

    private final TreeNodeLatchService latchService;
    private final EntrySizeLookup entrySizeLookup;
    private final int leafUnderflowThreshold;
    private final MultiRootGBPTree.Monitor monitor;
    private DepthData[] dataByDepth = new DepthData[10];
    private int depth = -1;
    private boolean pessimistic;
    private int operationCounter;
    private PageCursor cursor;
    private boolean escalationActive;
    private int escalationTarget;
    private boolean deepEscalation;
    private boolean stableParentUpgrade;

    EscalatingLatchCrabbingCoordination(
            TreeNodeLatchService latchService,
            EntrySizeLookup entrySizeLookup,
            int leafUnderflowThreshold,
            MultiRootGBPTree.Monitor monitor) {
        this.latchService = latchService;
        this.entrySizeLookup = entrySizeLookup;
        this.leafUnderflowThreshold = leafUnderflowThreshold;
        this.monitor = monitor;
    }

    @Override
    public void initialize(PageCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    public boolean checkForceReset() {
        var result = pessimistic || deepEscalation || stableParentUpgrade || operationCounter >= RESET_FREQUENCY;
        if (result) {
            operationCounter = 0;
        }
        return result;
    }

    @Override
    public void beginOperation() {
        this.pessimistic = false;
        this.escalationActive = false;
        this.deepEscalation = false;
        this.stableParentUpgrade = false;
        this.operationCounter++;
    }

    @Override
    public void beforeTraversingToChild(long childTreeNodeId, int childPos) {
        depth++;
        ensureEnoughDepth();
        DepthData depthData = dataByDepth[depth];
        if (depthData == null) {
            depthData = dataByDepth[depth] = new DepthData();
        }

        depthData.refLatch(childTreeNodeId, latchService);
        depthData.acquireLatch(pessimistic);
        depthData.childPos = childPos;
    }

    private void ensureEnoughDepth() {
        if (depth >= dataByDepth.length) {
            dataByDepth = Arrays.copyOf(dataByDepth, dataByDepth.length * 2);
        }
    }

    @Override
    public boolean arrivedAtChild(boolean isInternal, int availableSpace, boolean isStable, int keyCount) {
        DepthData depthData = dataByDepth[depth];
        depthData.availableSpace = availableSpace;
        depthData.isStable = isStable;
        depthData.keyCount = keyCount;
        if (isInternal || pessimistic) {
            return true;
        }

        if (!tryUpgradeReadLatchToWrite(depth)) {
            return false;
        }

        if (isStable) {
            if (positionedAtTheEdge(depth)) {
                // The successor creation will update sibling pointers, and an edge position means one of the
                // siblings sits under a neighbouring parent which isn't latched, so fall back to pessimistic.
                return false;
            }
            return tryUpgradeParentReadLatchToWrite();
        }
        return true;
    }

    private boolean positionedAtTheEdge(int depth) {
        int childPos = dataByDepth[depth].childPos;
        return childPos == 0 || (depth > 0 && childPos == dataByDepth[depth - 1].keyCount);
    }

    @Override
    public void updateChildInformation(int availableSpace, int keyCount) {
        var depthData = dataByDepth[depth];
        depthData.availableSpace = availableSpace;
        depthData.keyCount = keyCount;
    }

    @Override
    public boolean beforeSplittingLeaf(int bubbleEntrySize) {
        if (pessimistic) {
            return true;
        }
        if (depth == 0) {
            // The leaf is the root, splitting it grows the tree.
            return false;
        }

        int target = findAbsorbingAncestor(bubbleEntrySize);
        if (target == NO_TARGET) {
            return false;
        }
        for (int level = target; level < depth; level++) {
            if (!tryUpgradeReadLatchToWrite(level)) {
                return false;
            }
        }
        escalationActive = true;
        escalationTarget = target;
        boolean deep = target < depth - 1;
        if (deep) {
            deepEscalation = true;
        }
        monitor.treeWriterEscalated(deep);

        return true;
    }

    /**
     * Finds the lowest ancestor level which can absorb the entry insert cascading up from a leaf split
     */
    private int findAbsorbingAncestor(int bubbleEntrySize) {
        int need = bubbleEntrySize;
        for (int level = depth - 1; level >= 0; level--) {
            DepthData ancestor = dataByDepth[level];
            if (ancestor.isStable) {
                return NO_TARGET;
            }
            if (!ancestor.latchTypeIsWrite && !ancestor.latch.couldUpgradeToWrite()) {
                return NO_TARGET;
            }
            if (ancestor.availableSpace >= need) {
                return level;
            }
            if (level == 0) {
                // Cascade would split the root.
                return NO_TARGET;
            }
            if (ancestor.childPos == dataByDepth[level - 1].keyCount) {
                // This level would split while being the rightmost child of its parent, see class javadoc.
                return NO_TARGET;
            }
            need = Math.max(need, maxEntrySizeBound(level));
        }
        return NO_TARGET;
    }

    private int maxEntrySizeBound(int level) {
        DepthData node = dataByDepth[level];
        try {
            return entrySizeLookup.maxEntrySizeBound(
                    CursorCreator.bind(cursor), node.latch.treeNodeId(), node.keyCount);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean beforeRemovalFromLeaf(int sizeOfLeafEntryToRemove) {
        if (pessimistic) {
            return true;
        }

        int availableSpaceAfterRemoval = dataByDepth[depth].availableSpace + sizeOfLeafEntryToRemove;
        return availableSpaceAfterRemoval <= leafUnderflowThreshold;
    }

    @Override
    public boolean pessimistic() {
        return pessimistic;
    }

    @Override
    public void beforeSplitInternal(long treeNodeId) {
        if (pessimistic) {
            return;
        }
        if (escalationActive && depth > escalationTarget) {
            assert dataByDepth[depth].latchTypeIsWrite;
            assert dataByDepth[depth - 1].latchTypeIsWrite;
            return;
        }
        throw new IllegalStateException(
                format("Unexpected split of internal node [%d] in optimistic mode", treeNodeId));
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
        if (escalationActive && depth <= escalationTarget) {
            // The cascade has arrived at the absorbing level, anything structural above this point is
            // unauthorized again.
            escalationActive = false;
        }
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
        escalationActive = false;
        monitor.treeWriterFlippedToPessimistic();
    }

    @Override
    public void close() {
        depth = -1;
        IOUtils.closeAllUnchecked(dataByDepth);
    }

    private boolean tryUpgradeParentReadLatchToWrite() {
        if (depth == 0) {
            return false;
        }
        // A leaf successor only needs a child pointer replaced which does not create successor of the parent
        // Force reset afterward to prevent holding write latch on hot parent page across operations
        if (tryUpgradeReadLatchToWrite(depth - 1)) {
            if (dataByDepth[depth - 1].isStable) {
                stableParentUpgrade = true;
            }
            return true;
        }
        return false;
    }

    private boolean tryUpgradeReadLatchToWrite(int depth) {
        if (depth < 0) {
            return false;
        }
        return dataByDepth[depth].tryUpgradeLatchToWrite();
    }

    private void releaseLatchAtDepth(int depth) {
        dataByDepth[depth].releaseLatch();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(
                format("ESCALATING LATCHES %s depth:%d%n", pessimistic ? "PESSIMISTIC" : "OPTIMISTIC", depth));
        for (int i = 0; i <= depth; i++) {
            TreeNodeLatch latch = dataByDepth[i].latch;
            builder.append(dataByDepth[i].latchTypeIsWrite ? "W" : "R")
                    .append(latch.toString())
                    .append(System.lineSeparator());
        }
        return builder.toString();
    }

    private static class DepthData implements AutoCloseable {
        private TreeNodeLatch latch;
        private boolean latchTypeIsWrite;
        private boolean latchIsAcquired;
        private int availableSpace;
        private int keyCount;
        private int childPos;
        private boolean isStable;

        private void refLatch(long childTreeNodeId, TreeNodeLatchService latchService) {
            if (latch != null) {
                if (latch.treeNodeId() == childTreeNodeId) {
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
}
