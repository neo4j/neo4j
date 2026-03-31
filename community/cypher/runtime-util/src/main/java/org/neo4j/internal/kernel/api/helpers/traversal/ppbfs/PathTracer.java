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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs;

import java.util.function.Function;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * The PathTracer will use data produced by PGPathPropagatingBFS to return paths between the source and the target.
 * It runs a DFS from a given target through the produce path trace data back towards the source.
 *
 * The PathTracer is also responsible for doing bookkeeping related to PPBFS which is only possible to do during
 * path tracing. Therefore, it's very important to exhaust the path tracer for every target node.
 */
public final class PathTracer<Row> extends PrefetchingIterator<Row> {
    private final PPBFSHooks hooks;
    private final SignpostStack stack;
    private NodeState sourceNode;

    private Function<SignpostStack, Row> toRow;

    /**
     * Because path tracing performs much of the bookkeeping of PPBFS, we may need to continue to trace paths to a
     * target node, even if we have already yielded the K paths necessary for that target node.
     * This flag tracks whether we should continue to yield paths when tracing.
     */
    public boolean isSaturated() {
        return stack.target().isSaturated();
    }

    private boolean shouldReturnSingleNodePath;

    /**
     *  The PathTracer is designed to be reused, but its state is reset in two places ({@link #reset} and
     *  {@link #initialize}); this variable tracks whether we are in a valid state to iterate.
     */
    private boolean ready = false;

    public PathTracer(MemoryTracker memoryTracker, TraversalMatchModeFactory tracker, PPBFSHooks hooks) {
        this.hooks = hooks;
        this.stack = new SignpostStack(memoryTracker, tracker.twoWaySignpostTracking(), hooks);
    }

    /**
     * Clears the PathTracer and SignpostStack, allowing references to be garbage collected.
     * {@link #initialize} must be called after this to correctly set up the PathTracer.
     */
    @Override
    public void reset() {
        super.reset();
        this.ready = false; // until initialize is called, consider the iterator invalid
        this.sourceNode = null;
        this.stack.reset();
    }

    /**
     * Finish setting up the PathTracer; this method should be called every time a target node is to be traced at
     * a given length.
     * {@link #reset} must be called prior to this if the SignpostStack has been used previously.
     */
    public void initialize(
            Function<SignpostStack, Row> toRow, NodeState sourceNode, NodeState targetNode, int dgLength) {
        this.toRow = toRow;
        Preconditions.checkState(!ready, "PathTracer was not reset before initializing");
        this.ready = true;
        this.sourceNode = sourceNode;

        this.stack.initialize(targetNode, dgLength);
        this.shouldReturnSingleNodePath = targetNode == sourceNode && dgLength == 0;
    }

    /**
     * The PathTracer is designed to be reused, but its state is reset in two places ({@link #reset} and
     * {@link #initialize}); this function returns true if the tracer has been correctly set up/reset
     */
    public boolean ready() {
        return this.ready;
    }

    private void popAndPrune() {
        var popped = stack.pop();
        if (popped == null) {
            return;
        }

        int sourceLength = stack.lengthFromSource();
        if (!popped.isValidatedAtLength(sourceLength) && !stack.isProtectedFromPruning()) {
            popped.pruneSourceLength(sourceLength);
        }
    }

    @Override
    protected Row fetchNextOrNull() {
        if (!ready) {
            throw new IllegalStateException("PathTracer attempted to iterate without initializing.");
        }

        if (shouldReturnSingleNodePath && !isSaturated()) {
            shouldReturnSingleNodePath = false;
            Preconditions.checkState(
                    stack.lengthFromSource() == 0, "Attempting to return a path that does not reach the source");
            return toRow.apply(stack);
        }

        while (stack.hasNext()) {
            if (!stack.pushNext()) {
                popAndPrune();
            } else {
                var sourceSignpost = stack.headSignpost();
                // Set minTargetDistance unconditionally — the distance to target is structural
                // and correct regardless of trail validity. This enables propagation to create
                // longer source lengths through the chain after BFS lengths are pruned.
                if (!sourceSignpost.hasBeenTraced()) {
                    sourceSignpost.setMinTargetDistance(stack.lengthToTarget(), PGPathPropagatingBFS.Phase.Tracing);
                }

                if (stack.canAbandonTraceBranch()) {
                    hooks.skippingDuplicateRelationship(stack);
                    stack.pop();
                    // the order of these predicates is important since validate has side effects:
                } else if (sourceSignpost.prevNode == sourceNode && stack.validate() && !isSaturated()) {
                    Preconditions.checkState(
                            stack.lengthFromSource() == 0,
                            "Attempting to return a path that does not reach the source");
                    hooks.returnPath(stack);
                    return toRow.apply(stack);
                }
            }
        }
        return null;
    }

    public void decrementTargetCount() {
        stack.target().decrementTargetCount();
    }
}
