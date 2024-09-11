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

import java.util.BitSet;
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

    /** The length of the currently traced path when projected back to the data graph */
    private int dgLength;

    private final BitSet protectFromPruning;
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

    public PathTracer(MemoryTracker memoryTracker, PPBFSHooks hooks) {
        this.protectFromPruning = new BitSet();
        this.hooks = hooks;
        this.stack = new SignpostStack(memoryTracker, hooks);
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

        this.dgLength = dgLength;
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
        if (!popped.isVerifiedAtLength(sourceLength) && !this.protectFromPruning.get(stack.size())) {
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
                this.protectFromPruning.set(stack.size() - 1, false);

                if (stack.isTargetTrail() && !sourceSignpost.hasBeenTraced()) {
                    sourceSignpost.setMinTargetDistance(stack.lengthToTarget(), PGPathPropagatingBFS.Phase.Tracing);
                }

                if (allNodesAreValidatedBetweenDuplicates()) {
                    hooks.skippingDuplicateRelationship(stack);
                    stack.pop();
                    // the order of these predicates is important since validateTrail has side effects:
                } else if (sourceSignpost.prevNode == sourceNode && validateTrail() && !isSaturated()) {
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

    /** this function allows us to abandon a trace branch early. if we have detected a duplicate relationship then
     * the set of paths we're currently tracing are all invalid and so we should be able to abort tracing them, except
     * tracing also performs verification/validation.
     *
     * if the current node is validated then further tracing has no benefit, so we can pop back to the previous
     * node.
     * */
    private boolean allNodesAreValidatedBetweenDuplicates() {
        int dup = stack.distanceToDuplicate();

        if (dup == 0) {
            return false;
        }

        int sourceLength = stack.lengthFromSource();
        for (int i = 0; i <= dup; i++) {
            var candidate = stack.signpost(stack.size() - 1 - i);

            if (!candidate.prevNode.validatedAtLength(sourceLength)) {
                return false;
            }

            sourceLength += candidate.dataGraphLength();
        }

        this.protectFromPruning.set(stack.size() - 1 - dup, stack.size() - 1, true);
        return true;
    }

    private boolean validateTrail() {
        int sourceLength = 0;
        for (int i = stack.size() - 1; i >= 0; i--) {
            TwoWaySignpost signpost = stack.signpost(i);
            sourceLength += signpost.dataGraphLength();
            if (signpost instanceof TwoWaySignpost.RelSignpost rel) {
                var bitset = stack.relationshipPresenceAtDepth.get(rel.relId);
                assert bitset.get(i);
                if (bitset.length() > i + 1) {
                    hooks.invalidTrail(stack);
                    return false;
                }
            } else if (signpost instanceof TwoWaySignpost.MultiRelSignpost rels) {
                for (int j = 0; j < rels.rels.length; j++) {
                    long relId = rels.rels[j];
                    var bitset = stack.relationshipPresenceAtDepth.get(relId);
                    assert bitset.get(i);
                    if (bitset.length() > i + 1) {
                        hooks.invalidTrail(stack);
                        return false;
                    }
                }
            }
            if (!signpost.isVerifiedAtLength(sourceLength)) {
                signpost.setVerified(sourceLength);
                if (!signpost.forwardNode.validatedAtLength(sourceLength)) {
                    signpost.forwardNode.validateSourceLength(sourceLength, dgLength - sourceLength);
                }
            }
        }
        return true;
    }

    public void decrementTargetCount() {
        stack.target().decrementTargetCount();
    }
}
