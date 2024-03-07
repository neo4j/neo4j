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

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_ENTITY;

import java.util.BitSet;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.Measurable;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * A NodeData stores all algorithm-related bookkeeping for a given product (pair) of data graph node and NFA state.
 * Put differently, it is an incarnation of the nodes in a product graph, with attached PPBFS metadata.
 *
 * NodeData within an execution of the PPBFS algorithm should be unique for a given (node id, state id) pair; this
 * assumption is relied upon in several places. This is the purpose of {@link HeapTrackingNodeDatas}: to act as a repository
 * for all product graph nodes.
 */
public final class NodeData implements AutoCloseable, Measurable {

    private static final int SIGNPOSTS_INIT_SIZE = 2;

    private final long id; // Data graph id
    private final State state;

    final DataManager dataManager;

    private final HeapTrackingArrayList<TwoWaySignpost> sourceSignposts;
    private HeapTrackingArrayList<TwoWaySignpost> targetSignposts;

    // The depths (originating from the source node) at which this node has been found.
    private final BitSet lengthsFromSource;

    // A subset of lengthsFromSource. This is the set of lengths for which this node has verified SourceSignposts
    private final BitSet validatedLengthsFromSource;

    // The length of the shortest path in the data graph from the source node to this node which is accepted by the NFA.
    // This is not necessarily a trail length, the corresponding path may have repeated relationships.
    private final int distanceFromSource;

    // This is initialised to K when we run both SHORTEST K and SHORTEST K GROUPS.
    // It is then decremented whenever we return a path (or group) ending with this node.
    // NB: this mechanism relies on the NFA having a single final state
    private int remainingTargetCount = 0;

    private boolean isTarget = false;

    public NodeData(
            MemoryTracker mt, long id, State state, int distanceFromSource, DataManager dataManager, long intoTarget) {
        this.sourceSignposts = HeapTrackingArrayList.newArrayList(SIGNPOSTS_INIT_SIZE, mt);
        this.id = id;
        this.state = state;
        this.distanceFromSource = distanceFromSource;
        this.dataManager = dataManager;
        this.lengthsFromSource = new BitSet();
        this.validatedLengthsFromSource = new BitSet();

        if (state().isFinalState() && (intoTarget == NO_SUCH_ENTITY || intoTarget == id)) {
            this.remainingTargetCount = (int) dataManager.initialCountForTargetNodes;
            this.isTarget = true;
            dataManager.incrementLiveTargetCount();
        }

        mt.allocateHeap(estimatedHeapUsage());
    }

    public State state() {
        return state;
    }

    public boolean isTarget() {
        return this.isTarget;
    }

    public int nextSignpostIndexForLength(int currentIndex, int lengthFromSource) {
        for (int i = currentIndex + 1; i < sourceSignposts.size(); i++) {
            if (sourceSignposts.get(i).hasSourceLength(lengthFromSource)) {
                return i;
            }
        }
        return -1;
    }

    public TwoWaySignpost getSourceSignpost(int index) {
        return sourceSignposts.get(index);
    }

    public void synchronizeLengthAfterPrune(int lengthFromSource) {
        // Potential optimisation:
        //   This method is always called together with nextSignpostIndexForLength. If we were to return the first
        //   index with a SourceSignpost at the corresponding length from this method,
        //   it could be used to potentially avoid calling nextSignpostIndexForLength in some lucky cases, when the
        //   returned index is greater than the value of currentIndex being passed to nextSignpostIndexForLength.

        for (int i = 0; i < sourceSignposts.size(); i++) {
            if (sourceSignposts.get(i).hasSourceLength(lengthFromSource)) {
                return;
            }
        }

        assert !validatedLengthsFromSource.get(lengthFromSource) : "We should never remove validated length states";
        lengthsFromSource.set(lengthFromSource, false);
    }

    public boolean validatedAtLength(int lengthFromSource) {
        return validatedLengthsFromSource.get(lengthFromSource);
    }

    @Override
    public void close() {
        sourceSignposts.close();
        if (targetSignposts != null) {
            targetSignposts.close();
        }
    }

    public long id() {
        return id;
    }

    public void addSourceSignpost(TwoWaySignpost sourceSignpost, int lengthFromSource) {
        Preconditions.checkArgument(
                sourceSignpost.forwardNode == this, "Source signpost must be added to correct node");

        dataManager.hooks.addSourceSignpost(sourceSignpost, lengthFromSource);
        if (!lengthsFromSource.get(lengthFromSource)) {
            // Never seen the node at this depth before
            lengthsFromSource.set(lengthFromSource);

            int minDistToTarget = minDistToTarget();
            if (minDistToTarget != TwoWaySignpost.NO_TARGET_DISTANCE) {
                Preconditions.checkState(
                        lengthFromSource > distanceFromSource,
                        "When we find a shortest path to a node we shouldn't have TargetSignposts");

                dataManager.schedulePropagation(this, lengthFromSource, minDistToTarget);
            }

            if (isTarget()) {
                dataManager.addTarget(this);
            }
        } else {
            assert lengthFromSource == lengthsFromSource.stream().max().orElseThrow()
                    : "A node should only be seen by the BFS at increasingly deeper levels.";
        }

        sourceSignposts.add(sourceSignpost);
    }

    public void newPropagatedLengthFromSource(int lengthFromSource, int lengthToTarget) {
        if (!lengthsFromSource.get(lengthFromSource)) {
            // Never seen the node at this depth before
            lengthsFromSource.set(lengthFromSource);

            if (hasMinDistToTarget(lengthToTarget)) {
                Preconditions.checkState(
                        lengthFromSource > distanceFromSource,
                        "When we find a shortest path to a node we shouldn't have TargetSignposts");

                dataManager.schedulePropagation(this, lengthFromSource, lengthToTarget);
            }

            if (isTarget()) {
                dataManager.addTarget(this);
            }
        }
    }

    public void addTargetSignpost(TwoWaySignpost targetSignpost, int lengthToTarget) {
        this.dataManager.hooks.addTargetSignpost(targetSignpost, lengthToTarget);
        Preconditions.checkArgument(targetSignpost.prevNode == this, "Target signpost must be added to correct node");

        boolean firstTrace = false;
        if (targetSignposts == null) {
            targetSignposts = HeapTrackingArrayList.newArrayList(SIGNPOSTS_INIT_SIZE, dataManager.mt);
            firstTrace = true;
        }

        assert !firstTrace || lengthToTarget >= minDistToTarget()
                : "The first time a node is traced should be with the shortest trail to a target";

        if (!hasMinDistToTarget(lengthToTarget)) {
            // First time we find a trail to a target of length `lengthToTarget`

            int lengthFromSource = lengthsFromSource.nextSetBit(0);
            while (lengthFromSource != -1) {
                Preconditions.checkState(lengthsFromSource.get(lengthFromSource), "");

                // Register for propagation for validated non-shortest lengthStates if not shortestDistToATarget,
                // or all non-shortest lengthStates if shortestDistToATarget
                if ((firstTrace || validatedLengthsFromSource.get(lengthFromSource))
                        && lengthFromSource != distanceFromSource) {
                    dataManager.schedulePropagation(this, lengthFromSource, lengthToTarget);
                }
                lengthFromSource = lengthsFromSource.nextSetBit(lengthFromSource + 1);
            }
        }

        targetSignposts.add(targetSignpost);
    }

    public void propagateLengthPair(int lengthFromSource, int lengthToTarget) {
        dataManager.hooks.propagateLengthPair(this, lengthFromSource, lengthToTarget);

        if (!hasAnyMinDistToTarget()) {
            return;
        }

        if (targetSignposts != null) {
            for (TwoWaySignpost tsp : targetSignposts) {
                if (tsp.minDistToTarget() == lengthToTarget) {
                    tsp.propagate(lengthFromSource, lengthToTarget);
                }
            }
        }
    }

    public void validateLengthState(int lengthFromSource, int tracedLengthToTarget) {
        dataManager.hooks.validateLengthState(this, lengthFromSource, tracedLengthToTarget);

        Preconditions.checkState(
                !validatedLengthsFromSource.get(lengthFromSource),
                "Shouldn't validate the same length from source more than once");

        assert hasAnyMinDistToTarget() || (tracedLengthToTarget == 0 && isTarget())
                : "We only validate length states during tracing, and any traced node which isn't the target node of a "
                        + "path should've had a TargetSignpost registered in targetSignpostsByMinDist before being validated";

        assert (isTarget() && tracedLengthToTarget == 0) || tracedLengthToTarget == minDistToTarget()
                : "First time tracing should be with shortest length to target";

        validatedLengthsFromSource.set(lengthFromSource);
        // We don't want to register to propagate for the same length pair again
        if (targetSignposts != null) {
            for (TwoWaySignpost tsp : targetSignposts) {
                int lengthToTarget = tsp.minDistToTarget();
                if (lengthToTarget != TwoWaySignpost.NO_TARGET_DISTANCE) {
                    Preconditions.checkState(
                            lengthToTarget >= tracedLengthToTarget,
                            "First time tracing should be with shortest length to target");
                    if (lengthToTarget > tracedLengthToTarget) {
                        // We don't want to register to propagate for the same length pair again
                        dataManager.schedulePropagation(this, lengthFromSource, lengthToTarget);
                    }
                }
            }
        }
    }

    public void decrementTargetCount() {
        dataManager.hooks.decrementTargetCount(this, remainingTargetCount);

        remainingTargetCount--;
        Preconditions.checkState(remainingTargetCount >= 0, "Target count should never be negative");

        if (remainingTargetCount == 0) {
            this.dataManager.decrementLiveTargetCount();
        }
    }

    public boolean hasMinDistToTarget(int minDistToTarget) {
        if (targetSignposts == null) {
            return false;
        }
        for (TwoWaySignpost tsp : targetSignposts) {
            if (tsp.minDistToTarget() == minDistToTarget) {
                return true;
            }
        }
        return false;
    }

    public boolean hasAnyMinDistToTarget() {
        var res = targetSignposts != null;
        Preconditions.checkState(
                !res || targetSignposts.notEmpty(), "If targetSignposts isn't null it's never supposed to be empty");
        return res;
    }

    private int minDistToTarget() {
        if (targetSignposts == null) {
            return TwoWaySignpost.NO_TARGET_DISTANCE;
        }
        int min = Integer.MAX_VALUE;
        for (TwoWaySignpost tsp : targetSignposts) {
            int curr = tsp.minDistToTarget();
            if (curr != TwoWaySignpost.NO_TARGET_DISTANCE && curr < min) {
                min = curr;
            }
        }
        return min == Integer.MAX_VALUE ? TwoWaySignpost.NO_TARGET_DISTANCE : min;
    }

    @Override
    public String toString() {
        var stateName = String.valueOf(state.id());
        if (state.slotOrName() instanceof SlotOrName.VarName name) {
            stateName = name.name();
        }
        return "(" + id + "," + stateName + ')';
    }

    public int remainingTargetCount() {
        return remainingTargetCount;
    }

    private static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(NodeData.class);
    private static long BITSET_MIN_SIZE =
            HeapEstimator.shallowSizeOfInstance(BitSet.class) + HeapEstimator.sizeOfLongArray(1);

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + BITSET_MIN_SIZE + BITSET_MIN_SIZE;
    }
}
