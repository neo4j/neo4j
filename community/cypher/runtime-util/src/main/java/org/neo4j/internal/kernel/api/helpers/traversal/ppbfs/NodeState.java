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

import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.Measurable;
import org.neo4j.util.Preconditions;

/**
 * A NodeState stores all algorithm-related bookkeeping for a given product (pair) of data graph node and NFA state.
 * Put differently, it is an incarnation of the nodes in a product graph, with attached PPBFS metadata.
 *
 * NodeState within an execution of the PPBFS algorithm should be unique for a given (node id, state id) pair; this
 * assumption is relied upon in several places. This is the purpose of {@link FoundNodes}: to act as a repository
 * for all product graph nodes.
 */
public final class NodeState implements AutoCloseable, Measurable {
    private static final int NO_SOURCE_DISTANCE = -1;

    private static final int SIGNPOSTS_INIT_SIZE = 2;

    private final long nodeId;
    private final State state;

    // public so that it is accessible by TwoWaySignpost
    final GlobalState globalState;

    private final HeapTrackingArrayList<TwoWaySignpost> sourceSignposts;
    private HeapTrackingArrayList<TwoWaySignpost> targetSignposts;

    private final Lengths lengths;

    // The length of the shortest path in the data graph from the source node to this node which is accepted by the NFA.
    // This is not necessarily a trail length, the corresponding path may have repeated relationships.
    private int sourceDistance = NO_SOURCE_DISTANCE;

    // This is initialised to K when we run both SHORTEST K and SHORTEST K GROUPS.
    // It is then decremented whenever we return a path (or group) ending with this node.
    // NB: this mechanism relies on the NFA having a single final state
    private int remainingTargetCount = 0;

    private boolean isTarget = false;

    private boolean discoveredForward = false;
    private boolean discoveredBackward = false;

    public NodeState(GlobalState globalState, long nodeId, State state, long intoTarget) {
        this.sourceSignposts = HeapTrackingArrayList.newArrayList(SIGNPOSTS_INIT_SIZE, globalState.mt);
        this.nodeId = nodeId;
        this.state = state;
        this.globalState = globalState;
        this.lengths = new Lengths();

        if (state().isFinalState() && (intoTarget == NO_SUCH_ENTITY || intoTarget == nodeId)) {
            this.remainingTargetCount = (int) globalState.initialCountForTargetNodes;
            this.isTarget = true;
            globalState.incrementUnsaturatedTargets();
        }

        globalState.mt.allocateHeap(estimatedHeapUsage());
    }

    public void discover(TraversalDirection direction) {
        switch (direction) {
            case FORWARD -> this.discoveredForward = true;
            case BACKWARD -> this.discoveredBackward = true;
        }
    }

    public State state() {
        return state;
    }

    public boolean isTarget() {
        return isTarget;
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

        for (var sourceSignpost : sourceSignposts) {
            if (sourceSignpost.hasSourceLength(lengthFromSource)) {
                return;
            }
        }

        assert !lengths.get(lengthFromSource, Lengths.Type.ConfirmedSource)
                : "We should never remove validated length states";
        lengths.clear(lengthFromSource, Lengths.Type.Source);
    }

    public boolean validatedAtLength(int lengthFromSource) {
        return lengths.get(lengthFromSource, Lengths.Type.ConfirmedSource);
    }

    @Override
    public void close() {
        sourceSignposts.close();
        if (targetSignposts != null) {
            targetSignposts.close();
        }
    }

    public long id() {
        return nodeId;
    }

    public void addSourceSignpost(TwoWaySignpost sourceSignpost, int sourceLength) {
        Preconditions.checkArgument(
                sourceSignpost.forwardNode == this, "Source signpost must be added to correct node");

        assert sourceSignposts.stream().noneMatch(sourceSignpost::equals) : "Duplicate source signpost added";

        if (sourceDistance == NO_SOURCE_DISTANCE || sourceDistance > sourceLength) {
            sourceDistance = sourceLength;
        }

        globalState.hooks.addSourceSignpost(sourceSignpost, sourceLength);
        if (sourceLength != -1) {
            if (!lengths.get(sourceLength, Lengths.Type.Source)) {
                // Never seen the node at this depth before
                lengths.set(sourceLength, Lengths.Type.Source);

                int minTargetDistance = minTargetDistance();
                if (minTargetDistance != TwoWaySignpost.NO_TARGET_DISTANCE
                        && sourceLength + minTargetDistance >= globalState.depth()) {
                    globalState.schedule(
                            this, sourceLength, minTargetDistance, GlobalState.ScheduleSource.SourceSignpost);
                }

                if (isTarget()) {
                    globalState.addTarget(this);
                }
            } else {
                assert sourceLength == lengths.max(Lengths.Type.Source)
                        : "A node should only be seen by the BFS at increasingly deeper levels.";
            }
        }

        sourceSignposts.add(sourceSignpost);
    }

    public void newPropagatedSourceLength(int sourceLength, int targetLength) {
        if (!lengths.get(sourceLength, Lengths.Type.Source)) {
            // Never seen the node at this depth before
            lengths.set(sourceLength, Lengths.Type.Source);

            if (hasMinDistToTarget(targetLength)) {
                globalState.schedule(this, sourceLength, targetLength, GlobalState.ScheduleSource.Propagated);
            }

            if (isTarget()) {
                globalState.addTarget(this);
            }
        }
    }

    public void addTargetSignpost(TwoWaySignpost targetSignpost, int targetLength, PGPathPropagatingBFS.Phase phase) {
        globalState.hooks.addTargetSignpost(targetSignpost, targetLength);
        Preconditions.checkArgument(targetSignpost.prevNode == this, "Target signpost must be added to correct node");

        assert targetSignposts == null || targetSignposts.stream().noneMatch(targetSignpost::equals)
                : "Duplicate target signpost added";

        boolean firstTrace = false;
        if (targetSignposts == null) {
            targetSignposts = HeapTrackingArrayList.newArrayList(SIGNPOSTS_INIT_SIZE, globalState.mt);
            firstTrace = true;
        }

        assert !firstTrace || targetLength >= minTargetDistance()
                : "The first time a node is traced should be with the shortest trail to a target";

        if (!hasMinDistToTarget(targetLength)) {
            // First time we find a trail to a target of length `targetLength`

            for (int l = lengths.next(0, Lengths.Type.Source); l != -1; l = lengths.next(l + 1, Lengths.Type.Source)) {

                // Register for propagation for validated non-shortest lengthStates if not shortestDistToATarget,
                // or all non-shortest lengthStates if shortestDistToATarget

                if ((firstTrace || lengths.get(l, Lengths.Type.ConfirmedSource))
                        && (l > sourceDistance || phase == PGPathPropagatingBFS.Phase.Expansion)) {

                    var depth = globalState.depth();
                    if (phase == PGPathPropagatingBFS.Phase.Tracing) {
                        depth += 1;
                    }
                    if (l + targetLength >= depth) {
                        globalState.schedule(this, l, targetLength, GlobalState.ScheduleSource.TargetSignpost);
                    }
                }
            }
        }

        targetSignposts.add(targetSignpost);
    }

    private int realSourceDistance() {
        if (sourceDistance == NO_SOURCE_DISTANCE) {
            return 0;
        }
        return sourceDistance;
    }

    public void propagateLengthPair(int lengthFromSource, int lengthToTarget) {
        globalState.hooks.propagateLengthPair(this, lengthFromSource, lengthToTarget);

        if (!hasAnyMinDistToTarget()) {
            return;
        }

        for (TwoWaySignpost tsp : targetSignposts) {
            if (tsp.minTargetDistance() == lengthToTarget) {
                tsp.propagate(lengthFromSource, lengthToTarget);
            }
        }
    }

    public void validateSourceLength(int lengthFromSource, int tracedLengthToTarget) {
        globalState.hooks.validateSourceLength(this, lengthFromSource, tracedLengthToTarget);

        Preconditions.checkState(
                !lengths.get(lengthFromSource, Lengths.Type.ConfirmedSource),
                "Shouldn't validate the same length from source more than once");

        lengths.set(lengthFromSource, Lengths.Type.ConfirmedSource);
        if (!hasAnyMinDistToTarget()) {
            return;
        }

        for (TwoWaySignpost tsp : targetSignposts) {
            int lengthToTarget = tsp.minTargetDistance();
            if (lengthToTarget != TwoWaySignpost.NO_TARGET_DISTANCE) {
                Preconditions.checkState(
                        lengthToTarget >= tracedLengthToTarget,
                        "First time tracing should be with shortest length to target");

                // We don't want to register to propagate for the same length pair again
                if (lengthToTarget > tracedLengthToTarget) {
                    globalState.schedule(this, lengthFromSource, lengthToTarget, GlobalState.ScheduleSource.Validation);
                }
            }
        }
    }

    public void decrementTargetCount() {
        globalState.hooks.decrementTargetCount(this, remainingTargetCount);

        remainingTargetCount--;
        Preconditions.checkState(remainingTargetCount >= 0, "Target count should never be negative");

        if (remainingTargetCount == 0) {
            globalState.decrementUnsaturatedTargets();
        }
    }

    public boolean hasBeenSeen(TraversalDirection direction) {
        return direction == TraversalDirection.FORWARD && discoveredForward
                || direction == TraversalDirection.BACKWARD && discoveredBackward;
    }

    public boolean hasSourceSignpost(TwoWaySignpost signpost) {
        for (TwoWaySignpost tsp : sourceSignposts) {
            if (tsp.equals(signpost)) {
                return true;
            }
        }

        return false;
    }

    public boolean hasTargetSignpost(TwoWaySignpost signpost) {
        if (targetSignposts == null) {
            return false;
        }

        for (TwoWaySignpost tsp : targetSignposts) {
            if (tsp.equals(signpost)) {
                return true;
            }
        }

        return false;
    }

    public boolean hasMinDistToTarget(int minDistToTarget) {
        if (targetSignposts == null) {
            return false;
        }
        for (TwoWaySignpost tsp : targetSignposts) {
            if (tsp.minTargetDistance() == minDistToTarget) {
                return true;
            }
        }
        return false;
    }

    private boolean hasAnyMinDistToTarget() {
        var res = targetSignposts != null;
        Preconditions.checkState(
                !res || targetSignposts.notEmpty(), "If targetSignposts isn't null it's never supposed to be empty");
        return res;
    }

    private int minTargetDistance() {
        if (targetSignposts == null) {
            return TwoWaySignpost.NO_TARGET_DISTANCE;
        }
        int min = Integer.MAX_VALUE;
        for (TwoWaySignpost tsp : targetSignposts) {
            int curr = tsp.minTargetDistance();
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
        return "(" + nodeId + "," + stateName + ')';
    }

    public boolean isSaturated() {
        return remainingTargetCount == 0;
    }

    private static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(NodeState.class);

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + Lengths.SHALLOW_SIZE;
    }

    public <T extends TwoWaySignpost> T upsertSourceSignpost(T signpost) {
        for (var existing : sourceSignposts) {
            if (signpost.equals(existing)) {
                return (T) existing;
            }
        }
        addSourceSignpost(signpost, -1);
        return signpost;
    }
}
