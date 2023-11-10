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
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingIntArrayList;
import org.neo4j.common.EntityType;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * The PathTracer will use data produced by PGPathPropagatingBFS to return paths between the source and the target.
 * It runs a DFS from a given target through the produce path trace data back towards the source.
 *
 * The PathTracer is also responsible for doing bookkeeping related to PPBFS which is only possible to do during
 * path tracing. Therefore, it's very important to exhaust the path tracer for every target node.
 */
public final class PathTracer extends PrefetchingIterator<PathTracer.TracedPath> {
    private final PPBFSHooks hooks;
    private NodeData targetNode;
    private NodeData sourceNode;

    /** The length of the currently traced path when projected back to the data graph */
    private int dgLength;

    /** The current path of signposts from the target to the source */
    private final HeapTrackingArrayList<TwoWaySignpost> activeSignposts;

    /** The index of each signpost in activeSignposts, relative to its NodeData parent */
    private final HeapTrackingIntArrayList nodeSourceSignpostIndices;

    private final BitSet pgTrailToTarget;
    private final BitSet betweenDuplicateRels;
    private int currentDgLengthToTarget;

    /**
     * Because path tracing performs much of the bookkeeping of PPBFS, we may need to trace paths, even if we've
     * returned all of the paths that was requested to the current target. The {saturated}
     */
    private boolean saturated;

    private boolean shouldReturnSingleNodePath;

    public PathTracer(MemoryTracker memoryTracker, PPBFSHooks hooks) {
        this.activeSignposts = HeapTrackingArrayList.newArrayList(memoryTracker);
        this.nodeSourceSignpostIndices = HeapTrackingIntArrayList.newIntArrayList(memoryTracker);
        this.pgTrailToTarget = new BitSet();
        this.betweenDuplicateRels = new BitSet();
        this.hooks = hooks;
    }

    public void setSourceNode(NodeData sourceNode) {
        this.sourceNode = sourceNode;
    }

    public void resetWithNewTargetNodeAndDGLength(NodeData targetNode, int dgLength) {
        this.targetNode = targetNode;

        Preconditions.checkArgument(
                targetNode.remainingTargetCount() >= 0, "remainingTargetCount should not be decremented beyond 0");
        this.saturated = targetNode.remainingTargetCount() == 0;

        this.activeSignposts.clear();

        this.nodeSourceSignpostIndices.clear();
        this.nodeSourceSignpostIndices.add(-1);

        this.pgTrailToTarget.clear();
        this.pgTrailToTarget.set(0);

        this.betweenDuplicateRels.clear();

        this.dgLength = dgLength;
        this.currentDgLengthToTarget = 0;
        this.shouldReturnSingleNodePath = targetNode == sourceNode;
        super.reset();
    }

    private NodeData current() {
        return activeSignposts.isEmpty() ? targetNode : this.activeSignposts.last().prevNode;
    }

    private int currentIndex() {
        return this.nodeSourceSignpostIndices.last();
    }

    public void deactivateCurrent() {
        this.nodeSourceSignpostIndices.removeLast();
        if (this.activeSignposts.notEmpty()) {
            TwoWaySignpost currentSignpost = activeSignposts.removeLast();

            this.currentDgLengthToTarget -= currentSignpost.dataGraphLength();
            int dgLengthFromSource = this.dgLength - currentDgLengthToTarget;

            hooks.deactivatingSignpost(dgLengthFromSource, currentSignpost);
            currentSignpost.deActivate();
            if (!currentSignpost.isVerifiedAtLength(dgLengthFromSource)
                    && !this.betweenDuplicateRels.get(this.activeSignposts.size())) {
                NodeData nodeBeforeCurrent = activeSignposts.isEmpty()
                        ? targetNode
                        : activeSignposts.get(activeSignposts.size() - 1).prevNode;
                currentSignpost.pruneSourceLength(dgLengthFromSource);
                nodeBeforeCurrent.synchronizeLength(dgLengthFromSource);
            }
        }
    }

    void activate(NodeData current, int nextIndex) {
        var sourceSignpost = current.getSourceSignpost(nextIndex);
        this.activeSignposts.add(sourceSignpost);
        this.betweenDuplicateRels.set(this.activeSignposts.size() - 1, false);

        hooks.activatingSignpost(dgLength - currentDgLengthToTarget, sourceSignpost);

        this.currentDgLengthToTarget += sourceSignpost.dataGraphLength();
        this.nodeSourceSignpostIndices.set(this.nodeSourceSignpostIndices.size() - 1, nextIndex);
        this.nodeSourceSignpostIndices.add(-1);

        boolean isTargetPGTrail = pgTrailToTarget.get(this.activeSignposts.size() - 1) && !sourceSignpost.isActive();
        pgTrailToTarget.set(this.activeSignposts.size(), isTargetPGTrail);

        if (isTargetPGTrail && !sourceSignpost.hasBeenTraced()) {

            sourceSignpost.setMinDistToTarget(currentDgLengthToTarget);
        }
    }

    @Override
    protected TracedPath fetchNextOrNull() {
        if (shouldReturnSingleNodePath && !saturated) {
            shouldReturnSingleNodePath = false;
            return currentPath();
        }

        while (this.nodeSourceSignpostIndices.notEmpty()) {
            NodeData current = current();
            int currentIndex = currentIndex();
            int nextIndex = current.nextSignpostIndexForLength(currentIndex, this.dgLength - currentDgLengthToTarget);
            if (nextIndex == -1) {
                deactivateCurrent();
            } else {
                this.activate(current, nextIndex);
                TwoWaySignpost sourceSignpost = activeSignposts.last();

                // Possible optimisation:
                // instead of isActive(), use !pgTrailToTarget.get(currentIndex) however we would often find that the
                // signpost was not duplicated and possibly run allNodesAreValidatedBetweenDuplicates for no benefit
                // (would also need to update that method)
                if (sourceSignpost.isActive() && allNodesAreValidatedBetweenDuplicates()) {
                    hooks.skippingDuplicateRelationship(this.targetNode, this.activeSignposts);
                    hooks.deactivatingSignpost(dgLength - currentDgLengthToTarget, sourceSignpost);
                    this.activeSignposts.removeLast();
                    this.nodeSourceSignpostIndices.removeLast();
                    this.currentDgLengthToTarget -= sourceSignpost.dataGraphLength();
                } else {
                    sourceSignpost.activate();

                    // the order of these predicates is important since validTrail has side effects:
                    if (sourceSignpost.prevNode == sourceNode && validTrail() && !saturated) {
                        TracedPath path = currentPath();
                        hooks.returnPath(path);
                        return path;
                    }
                }
            }
        }
        return null;
    }

    private boolean allNodesAreValidatedBetweenDuplicates() {
        var lastSignpost = this.activeSignposts.last();
        int dgLengthFromSource = dgLength - currentDgLengthToTarget;

        if (!lastSignpost.prevNode.validatedAtLength(dgLengthFromSource)) {
            return false;
        }

        dgLengthFromSource += lastSignpost.dataGraphLength();
        for (int i = this.activeSignposts.size() - 2; i >= 0; i--) {
            var candidate = this.activeSignposts.get(i);

            if (!candidate.prevNode.validatedAtLength(dgLengthFromSource)) {
                return false;
            }

            if (candidate.dataGraphRelationshipEquals(lastSignpost)) {
                // i + 1 because the upper duplicate isn't between duplicates and shouldn't be protected from pruning
                this.betweenDuplicateRels.set(i + 1, this.activeSignposts.size() - 1, true);
                return true;
            }

            dgLengthFromSource += candidate.dataGraphLength();
        }

        throw new IllegalStateException("Expected duplicate relationship in SHORTEST trail validation");
    }

    private TracedPath currentPath() {
        var entities = new PathEntity[activeSignposts.size() + dgLength + 1];

        int index = entities.length - 1;
        entities[index--] = PathEntity.fromNode(targetNode);

        for (var signpost : this.activeSignposts) {
            if (signpost instanceof TwoWaySignpost.RelSignpost relSignpost) {
                entities[index--] = PathEntity.fromRel(relSignpost);
            }

            entities[index--] = PathEntity.fromNode(signpost.prevNode);
        }

        Preconditions.checkState(index == -1, "Traced path length was not as expected");

        return new TracedPath(entities);
    }

    private boolean validTrail() {
        int dgLengthFromSource = 0;
        for (int i = activeSignposts.size() - 1; i >= 0; i--) {
            TwoWaySignpost signpost = activeSignposts.get(i);
            dgLengthFromSource += signpost.dataGraphLength();
            for (int j = activeSignposts.size() - 1; j > i; j--) {
                if (signpost.dataGraphRelationshipEquals(activeSignposts.get(j))) {
                    hooks.invalidTrail(this::currentPath);
                    return false;
                }
            }
            if (!signpost.isVerifiedAtLength(dgLengthFromSource)) {
                signpost.addVerified(dgLengthFromSource);
                NodeData node = i == 0 ? targetNode : activeSignposts.get(i - 1).prevNode;
                if (!node.validatedAtLength(dgLengthFromSource)) {
                    node.validateLengthState(dgLengthFromSource, dgLength - dgLengthFromSource);
                }
            }
        }
        return true;
    }

    public NodeData targetNode() {
        return targetNode;
    }

    public void decrementTargetCount() {
        if (this.targetNode.decrementTargetCount()) {
            this.saturated = true;
        }
    }

    public record PathEntity(SlotOrName slotOrName, long id, EntityType entityType) {
        static PathEntity fromNode(NodeData node) {
            return new PathEntity(node.state().slotOrName(), node.id(), EntityType.NODE);
        }

        static PathEntity fromRel(TwoWaySignpost.RelSignpost signpost) {
            return new PathEntity(signpost.slotOrName(), signpost.relId, EntityType.RELATIONSHIP);
        }

        public AnyValue idValue() {
            return switch (entityType) {
                case NODE -> VirtualValues.node(id);
                case RELATIONSHIP -> VirtualValues.relationship(id);
            };
        }

        @Override
        public String toString() {
            return id + " | " + slotOrName;
        }
    }

    public record TracedPath(PathEntity[] entities) {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("(");
            for (var e : entities) {
                switch (e.entityType) {
                    case NODE -> sb.append(e);
                    case RELATIONSHIP -> sb.append(")-[").append(e).append("]-(");
                }
            }
            sb.append(")");

            return sb.toString();
        }
    }
}
