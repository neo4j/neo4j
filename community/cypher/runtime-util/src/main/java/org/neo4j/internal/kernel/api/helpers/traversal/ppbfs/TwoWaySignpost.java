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

import java.util.Objects;
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipExpansion;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.Measurable;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * Represents a relationship in the Product Graph with:
 * <ul>
 * <li> a set of path lengths from the source (Source Signpost)
 * <li> the minimum sourceLength to the target (Target Signpost)
 */
public abstract sealed class TwoWaySignpost implements Measurable {

    public static final int NO_TARGET_DISTANCE = -1;

    public final NodeState prevNode;
    public final NodeState forwardNode;

    // Source signpost
    protected final Lengths lengths;

    // targetSignpost
    protected int minTargetDistance = NO_TARGET_DISTANCE;

    protected TwoWaySignpost(MemoryTracker mt, NodeState prevNode, NodeState forwardNode) {
        this.prevNode = prevNode;
        this.forwardNode = forwardNode;
        this.lengths = new Lengths();
        mt.allocateHeap(estimatedHeapUsage());
    }

    protected TwoWaySignpost(MemoryTracker mt, NodeState prevNode, NodeState forwardNode, int sourceLength) {
        this(mt, prevNode, forwardNode);
        this.lengths.set(sourceLength, Lengths.Type.Source);
    }

    public static RelSignpost fromRelExpansion(
            MemoryTracker mt,
            NodeState prevNode,
            long relId,
            NodeState forwardNode,
            RelationshipExpansion relationshipExpansion,
            int sourceLength) {
        return new RelSignpost(mt, prevNode, relId, forwardNode, relationshipExpansion, sourceLength);
    }

    public static RelSignpost fromRelExpansion(
            MemoryTracker mt,
            NodeState prevNode,
            long relId,
            NodeState forwardNode,
            RelationshipExpansion relationshipExpansion) {
        return new RelSignpost(mt, prevNode, relId, forwardNode, relationshipExpansion);
    }

    public static NodeSignpost fromNodeJuxtaposition(
            MemoryTracker mt, NodeState prevNode, NodeState forwardNode, int sourceLength) {
        return new NodeSignpost(mt, prevNode, forwardNode, sourceLength);
    }

    public static NodeSignpost fromNodeJuxtaposition(MemoryTracker mt, NodeState prevNode, NodeState forwardNode) {
        return new NodeSignpost(mt, prevNode, forwardNode);
    }

    public abstract int dataGraphLength();

    /**
     * The "hasBeenTraced" mechanism is used to control how we register TargetSignposts. For reasons explained in the
     * PPBFS guide (https://neo4j.atlassian.net/wiki/spaces/CYPHER/pages/180977665/Shortest+K+Implementation),
     * we only want a given SourceSignpost to be contained in at most one TargetStep. This TargetSignpost is created
     * the first time the SourceSignpost is traced, and the hasBeenTraced mechanism is used to determine this.
     */
    public boolean hasBeenTraced() {
        return this.minTargetDistance != NO_TARGET_DISTANCE;
    }

    public void setMinTargetDistance(int distance, PGPathPropagatingBFS.Phase phase) {
        Preconditions.checkState(
                this.minTargetDistance == NO_TARGET_DISTANCE,
                "A signpost should only have setMinDistToTarget() called upon it on the first trace");
        this.minTargetDistance = distance;
        this.prevNode.addTargetSignpost(this, distance, phase);
    }

    public int minTargetDistance() {
        return this.minTargetDistance;
    }

    public void addSourceLength(int sourceLength) {
        this.lengths.set(sourceLength, Lengths.Type.Source);
        prevNode.globalState.hooks.addSourceLength(this, sourceLength);
    }

    public boolean hasSourceLength(int sourceLength) {
        return lengths.get(sourceLength, Lengths.Type.Source);
    }

    /**
     * Propagate the sourceLength pair up to the forward node and register the new source sourceLength with the forward node
     */
    public void propagate(int sourceLength, int targetLength) {
        int newLength = sourceLength + dataGraphLength();
        forwardNode.newPropagatedSourceLength(newLength, targetLength - dataGraphLength());
        this.addSourceLength(newLength);
    }

    public void pruneSourceLength(int sourceLength) {
        prevNode.globalState.hooks.pruneSourceLength(this, sourceLength);
        this.lengths.clear(sourceLength, Lengths.Type.Source);
        this.forwardNode.synchronizeLengthAfterPrune(sourceLength);
    }

    public void setVerified(int sourceLength) {
        prevNode.globalState.hooks.setVerified(this, sourceLength);
        this.lengths.set(sourceLength, Lengths.Type.ConfirmedSource);
    }

    public boolean isVerifiedAtLength(int sourceLength) {
        return lengths.get(sourceLength, Lengths.Type.ConfirmedSource);
    }

    /** A signpost that points across a relationship traversal */
    public static final class RelSignpost extends TwoWaySignpost {

        public final long relId;
        public final RelationshipExpansion relationshipExpansion;

        private RelSignpost(
                MemoryTracker mt,
                NodeState prevNode,
                long relId,
                NodeState forwardNode,
                RelationshipExpansion relationshipExpansion,
                int lengthFromSource) {
            super(mt, prevNode, forwardNode, lengthFromSource);
            this.relId = relId;
            this.relationshipExpansion = relationshipExpansion;
        }

        private RelSignpost(
                MemoryTracker mt,
                NodeState prevNode,
                long relId,
                NodeState forwardNode,
                RelationshipExpansion relationshipExpansion) {
            super(mt, prevNode, forwardNode);
            this.relId = relId;
            this.relationshipExpansion = relationshipExpansion;
        }

        @Override
        public int dataGraphLength() {
            return 1;
        }

        private SlotOrName slotOrName() {
            return relationshipExpansion.slotOrName();
        }

        @Override
        public String toString() {
            var sb = new StringBuilder("RE ").append(prevNode).append("-[");

            if (slotOrName() != SlotOrName.none()) {
                sb.append(slotOrName()).append("@");
            }

            sb.append(relId).append("]->").append(forwardNode);

            if (minTargetDistance != NO_TARGET_DISTANCE) {
                sb.append(", minTargetDistance: ").append(minTargetDistance);
            }

            var sourceLengths = lengths.renderSourceLengths();
            if (!sourceLengths.isEmpty()) {
                sb.append(", sourceLengths: ").append(sourceLengths);
            }

            return sb.toString();
        }

        private static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(RelSignpost.class);

        @Override
        public long estimatedHeapUsage() {
            return SHALLOW_SIZE + Lengths.SHALLOW_SIZE;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RelSignpost that = (RelSignpost) o;
            return prevNode == that.prevNode && forwardNode == that.forwardNode && relId == that.relId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(prevNode, forwardNode, relId);
        }
    }

    /** A signpost that points across a node juxtaposition */
    public static final class NodeSignpost extends TwoWaySignpost {

        private NodeSignpost(MemoryTracker mt, NodeState prevNode, NodeState forwardNode, int lengthFromSource) {
            super(mt, prevNode, forwardNode, lengthFromSource);
            assert prevNode != forwardNode : "A state cannot have a node juxtaposition to itself";
        }

        private NodeSignpost(MemoryTracker mt, NodeState prevNode, NodeState forwardNode) {
            super(mt, prevNode, forwardNode);
            assert prevNode != forwardNode : "A state cannot have a node juxtaposition to itself";
        }

        @Override
        public int dataGraphLength() {
            return 0;
        }

        @Override
        public String toString() {
            var sb = new StringBuilder("NJ ").append(prevNode).append(" ").append(forwardNode);

            if (minTargetDistance != NO_TARGET_DISTANCE) {
                sb.append(", minTargetDistance: ").append(minTargetDistance);
            }

            var sourceLengths = lengths.renderSourceLengths();
            if (!sourceLengths.isEmpty()) {
                sb.append(", sourceLengths: ").append(sourceLengths);
            }

            return sb.toString();
        }

        private static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(NodeSignpost.class);

        @Override
        public long estimatedHeapUsage() {
            return SHALLOW_SIZE + Lengths.SHALLOW_SIZE;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeSignpost that = (NodeSignpost) o;
            return prevNode == that.prevNode && forwardNode == that.forwardNode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(prevNode, forwardNode);
        }
    }
}
