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

import java.util.Arrays;
import java.util.Objects;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.MultiRelationshipExpansion;
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
    public final Lengths lengths;

    // targetSignpost
    protected int minTargetDistance = NO_TARGET_DISTANCE;

    // The source length assigned during BFS expansion (-1 if none).
    // Used to identify the BFS-discovered length so we can preserve the node's reachability
    // when pruning (see pruneSourceLength).
    private int bfsSourceLength = -1;

    protected TwoWaySignpost(NodeState prevNode, NodeState forwardNode, Lengths lengths) {
        this.prevNode = prevNode;
        this.forwardNode = forwardNode;
        this.lengths = lengths;
    }

    protected TwoWaySignpost(NodeState prevNode, NodeState forwardNode, int sourceLength, Lengths lengths) {
        this(prevNode, forwardNode, lengths);
        this.lengths.markAsSeen(sourceLength);
        this.bfsSourceLength = sourceLength;
    }

    public static RelSignpost fromRelExpansion(
            MemoryTracker mt,
            NodeState prevNode,
            long relId,
            NodeState forwardNode,
            RelationshipExpansion relationshipExpansion,
            int sourceLength,
            Lengths lengths) {
        return allocate(
                mt, new RelSignpost(prevNode, relId, forwardNode, relationshipExpansion, sourceLength, lengths));
    }

    public static RelSignpost fromRelExpansion(
            MemoryTracker mt,
            NodeState prevNode,
            long relId,
            NodeState forwardNode,
            RelationshipExpansion relationshipExpansion,
            Lengths lengths) {
        return allocate(mt, new RelSignpost(prevNode, relId, forwardNode, relationshipExpansion, lengths));
    }

    public static NodeSignpost fromNodeJuxtaposition(
            MemoryTracker mt, NodeState prevNode, NodeState forwardNode, int sourceLength, Lengths lengths) {
        return allocate(mt, new NodeSignpost(prevNode, forwardNode, sourceLength, lengths));
    }

    public static NodeSignpost fromNodeJuxtaposition(
            MemoryTracker mt, NodeState prevNode, NodeState forwardNode, Lengths lengths) {
        return allocate(mt, new NodeSignpost(prevNode, forwardNode, lengths));
    }

    public static MultiRelSignpost fromMultiRel(
            MemoryTracker mt,
            NodeState prevNode,
            long[] rels,
            long[] nodes,
            MultiRelationshipExpansion expansion,
            NodeState forwardNode,
            Lengths lengths) {
        return allocate(mt, new MultiRelSignpost(prevNode, rels, nodes, forwardNode, expansion, lengths));
    }

    public static MultiRelSignpost fromMultiRel(
            MemoryTracker mt,
            NodeState prevNode,
            long[] rels,
            long[] nodes,
            MultiRelationshipExpansion expansion,
            NodeState forwardNode,
            int sourceLength,
            Lengths lengths) {
        return allocate(mt, new MultiRelSignpost(prevNode, rels, nodes, forwardNode, expansion, sourceLength, lengths));
    }

    private static <T extends TwoWaySignpost> T allocate(MemoryTracker mt, T signpost) {
        mt.allocateHeap(signpost.estimatedHeapUsage());
        return signpost;
    }

    /** The number of entities this signpost represents for tracing: prevNode, plus any interior relationship/nodes */
    public abstract int entityCount();

    public abstract int dataGraphLength();

    /** Write the entities of this signpost out to a row */
    public abstract void materialize(PathWriter pathWriter);

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
        this.lengths.markAsSeen(sourceLength);
        prevNode.globalState.hooks.addSourceLength(this, sourceLength);
    }

    public boolean seenAt(int sourceLength) {
        return lengths.seenAt(sourceLength);
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
        this.lengths.clearSeen(sourceLength);
        // In trail mode, when pruning the BFS-discovered source length, preserve the node's
        // reachability by skipping synchronizeLengthAfterPrune. The signpost loses the length
        // (tracer won't retry at the BFS length), but the node keeps it so propagation can
        // create longer source lengths at downstream signposts. Combined with unconditional
        // setMinTargetDistance in PathTracer, this allows valid trails to be found at deeper
        // depths via propagated (longer) source lengths.
        if (lengths.isWalkMode() || sourceLength != bfsSourceLength) {
            this.forwardNode.synchronizeLengthAfterPrune(sourceLength);
        }
    }

    public void validate(int sourceLength) {
        prevNode.globalState.hooks.setValidated(this, sourceLength);
        this.lengths.markAsValidated(sourceLength);
    }

    public boolean isValidatedAtLength(int sourceLength) {
        return lengths.validatedAt(sourceLength);
    }

    /** A signpost that points across a relationship traversal */
    public static final class RelSignpost extends TwoWaySignpost {

        public final long relId;
        public final RelationshipExpansion relationshipExpansion;

        private RelSignpost(
                NodeState prevNode,
                long relId,
                NodeState forwardNode,
                RelationshipExpansion relationshipExpansion,
                int lengthFromSource,
                Lengths lengths) {
            super(prevNode, forwardNode, lengthFromSource, lengths);
            this.relId = relId;
            this.relationshipExpansion = relationshipExpansion;
        }

        private RelSignpost(
                NodeState prevNode,
                long relId,
                NodeState forwardNode,
                RelationshipExpansion relationshipExpansion,
                Lengths lengths) {
            super(prevNode, forwardNode, lengths);
            this.relId = relId;
            this.relationshipExpansion = relationshipExpansion;
        }

        @Override
        public void materialize(PathWriter pathWriter) {
            pathWriter.writeNode(prevNode.state().slotOrName(), prevNode.id());
            pathWriter.writeRel(relationshipExpansion.slotOrName(), relId);
        }

        @Override
        public int entityCount() {
            return 2; // prevNode and rel
        }

        @Override
        public int dataGraphLength() {
            return 1;
        }

        @Override
        public String toString() {
            var sb = new StringBuilder("RE ").append(prevNode).append("-[");

            if (relationshipExpansion.slotOrName() != SlotOrName.none()) {
                sb.append(relationshipExpansion.slotOrName()).append("@");
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
            return SHALLOW_SIZE + lengths.estimatedHeapUsage();
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

        private NodeSignpost(NodeState prevNode, NodeState forwardNode, int lengthFromSource, Lengths lengths) {
            super(prevNode, forwardNode, lengthFromSource, lengths);
            assert prevNode != forwardNode : "A state cannot have a node juxtaposition to itself";
        }

        @Override
        public void materialize(PathWriter pathWriter) {
            pathWriter.writeNode(prevNode.state().slotOrName(), prevNode.id());
        }

        @Override
        public int entityCount() {
            return 1; // prevNode
        }

        private NodeSignpost(NodeState prevNode, NodeState forwardNode, Lengths lengths) {
            super(prevNode, forwardNode, lengths);
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
            return SHALLOW_SIZE + lengths.estimatedHeapUsage();
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

    public static final class MultiRelSignpost extends TwoWaySignpost {

        public final long[] rels;
        public final long[] nodes;
        public final MultiRelationshipExpansion transition;

        private MultiRelSignpost(
                NodeState prevNode,
                long[] rels,
                long[] nodes,
                NodeState forwardNode,
                MultiRelationshipExpansion transition,
                int lengthFromSource,
                Lengths lengths) {
            super(prevNode, forwardNode, lengthFromSource, lengths);
            this.rels = rels;
            this.nodes = nodes;
            this.transition = transition;

            assert rels.length == transition.rels().length;
            assert nodes.length == transition.nodes().length;
        }

        @Override
        public void materialize(PathWriter pathWriter) {
            pathWriter.writeNode(prevNode.state().slotOrName(), prevNode.id());

            for (int i = 0; i < nodes.length; i++) {
                var rel = rels[i];
                var relSlot = transition.rels()[i].slotOrName();
                pathWriter.writeRel(relSlot, rel);

                var node = nodes[i];
                var nodeSlot = transition.nodes()[i].slotOrName();
                pathWriter.writeNode(nodeSlot, node);
            }

            var lastRel = rels[rels.length - 1];
            var lastSlot = transition.rels()[transition.rels().length - 1].slotOrName();

            pathWriter.writeRel(lastSlot, lastRel);
        }

        private MultiRelSignpost(
                NodeState prevNode,
                long[] rels,
                long[] nodes,
                NodeState forwardNode,
                MultiRelationshipExpansion transition,
                Lengths lengths) {
            super(prevNode, forwardNode, lengths);
            this.rels = rels;
            this.nodes = nodes;
            this.transition = transition;

            assert rels.length == transition.rels().length;
            assert nodes.length == transition.nodes().length;
        }

        @Override
        public long estimatedHeapUsage() {
            return SHALLOW_SIZE
                    + lengths.estimatedHeapUsage()
                    + HeapEstimator.sizeOf(rels)
                    + HeapEstimator.sizeOf(nodes);
        }

        private static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(MultiRelSignpost.class);

        @Override
        public int entityCount() {
            return 1 + rels.length + nodes.length;
        }

        @Override
        public int dataGraphLength() {
            return transition.length();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MultiRelSignpost that = (MultiRelSignpost) o;
            // we can skip comparing interior nodes because those are implied by the relationships
            return prevNode == that.prevNode && forwardNode == that.forwardNode && Arrays.equals(rels, that.rels);
        }

        @Override
        public int hashCode() {
            return Objects.hash(prevNode, forwardNode, Arrays.hashCode(rels));
        }

        @Override
        public String toString() {
            var sb = new StringBuilder("MRE ").append(prevNode);
            if (transition.rels()[0].direction() == Direction.INCOMING) {
                sb.append("<");
            }
            sb.append("-[");

            for (int i = 0; i < transition.length(); i++) {
                var rel = transition.rels()[i];
                if (rel.slotOrName() != SlotOrName.none()) {
                    sb.append(rel.slotOrName()).append("@");
                }

                sb.append(rels[i]).append("]-");
                if (rel.direction() == Direction.OUTGOING) {
                    sb.append(">");
                }

                if (i < nodes.length) {
                    sb.append("(");
                    var node = transition.nodes()[i];
                    if (node.slotOrName() != SlotOrName.none()) {
                        sb.append(node.slotOrName()).append("@");
                    }
                    sb.append(nodes[i]).append(")");
                    if (transition.rels()[i + 1].direction() == Direction.INCOMING) {
                        sb.append("<");
                    }
                    sb.append("-[");
                }
            }

            sb.append(forwardNode);

            if (minTargetDistance != NO_TARGET_DISTANCE) {
                sb.append(", minTargetDistance: ").append(minTargetDistance);
            }

            var sourceLengths = lengths.renderSourceLengths();
            if (!sourceLengths.isEmpty()) {
                sb.append(", sourceLengths: ").append(sourceLengths);
            }

            return sb.toString();
        }
    }
}
