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
import java.util.stream.Collectors;
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipExpansion;
import org.neo4j.util.Preconditions;

/**
 * Represents a relationship in the Product Graph with:
 * <ul>
 * <li> a set of path lengths from the source (Source Signpost)
 * <li> the minimum length to the target (Target Signpost)
 */
public abstract sealed class TwoWaySignpost {

    public static final int NO_TARGET_DISTANCE = -1;

    public final NodeData prevNode;
    public final NodeData forwardNode;

    // Source signpost
    protected final BitSet lengthsFromSource;
    protected final BitSet verifiedAtLengthFromSource;

    // targetSignpost
    protected int minDistToTarget = NO_TARGET_DISTANCE;

    public TwoWaySignpost(NodeData prevNode, NodeData forwardNode, int lengthFromSource) {
        this.prevNode = prevNode;
        this.forwardNode = forwardNode;
        this.lengthsFromSource = new BitSet();
        this.verifiedAtLengthFromSource = new BitSet();
        this.lengthsFromSource.set(lengthFromSource);
    }

    public static RelSignpost fromRelExpansion(
            NodeData prevNode,
            long relId,
            NodeData forwardNode,
            RelationshipExpansion relationshipExpansion,
            int lengthFromSource) {
        return new RelSignpost(prevNode, relId, forwardNode, relationshipExpansion, lengthFromSource);
    }

    public static NodeSignpost fromNodeJuxtaposition(NodeData prevNode, NodeData forwardNode, int lengthFromSource) {
        return new NodeSignpost(prevNode, forwardNode, lengthFromSource);
    }

    public abstract int dataGraphLength();

    /**
     * The "hasBeenTraced" mechanism is used to control how we register TargetSignposts. For reasons explained in the
     * PPBFS guide (https://neo4j.atlassian.net/wiki/spaces/CYPHER/pages/180977665/Shortest+K+Implementation),
     * we only want a given SourceSignpost to be contained in at most one TargetStep. This TargetSignpost is created
     * the first time the SourceSignpost is traced, and the hasBeenTraced mechanism is used to determine this.
     */
    public boolean hasBeenTraced() {
        return this.minDistToTarget != NO_TARGET_DISTANCE;
    }

    /**
     * See java doc comment for SourceSignpost.hasBeenTraced()
     */
    public void setMinDistToTarget(int dgDist) {
        Preconditions.checkState(
                !hasBeenTraced(), "A signpost should only have setMinDistToTarget() called upon it on the first trace");
        this.minDistToTarget = dgDist;
        this.prevNode.addTargetSignpost(this, dgDist);
    }

    public int minDistToTarget() {
        return this.minDistToTarget;
    }

    /**
     * The "active" mechanism is used during path tracing to simplify duplicate relationship checking. While we're
     * laying out a path during path tracing, we always start from the target and trace our way back to the source
     * incrementally. During this process, we're interested in knowing if the currently laid out subpath towards
     * the target is a trail in the product graph.
     * <p>
     * The "active" mechanism allows us to avoid a bunch of looping over the currently laid out subpath, or *active*
     * subpath, to determine if there are duplicated relationships. Instead, since it is the case that every
     * product graph relationships uniquely corresponds to a SourceSignpost instance, we can keep counters
     * in these signposts to determine if a given signpost is active, and if so, we know
     * that the product graph relationships is duplicated in the active subpath.
     * <p>
     * It's important to call deActivate() on the signposts when we remove them from the laid out subpath we're tracing.
     */
    public abstract boolean isActive();

    /**
     * See java doc comment for SourceSignpost.isActive()
     */
    public abstract void activate();

    /**
     * See java doc comment for SourceSignpost.isActive()
     */
    public abstract void deActivate();

    public void addSourceLength(int lengthFromSource) {
        this.lengthsFromSource.set(lengthFromSource);
    }

    public boolean hasSourceLength(int length) {
        return lengthsFromSource.get(length);
    }

    /**
     * Propagate the length pair up to the forward node and register the new source length with the forward node
     */
    public void propagate(int lengthFromSource, int lengthToTarget) {
        int newLength = lengthFromSource + dataGraphLength();
        forwardNode.newPropagatedLengthFromSource(newLength, lengthToTarget - dataGraphLength());
        this.addSourceLength(newLength);
    }

    public void pruneSourceLength(int lengthFromSource) {
        prevNode.dataManager.hooks.pruneSourceLength(this, lengthFromSource);
        this.lengthsFromSource.set(lengthFromSource, false);
    }

    public void setVerified(int lengthFromSource) {
        prevNode.dataManager.hooks.setVerified(this, lengthFromSource);
        this.verifiedAtLengthFromSource.set(lengthFromSource, true);
    }

    public boolean isVerifiedAtLength(int length) {
        return verifiedAtLengthFromSource.get(length);
    }

    public abstract boolean dataGraphRelationshipEquals(TwoWaySignpost other);

    /** A signpost that points across a relationship traversal */
    public static final class RelSignpost extends TwoWaySignpost {

        public final long relId;
        private final RelationshipExpansion relationshipExpansion;
        private int activations;

        private RelSignpost(
                NodeData prevNode,
                long relId,
                NodeData forwardNode,
                RelationshipExpansion relationshipExpansion,
                int lengthFromSource) {
            super(prevNode, forwardNode, lengthFromSource);
            this.relId = relId;
            this.relationshipExpansion = relationshipExpansion;
            this.activations = 0;
        }

        @Override
        public boolean isActive() {
            return activations > 0;
        }

        @Override
        public void activate() {
            activations += 1;
        }

        @Override
        public void deActivate() {
            Preconditions.checkArgument(activations > 0, "Signpost activations should never be negative");
            activations -= 1;
        }

        @Override
        public int dataGraphLength() {
            return 1;
        }

        @Override
        public boolean dataGraphRelationshipEquals(TwoWaySignpost other) {
            if (!(other instanceof RelSignpost otherRS)) {
                return false;
            }
            return relId == otherRS.relId;
        }

        public SlotOrName slotOrName() {
            return relationshipExpansion.slotOrName();
        }

        @Override
        public String toString() {
            var sb = new StringBuilder("RE ").append(prevNode).append("-[");

            if (slotOrName() != SlotOrName.none()) {
                sb.append(slotOrName()).append("@");
            }

            sb.append(relId).append("]->").append(forwardNode);

            if (minDistToTarget != NO_TARGET_DISTANCE) {
                sb.append(", minDistToTarget: ").append(minDistToTarget);
            }

            if (!lengthsFromSource.isEmpty()) {
                var lengths = lengthsFromSource.stream()
                        .mapToObj(i -> i + (verifiedAtLengthFromSource.get(i) ? "✓" : "?"))
                        .collect(Collectors.joining(",", "{", "}"));
                sb.append(", lengthsFromSource: ").append(lengths);
            }

            return sb.toString();
        }
    }

    /** A signpost that points across a node juxtaposition */
    public static final class NodeSignpost extends TwoWaySignpost {

        private NodeSignpost(NodeData prevNode, NodeData forwardNode, int lengthFromSource) {
            super(prevNode, forwardNode, lengthFromSource);
        }

        @Override
        public boolean isActive() {
            // Node juxtapositions may be duplicated, and we implement this by saying that they're never active
            return false;
        }

        @Override
        public void activate() {
            // Node juxtapositions may be duplicated, and we implement this by saying that they're never active
        }

        @Override
        public void deActivate() {
            // Node juxtapositions may be duplicated, and we implement this by saying that they're never active
        }

        @Override
        public int dataGraphLength() {
            return 0;
        }

        @Override
        public boolean dataGraphRelationshipEquals(TwoWaySignpost other) {
            // Node juxtapositions disappear after projection to the data graph, so they can never be the cause
            // of duplicated data graph relationships.
            return false;
        }

        @Override
        public String toString() {
            var sb = new StringBuilder("NJ ").append(prevNode).append(" ").append(forwardNode);

            if (minDistToTarget != NO_TARGET_DISTANCE) {
                sb.append(", minDistToTarget: ").append(minDistToTarget);
            }

            if (!lengthsFromSource.isEmpty()) {
                var lengths = lengthsFromSource.stream()
                        .mapToObj(i -> i + (verifiedAtLengthFromSource.get(i) ? "✓" : "?"))
                        .collect(Collectors.joining(",", "{", "}"));
                sb.append(", lengthsFromSource: ").append(lengths);
            }

            return sb.toString();
        }
    }
}
