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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks;

import java.util.function.Supplier;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingIntObjectHashMap;
import org.neo4j.collection.trackable.HeapTrackingUnifiedSet;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeData;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.NodeJuxtaposition;
import org.neo4j.values.virtual.VirtualNodeValue;

/**
 * Provides a way to inspect the progress of the algorithm for the purposes of logging and testing.
 * <p>
 * The production environment should use {@link NullPPBFSHooks#instance}
 */
public abstract class PPBFSHooks {
    private static PPBFSHooks current;

    public static PPBFSHooks getInstance() {
        if (current == null) {
            current = NullPPBFSHooks.instance;
        }
        return current;
    }

    public static void setInstance(PPBFSHooks hooks) {
        current = hooks;
    }

    // NodeData
    public void foundTargetAtNewLength(NodeData nodeData, int lengthFromSource) {}

    public void addSourceSignpost(NodeData nodeData, TwoWaySignpost signpost, int lengthFromSource) {}

    public void addPropagatedSignpost(
            NodeData nodeData, TwoWaySignpost signpost, int lengthFromSource, int lengthToTarget) {}

    public void addTargetSignpost(NodeData nodeData, TwoWaySignpost signpost, int lengthToTarget) {}

    public void propagateLengthPair(NodeData nodeData, int lengthFromSource, int lengthToTarget) {}

    public void propagateAtLengthPair(int lengthFromSource, int lengthToTarget) {}

    public void validateLengthState(NodeData nodeData, int lengthFromSource, int tracedLengthToTarget) {}

    // PathTraceTree
    public void tracingPathsOfLength(int length) {}

    public void returnPath(PathTracer.TracedPath tracedPath) {}

    public void invalidTrail(Supplier<PathTracer.TracedPath> getTracedPath) {}

    // PGPathPropagatingBFS
    public void nextLevel(int currentDepth) {}

    public void zeroHopLevel() {}

    public void noMoreNodes() {}

    // DataManager
    public void registerNodeToPropagate(NodeData nodeData, int lengthFromSource, int lengthToTarget) {}

    public void beginPropagation(
            HeapTrackingIntObjectHashMap<HeapTrackingIntObjectHashMap<HeapTrackingUnifiedSet<NodeData>>>
                    nodesToPropagate) {}

    public void propagateToLength(int totalLength) {}

    public void propagateToLength(int totalLength, int minLengthFromSourceToPropagate) {}

    public void propagatingNode(NodeData node, HeapTrackingUnifiedSet<NodeData> nodesToPropagateAtLengthPair) {}

    public void newRow(VirtualNodeValue node) {}

    public void finishedPropagation(HeapTrackingArrayList<NodeData> targets) {}

    public void expandLevel(int currentDepth) {}

    public void foundTargets() {}

    public void tracingTarget(NodeData target) {}

    public void traverseNodeJuxtaposition(NodeJuxtaposition nj) {}

    public void newNodeData(NodeData nodeData) {}

    public void activatingSignpost(int currentLength, TwoWaySignpost child) {}

    public void deactivatingSignpost(int currentLength, TwoWaySignpost last) {}

    public void decrementTargetCount(NodeData nodeData, int remainingTargetCount) {}

    public void skippingDuplicateRelationship(NodeData target, HeapTrackingArrayList<TwoWaySignpost> activePath) {}

    public void removeLengthFromSignpost(TwoWaySignpost sourceSignpost, int lengthFromSource) {}

    public void addVerifiedToSourceSignpost(TwoWaySignpost sourceSignpost, int lengthFromSource) {}
}
