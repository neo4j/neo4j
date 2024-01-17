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

import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingIntObjectHashMap;
import org.neo4j.collection.trackable.HeapTrackingUnifiedSet;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * This class is heavily coupled with {@link PGPathPropagatingBFS} and has three responsibilities:
 * <p>
 * 1. Keep track of all the data needed to execute the PPBFS algorithm
 * <br>
 * 2. Run propagation
 * <br>
 * 3. Purge TargetSignposts which only lead to saturated target nodes
 * <br>
 * <p>
 * The `DataManager` holds:
 * <p>
 * 1. All nodes and their corresponding {@link NodeData}s.
 * <br>
 * 2. The next and current level of the BFS (which are partitions of the nodeDatas field).
 * <br>
 * 3. The targets at the current BFS expansion depth.
 * <br>
 * 4. The nodes which are registered to propagate.
 */
final class DataManager implements AutoCloseable {

    private final HeapTrackingNodeDatas nodeDatas;
    private final PGPathPropagatingBFS ppbfs;
    private final HeapTrackingArrayList<NodeData> targets;

    // Possible optimisation: Try to turn nodesToPropagateAtLengthPair into a queue and check for set deduplication
    //                        rather than Map<Set<>>
    private final HeapTrackingIntObjectHashMap<HeapTrackingIntObjectHashMap<HeapTrackingUnifiedSet<NodeData>>>
            nodesToPropagate; // Indexed with (lengthFromSource + lengthToTarget, lengthFromSource)
    final MemoryTracker mt;
    final PPBFSHooks hooks;
    final long initialCountForTargetNodes;
    private int liveTargets = 0;

    /**
     * @param initialCountForTargetNodes Initial countdown value for each target node.
     *                                   This is 'K', both when we have SHORTEST K and SHORTEST K GROUPS. The operators
     *                                   are responsible for decrementing it in alignment with whatever semantics are
     *                                   relevant.
     * @param numberOfNFAStates          Width of the NFA state array for each datagraph node
     */
    public DataManager(
            MemoryTracker memoryTracker,
            PPBFSHooks hooks,
            PGPathPropagatingBFS ppbfs,
            int initialCountForTargetNodes,
            int numberOfNFAStates) {
        this.ppbfs = ppbfs;
        this.mt = memoryTracker;
        this.hooks = hooks;
        this.nodeDatas = new HeapTrackingNodeDatas(memoryTracker, numberOfNFAStates);
        this.nodesToPropagate = HeapTrackingIntObjectHashMap.createIntObjectHashMap(memoryTracker);
        this.targets = HeapTrackingArrayList.newArrayList(memoryTracker);
        this.initialCountForTargetNodes = initialCountForTargetNodes;
    }

    public void incrementLiveTargetCount() {
        this.liveTargets += 1;
    }

    public void decrementLiveTargetCount() {
        this.liveTargets -= 1;
        Preconditions.checkState(this.liveTargets >= 0, "Live target count should never be negative");
    }

    public boolean hasLiveTargets() {
        return this.liveTargets > 0;
    }

    public void addToNextLevel(NodeData node) {
        nodeDatas.addToNextLevel(node);
    }

    public void allocateNextLevel() {
        nodeDatas.allocateNextLevel();
    }

    @Override
    public void close() {
        nodeDatas.close();
        nodesToPropagate.forEach(map -> {
            map.forEach(HeapTrackingUnifiedSet::close);
            map.close();
        });
        nodesToPropagate.close();
        targets.close();
    }

    public HeapTrackingNodeDatas nodeDatas() {
        return nodeDatas;
    }

    public NodeData getNodeData(long nodeId, int stateId) {
        return nodeDatas.get(nodeId, stateId);
    }

    public void schedulePropagation(NodeData nodeData, int lengthFromSource, int lengthToTarget) {
        hooks.schedulePropagation(nodeData, lengthFromSource, lengthToTarget);

        nodesToPropagate
                .getIfAbsentPut(
                        lengthFromSource + lengthToTarget,
                        () -> HeapTrackingIntObjectHashMap.createIntObjectHashMap(mt))
                .getIfAbsentPut(lengthFromSource, () -> HeapTrackingUnifiedSet.createUnifiedSet(mt))
                .add(nodeData);
    }

    /**
     * Propagates all path data that results in paths of length {@param totalLength}. To understand how it works,
     * read the PPBFS under trail semantics section in the PPBFS guide:
     * https://neo4j.atlassian.net/wiki/spaces/CYPHER/pages/180977665/Shortest+K+Implementation
     *
     * @param totalLength
     */
    public void propagateAll(int totalLength) {
        hooks.propagateAll(this.nodesToPropagate, totalLength);

        var nodesToPropagateForLength = nodesToPropagate.get(totalLength);
        if (nodesToPropagateForLength == null) {
            return;
        }

        int minLengthFromSourceToPropagate =
                nodesToPropagateForLength.keysView().min();

        for (int lengthFromSource = minLengthFromSourceToPropagate;
                lengthFromSource <= totalLength;
                lengthFromSource++) {
            int lengthToTarget = totalLength - lengthFromSource;

            hooks.propagateAllAtLengths(lengthFromSource, lengthToTarget);

            HeapTrackingUnifiedSet<NodeData> nodesToPropagateAtLengthPair =
                    nodesToPropagateForLength.get(lengthFromSource);

            if (nodesToPropagateAtLengthPair != null) {
                while (nodesToPropagateAtLengthPair.notEmpty()) {
                    NodeData node = nodesToPropagateAtLengthPair.getLast();
                    nodesToPropagateAtLengthPair.remove(node);
                    node.propagateLengthPair(lengthFromSource, lengthToTarget);
                }

                nodesToPropagateForLength.remove(lengthFromSource);

                nodesToPropagateAtLengthPair.close();
            }
        }

        nodesToPropagate.remove(totalLength).close();

        hooks.finishedPropagation(targets);
    }

    public boolean hasNodesToPropagateOrExpand() {
        assert nodesToPropagate.isEmpty() || nodesToPropagate.keysView().min() >= ppbfs.nextDepth()
                : "The current implementation is structured such that we never should schedule nodes to propagate for a "
                        + "depth which has already passed. If we do (as the algo is implemented here), we will loop for ever.";
        return nodeDatas.currentLevelHasNodes() || nodesToPropagate.notEmpty();
    }

    public void addTarget(NodeData nodeData) {
        Preconditions.checkArgument(nodeData.isTarget(), "Node must be a target");
        Preconditions.checkState(
                !targets.contains(nodeData),
                "Caller is responsible for adding any node as a target at most once per level");
        targets.add(nodeData);
    }

    public HeapTrackingArrayList<NodeData> targets() {
        return targets;
    }

    public boolean hasTargetsWithRemainingCount() {
        for (var t : targets) {
            if (t.remainingTargetCount() > 0) {
                return true;
            }
        }
        return false;
    }

    public boolean hasNodesToExpand() {
        return nodeDatas.currentLevelHasNodes();
    }

    public boolean hasTargets() {
        return targets.notEmpty();
    }

    public void clearTargets() {
        targets.clear();
    }
}
