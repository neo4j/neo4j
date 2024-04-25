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
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.memory.MemoryTracker;

/**
 * This class holds all of the (node,state) pairs that are found during PGPathPropagatingBFS evaluation.
 * <p>
 * It is organised first by datagraph node id, and then by NFA state id, so that all of the states for a node can be
 * retrieved and supplied to the product graph cursor at once.
 *
 *
 * <pre>
 * We store all the nodes we've seen level by level. We do this so that we can have access to both the current and
 * next levels, without duplicating data, or reallocating collections due to .grow calls.
 *
 * To enable us to group nodes by their data graph id, we keep NodeStates in something similar to a two dimensional
 * hash map. For example, to get the node (nodeId=2, stateId=3) from the currentLevel, we'd call
 * currentLevel.get(2).get(3). The fact that stateId's are sequential allows us to let the type of currentLevel
 * be HeapTrackingLongObjectHashMap<HeapTrackingArrayList<NodeState>>, so currentLevel.get(2) returns an array list,
 * where the NodeState corresponding to stateId=3 is stored at index 3. This may lead to over allocation and sparse
 * arrays for certain NFA's, so we may want to revise this in the future if benchmarks tell us to.
 *
 * We keep all of our nodes in three disjoint collections, all of them adhere to the same indexing scheme as
 * explained above for currentLevel:
 *
 *  1) history. This is an array list of (nodeId, stateId) -> nodeState maps which store all the levels we've
 *     previously seen. So previousLevels.get(3) stores the nodes which were discovered in the previous level
 *  2) frontier. This is a map with (nodeId, stateId) -> nodeState for the current level
 *  3) frontierBuffer. This is a map with (nodeId, stateId) -> nodeState for the next level
 *
 *  So for example, if we're currently expanding level 3, to find nodes at distance 4 from the source,
 *  our data would look like
 *
 *  ┌───────────────────────────┐
 *  │          history          │
 *  │ ┌─────┐  ┌─────┐  ┌─────┐ │  ┌──────────────┐  ┌────────────┐
 *  │ │  0  │  │  1  │  │  2  │ │  │ 3 (frontier) │  │ 4 (buffer) │
 *  │ └─────┘  └─────┘  └─────┘ │  └──────────────┘  └────────────┘
 *  └───────────────────────────┘
 *
 * Keeping our nodeStates batched by level like this allows us to avoid rehashing and reallocating the whole
 * collection when we need to grow it, we only ever rehash/reallocate the buffer as it grows.
 *
 * A downside of this design is that looking up a node is linear w.r.t the depth of the bfs.
 * </pre>
 */
public final class FoundNodes implements AutoCloseable {
    private final HeapTrackingArrayList<HeapTrackingLongObjectHashMap<HeapTrackingArrayList<NodeState>>>
            history; // levelDepth x nodeId x stateId -> NodeState

    private HeapTrackingLongObjectHashMap<HeapTrackingArrayList<NodeState>> frontier; // nodeId x stateId -> NodeState

    private HeapTrackingLongObjectHashMap<HeapTrackingArrayList<NodeState>>
            frontierBuffer; // nodeId x stateId -> NodeState

    private final MemoryTracker memoryTracker;
    private final int nfaStateCount;

    public FoundNodes(MemoryTracker memoryTracker, int nfaStateCount) {
        this.memoryTracker = memoryTracker.getScopedMemoryTracker();
        this.history = HeapTrackingArrayList.newArrayList(this.memoryTracker);
        this.frontier = HeapTrackingLongObjectHashMap.createLongObjectHashMap(this.memoryTracker);
        this.frontierBuffer = HeapTrackingLongObjectHashMap.createLongObjectHashMap(this.memoryTracker);
        this.nfaStateCount = nfaStateCount;
    }

    public void addToBuffer(NodeState nodeState) {
        var nodeStates = frontierBuffer.get(nodeState.id());
        if (nodeStates == null) {
            nodeStates = HeapTrackingArrayList.newEmptyArrayList(nfaStateCount, memoryTracker);
            frontierBuffer.put(nodeState.id(), nodeStates);
        }
        nodeStates.set(nodeState.state().id(), nodeState);
    }

    /** Look up a NodeState. O(N) wrt history length */
    public NodeState get(long nodeId, int stateId) {
        var nodeStates = frontierBuffer.get(nodeId);
        if (nodeStates != null) {
            var nodeState = nodeStates.get(stateId);
            if (nodeState != null) {
                return nodeState;
            }
        }
        nodeStates = frontier.get(nodeId);
        if (nodeStates != null) {
            var nodeState = nodeStates.get(stateId);
            if (nodeState != null) {
                return nodeState;
            }
        }
        for (int i = history.size() - 1; i >= 0; i--) {
            nodeStates = history.get(i).get(nodeId);
            if (nodeStates != null) {
                var nodeState = nodeStates.get(stateId);
                if (nodeState != null) {
                    return nodeState;
                }
            }
        }
        return null;
    }

    public void shuffleFrontiers() {
        history.add(frontier);
        frontier = frontierBuffer;
        frontierBuffer = HeapTrackingLongObjectHashMap.createLongObjectHashMap(this.memoryTracker, frontier.size());
    }

    public HeapTrackingLongObjectHashMap<HeapTrackingArrayList<NodeState>> frontier() {
        return frontier;
    }

    public boolean hasMore() {
        return frontier.notEmpty() || frontierBuffer.notEmpty();
    }

    @Override
    public void close() {
        // we don't need to iterate & close the inner collections because we can just close the scoped memory tracker
        this.memoryTracker.close();
    }

    public int depth() {
        return this.history.size();
    }
}
