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

import org.eclipse.collections.api.block.procedure.Procedure;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.memory.MemoryTracker;

/**
 * This class holds all of the (node,state) pairs that are found during PGPathPropagatingBFS evaluation.
 *
 * It allows the consumer to iterate over the 'Current' level, and add to iterate over the 'Next' level.
 * Importantly it permits comodification so that nodes can be added to the level while iterating it, which is used to
 * flood node juxtapositions.
 *
 * It is organised first by datagraph node id, and then by NFA state id, so that all of the states for a node can be
 * retrieved and supplied to the product graph cursor at once.
 */
final class HeapTrackingNodeDatas implements AutoCloseable {

    /**
     * <pre>
     * We store all the nodes we've seen level by level. We do this so that we can have access to both the current and
     * next levels, without duplicating data, or reallocating collections due to .grow calls.
     *
     * To enable us to group nodes by their data graph id, we keep nodeDatas in something similar to a two dimensional
     * hash map. For example, to get the node (nodeId=2, stateId=3) from the currentLevel, we'd call
     * currentLevel.get(2).get(3). The fact that stateId's are sequential allows us to let the type of currentLevel
     * be HeapTrackingLongObjectHashMap<HeapTrackingArrayList<NodeData>>, so currentLevel.get(2) returns an array list,
     * where the NodeData corresponding to stateId=3 is stored at index 3. This may lead to over allocation and sparse
     * arrays for certain NFA's, so we may want to revise this in the future if benchmarks tell us to.
     *
     * We keep all of our nodes in three disjoint collections, all of them adhere to the same indexing scheme as
     * explained above for currentLevel:
     *
     *  1) previousLevels. This is an array list of (nodeId, stateId) -> nodeData maps which store all the levels we've
     *  previously seen. So previousLevels.get(3) stores the nodes which were discovered in the previous level
     *
     *  2) currentLevel. This is just the map with (nodeId, stateId) -> nodeData for the current level
     *
     *  3) nextLevel. This is a map with (nodeId, stateId) -> nodeData for the next level. We also maintain an
     *  arraylist of nodes in the next level so that we can iterate through it in insertion order during node
     *  juxtaposition flooding.
     *
     *  So for example, if we're currently expanding level 3, to find nodes at distance 4 from the source,
     *  our data would look like
     *
     *  ┌───────────────────────────┐
     *  │         previous          │
     *  │ ┌─────┐  ┌─────┐  ┌─────┐ │  ┌─────────────┐  ┌──────────┐
     *  │ │  0  │  │  1  │  │  2  │ │  │ 3 (current) │  │ 4 (next) │
     *  │ └─────┘  └─────┘  └─────┘ │  └─────────────┘  └──────────┘
     *  └───────────────────────────┘
     *
     * Keeping our nodeDatas batched by level like this allow us to avoid rehashing and reallocating the whole
     * collection when we need to grow it, we only ever rehash/reallocate nextLevel.
     *
     * A downside of this design is that looking up a node is linear w.r.t the depth of the bfs.
     * </pre>
     */
    private final HeapTrackingArrayList<HeapTrackingLongObjectHashMap<HeapTrackingArrayList<NodeData>>>
            previousLevels; // levelDepth x nodeId x stateId -> NodeData

    private HeapTrackingLongObjectHashMap<HeapTrackingArrayList<NodeData>> currentLevel; // nodeId x stateId -> NodeData

    private HeapTrackingLongObjectHashMap<HeapTrackingArrayList<NodeData>> nextLevel; // nodeId x stateId -> NodeData

    private final HeapTrackingArrayList<HeapTrackingArrayList<NodeData>> nextLevelQueue;

    private final MemoryTracker memoryTracker;
    private final int numberOfNFAStates;

    public HeapTrackingNodeDatas(MemoryTracker memoryTracker, int numberOfNFAStates) {
        this.memoryTracker = memoryTracker.getScopedMemoryTracker();
        this.previousLevels = HeapTrackingArrayList.newArrayList(this.memoryTracker);
        this.currentLevel = HeapTrackingLongObjectHashMap.createLongObjectHashMap(this.memoryTracker);
        this.nextLevel = HeapTrackingLongObjectHashMap.createLongObjectHashMap(this.memoryTracker);
        this.nextLevelQueue = HeapTrackingArrayList.newArrayList(this.memoryTracker);
        this.numberOfNFAStates = numberOfNFAStates;
    }

    public void addToNextLevel(NodeData nodeData) {
        var dgNodeDatas = nextLevel.get(nodeData.id());
        if (dgNodeDatas == null) {
            dgNodeDatas = HeapTrackingArrayList.newEmptyArrayList(numberOfNFAStates, memoryTracker);
            nextLevel.put(nodeData.id(), dgNodeDatas);
            nextLevelQueue.add(dgNodeDatas);
        }
        dgNodeDatas.set(nodeData.state().id(), nodeData);
    }

    public NodeData get(long nodeId, int stateId) {
        var dgNodeDatas = nextLevel.get(nodeId);
        if (dgNodeDatas != null) {
            NodeData nodeData = dgNodeDatas.get(stateId);
            if (nodeData != null) {
                return nodeData;
            }
        }
        dgNodeDatas = currentLevel.get(nodeId);
        if (dgNodeDatas != null) {
            NodeData nodeData = dgNodeDatas.get(stateId);
            if (nodeData != null) {
                return nodeData;
            }
        }
        for (int i = previousLevels.size() - 1; i >= 0; i--) {
            dgNodeDatas = previousLevels.get(i).get(nodeId);
            if (dgNodeDatas != null) {
                NodeData nodeData = dgNodeDatas.get(stateId);
                if (nodeData != null) {
                    return nodeData;
                }
            }
        }
        return null;
    }

    public void allocateNextLevel() {
        previousLevels.add(currentLevel);
        currentLevel = nextLevel;
        nextLevel = HeapTrackingLongObjectHashMap.createLongObjectHashMap(this.memoryTracker, currentLevel.size());
        nextLevelQueue.clear();
    }

    public void forEachNodeInNextLevel(Procedure<? super NodeData> p) {
        for (int i = 0; i < nextLevelQueue.size(); i++) {
            var dgNodeDatas = nextLevelQueue.get(i);

            for (int j = 0; j < numberOfNFAStates; j++) {
                NodeData nodeData = dgNodeDatas.get(j);
                if (nodeData != null) {
                    p.accept(nodeData);
                }
            }
        }
    }

    public HeapTrackingLongObjectHashMap<HeapTrackingArrayList<NodeData>> getCurrentLevelDGDatas() {
        return currentLevel;
    }

    public boolean currentLevelHasNodes() {
        return currentLevel.notEmpty();
    }

    @Override
    public void close() {
        // we don't need to iterate & close the inner collections because we can just close the scoped memory tracker
        this.memoryTracker.close();
    }
}
