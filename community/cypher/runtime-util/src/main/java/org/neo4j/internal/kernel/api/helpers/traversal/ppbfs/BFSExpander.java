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
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State;
import org.neo4j.memory.MemoryTracker;

final class BFSExpander implements AutoCloseable {
    private final MemoryTracker mt;
    private final PPBFSHooks hooks;
    private final DataManager dataManager;
    private final ProductGraphTraversalCursor pgCursor;
    private final long intoTarget;

    // allocated once and reused per source node;
    // max capacity should be total number of states in nfa if we had that info here,
    // but it will grow to the required size soon enough
    private final HeapTrackingArrayList<State> statesList;

    public BFSExpander(
            DataManager dataManager,
            ProductGraphTraversalCursor pgCursor,
            long intoTarget,
            PPBFSHooks hooks,
            MemoryTracker mt) {
        this.mt = mt;
        this.hooks = hooks;
        this.dataManager = dataManager;
        this.pgCursor = pgCursor;
        this.intoTarget = intoTarget;
        this.statesList = HeapTrackingArrayList.newArrayList(2, mt);
    }

    public void floodInitialNodeJuxtapositions() {
        this.floodNodeJuxtapositions(0);
    }

    private void floodNodeJuxtapositions(int depthOfNextLevel) {
        // nb: level is growing while we iterate over it but that is what we want
        dataManager.nodeDatas().forEachNodeInNextLevel(currentNode -> {
            var state = currentNode.state();
            for (var nj : state.getNodeJuxtapositions()) {
                if (nj.testNode(currentNode.id())) {
                    NodeData nextNode = dataManager.getNodeData(
                            currentNode.id(), nj.targetState().id());

                    if (nextNode == null) { // Only add unseen nodes to next level
                        nextNode = new NodeData(
                                mt, currentNode.id(), nj.targetState(), depthOfNextLevel, dataManager, intoTarget);
                        dataManager.addToNextLevel(nextNode);
                    }

                    nextNode.addSourceSignpost(
                            TwoWaySignpost.fromNodeJuxtaposition(mt, currentNode, nextNode, depthOfNextLevel),
                            depthOfNextLevel);
                }
            }
        });
    }

    public void expandLevel(int depthOfNextLevel) {
        for (var pair : dataManager.nodeDatas().getCurrentLevelDGDatas().keyValuesView()) {
            var dgNodeId = pair.getOne();
            var pgNodeDatas = pair.getTwo();

            statesList.clear();
            for (NodeData pgNodeInStateOrNull : pgNodeDatas) {
                if (pgNodeInStateOrNull != null) {
                    statesList.add(pgNodeInStateOrNull.state());
                }
            }

            pgCursor.setNodeAndStates(dgNodeId, statesList);
            while (pgCursor.next()) {
                long foundNode = pgCursor.otherNodeReference();
                NodeData nextNode = dataManager.getNodeData(
                        foundNode, pgCursor.targetState().id());

                if (nextNode == null) {
                    nextNode = new NodeData(
                            mt, foundNode, pgCursor.targetState(), depthOfNextLevel, dataManager, intoTarget);

                    dataManager.addToNextLevel(nextNode); // Only add unseen nodes to next level
                }

                NodeData currentNode =
                        pgNodeDatas.get(pgCursor.currentInputState().id());

                TwoWaySignpost signpost = TwoWaySignpost.fromRelExpansion(
                        mt,
                        currentNode,
                        pgCursor.relationshipReference(),
                        nextNode,
                        pgCursor.relationshipExpansion(),
                        depthOfNextLevel);
                nextNode.addSourceSignpost(signpost, depthOfNextLevel);
            }
        }

        floodNodeJuxtapositions(depthOfNextLevel);
    }

    public void setTracer(KernelReadTracer tracer) {
        pgCursor.setTracer(tracer);
    }

    @Override
    public void close() throws Exception {
        // dataManager is not owned by this class; it should be closed by the consumer
        this.pgCursor.close();
        this.statesList.close();
    }
}
