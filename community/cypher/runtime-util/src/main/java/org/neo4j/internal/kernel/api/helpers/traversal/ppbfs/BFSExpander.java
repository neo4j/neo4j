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
    private final GlobalState globalState;
    private final ProductGraphTraversalCursor pgCursor;
    private final long intoTarget;

    // allocated once and reused per source node
    private final HeapTrackingArrayList<State> statesList;
    private final FoundNodes foundNodes;

    public BFSExpander(
            FoundNodes foundNodes,
            GlobalState globalState,
            ProductGraphTraversalCursor pgCursor,
            long intoTarget,
            int nfaStateCount) {
        this.mt = globalState.mt;
        this.hooks = globalState.hooks;
        this.globalState = globalState;
        this.pgCursor = pgCursor;
        this.intoTarget = intoTarget;
        this.statesList = HeapTrackingArrayList.newArrayList(nfaStateCount, mt);
        this.foundNodes = foundNodes;
    }

    /** discover a node that has not been seen before */
    public void discover(NodeState node) {
        foundNodes.addToBuffer(node);

        var state = node.state();
        for (var nj : state.getNodeJuxtapositions()) {
            if (nj.testNode(node.id())) {
                var nextNode = encounter(node.id(), nj.targetState());

                nextNode.addSourceSignpost(
                        TwoWaySignpost.fromNodeJuxtaposition(mt, node, nextNode, foundNodes.depth()),
                        foundNodes.depth());
            }
        }
    }

    /** encounter a node that may or may not have been seen before */
    private NodeState encounter(long nodeId, State state) {
        var nextNode = foundNodes.get(nodeId, state.id());

        if (nextNode == null) {
            nextNode = new NodeState(globalState, nodeId, state, intoTarget);
            discover(nextNode);
        }

        return nextNode;
    }

    public void expand() {
        for (var pair : foundNodes.frontier().keyValuesView()) {
            var dbNodeId = pair.getOne();
            var statesById = pair.getTwo();

            statesList.clear();
            for (var nodeState : statesById) {
                if (nodeState != null) {
                    statesList.add(nodeState.state());
                }
            }

            pgCursor.setNodeAndStates(dbNodeId, statesList);
            while (pgCursor.next()) {
                long foundNode = pgCursor.otherNodeReference();
                var nextNode = encounter(foundNode, pgCursor.targetState());

                var currentNode = statesById.get(pgCursor.currentInputState().id());

                var signpost = TwoWaySignpost.fromRelExpansion(
                        mt,
                        currentNode,
                        pgCursor.relationshipReference(),
                        nextNode,
                        pgCursor.relationshipExpansion(),
                        foundNodes.depth());
                nextNode.addSourceSignpost(signpost, foundNodes.depth());
            }
        }

        foundNodes.shuffleFrontiers();
    }

    public void setTracer(KernelReadTracer tracer) {
        pgCursor.setTracer(tracer);
    }

    @Override
    public void close() throws Exception {
        // globalState is not owned by this class; it should be closed by the consumer
        pgCursor.close();
        statesList.close();
    }
}
