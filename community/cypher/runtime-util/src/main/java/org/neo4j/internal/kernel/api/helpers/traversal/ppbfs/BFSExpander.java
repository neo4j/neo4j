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

    // allocated once and reused per source nodeState
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

    /** discover a nodeState that has not been seen before */
    public void discover(NodeState node, TraversalDirection direction) {
        hooks.discover(node, direction);
        foundNodes.addToBuffer(node);
        node.discover(direction);

        var state = node.state();

        for (var nj : state.getNodeJuxtapositions(direction)) {
            if (nj.state(direction).test(node.id())) {
                switch (direction) {
                    case FORWARD -> {
                        var nextNode = encounter(node.id(), nj.targetState(), direction);
                        var signpost =
                                TwoWaySignpost.fromNodeJuxtaposition(mt, node, nextNode, foundNodes.forwardDepth());
                        if (globalState.searchMode == SearchMode.Unidirectional
                                || !nextNode.hasSourceSignpost(signpost)) {
                            nextNode.addSourceSignpost(signpost, foundNodes.forwardDepth());
                        }
                    }

                    case BACKWARD -> {
                        var nextNode = encounter(node.id(), nj.sourceState(), direction);
                        var signpost = TwoWaySignpost.fromNodeJuxtaposition(mt, nextNode, node);

                        if (!nextNode.hasTargetSignpost(signpost)) {
                            var addedSignpost = node.upsertSourceSignpost(signpost);
                            addedSignpost.setMinTargetDistance(
                                    foundNodes.backwardDepth(), PGPathPropagatingBFS.Phase.Expansion);
                        }
                    }
                }
            }
        }
    }

    /** encounter a nodeState that may or may not have been seen before */
    public NodeState encounter(long nodeId, State state, TraversalDirection direction) {
        var nodeState = foundNodes.get(nodeId, state.id());

        if (nodeState == null) {
            nodeState = new NodeState(globalState, nodeId, state, intoTarget);
            discover(nodeState, direction);
        } else if (globalState.searchMode == SearchMode.Bidirectional && !nodeState.hasBeenSeen(direction)) {
            // this branch means we continue expanding in both directions past the opposite frontier, if the node has
            // not been previously seen by *this* direction
            discover(nodeState, direction);
        }

        return nodeState;
    }

    public void expand() {
        foundNodes.openBuffer();

        var direction = foundNodes.getNextExpansionDirection();
        hooks.expand(direction, foundNodes);

        for (var pair : foundNodes.frontier(direction).keyValuesView()) {
            var dbNodeId = pair.getOne();
            var statesById = pair.getTwo();

            statesList.clear();
            for (var nodeState : statesById) {
                if (nodeState != null) {
                    statesList.add(nodeState.state());
                }
            }

            hooks.expandNode(dbNodeId, statesList, direction);

            pgCursor.setNodeAndStates(dbNodeId, statesList, direction);
            while (pgCursor.next()) {
                long foundNode = pgCursor.otherNodeReference();
                var re = pgCursor.relationshipExpansion();

                switch (direction) {
                    case FORWARD -> {
                        var nextNode = encounter(foundNode, re.targetState(), direction);
                        var node = statesById.get(re.sourceState().id());

                        var signpost = TwoWaySignpost.fromRelExpansion(
                                mt, node, pgCursor.relationshipReference(), nextNode, re, foundNodes.forwardDepth());

                        if (globalState.searchMode == SearchMode.Unidirectional
                                || !nextNode.hasSourceSignpost(signpost)) {
                            nextNode.addSourceSignpost(signpost, foundNodes.forwardDepth());
                        }
                    }

                    case BACKWARD -> {
                        var nextNode = encounter(foundNode, re.sourceState(), direction);
                        var node = statesById.get(re.targetState().id());

                        var signpost = TwoWaySignpost.fromRelExpansion(
                                mt, nextNode, pgCursor.relationshipReference(), node, re);

                        if (!nextNode.hasTargetSignpost(signpost)) {
                            var addedSignpost = node.upsertSourceSignpost(signpost);
                            addedSignpost.setMinTargetDistance(
                                    foundNodes.backwardDepth(), PGPathPropagatingBFS.Phase.Expansion);
                        }
                    }
                }
                ;
            }
        }

        foundNodes.commitBuffer(direction);
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
