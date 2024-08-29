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
import org.neo4j.collection.trackable.HeapTrackingLongArrayList;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.MultiRelationshipExpansion;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State;
import org.neo4j.memory.MemoryTracker;

final class BFSExpander implements AutoCloseable {
    private final MemoryTracker mt;
    private final PPBFSHooks hooks;
    private final GlobalState globalState;
    private final ProductGraphTraversalCursor pgCursor;
    private final ProductGraphTraversalCursor.DataGraphRelationshipCursor relCursor;
    private final long intoTarget;

    // allocated once and reused per source nodeState
    private final HeapTrackingArrayList<State> statesList;
    private final FoundNodes foundNodes;

    public BFSExpander(
            FoundNodes foundNodes,
            GlobalState globalState,
            ProductGraphTraversalCursor pgCursor,
            ProductGraphTraversalCursor.DataGraphRelationshipCursor relCursor,
            long intoTarget,
            int nfaStateCount) {
        this.mt = globalState.mt;
        this.hooks = globalState.hooks;
        this.globalState = globalState;
        this.pgCursor = pgCursor;
        this.relCursor = relCursor;
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
            if (nj.endState(direction).test(node.id())) {
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

    private void multiHopDFS(NodeState startNode, MultiRelationshipExpansion expansion, TraversalDirection direction) {
        var rels = new long[expansion.length()];
        var nodes = new long[expansion.length() - 1];

        var nodeTree = new HeapTrackingLongArrayList[expansion.length() + 1];
        nodeTree[0] = HeapTrackingLongArrayList.newLongArrayList(1, mt);
        nodeTree[0].add(startNode.id());
        var relTree = new HeapTrackingLongArrayList[expansion.length()];

        int depth = 0;
        while (depth != -1) {
            assert depth <= expansion.length()
                    : "Multi-hop depth first search should never exceed total expansion length";
            if (nodeTree[depth] == null || nodeTree[depth].isEmpty()) {
                if (depth > 0) {
                    rels[direction.isBackward() ? (rels.length - depth) : depth - 1] = 0;

                    if (depth <= nodes.length) {
                        nodes[direction.isBackward() ? (nodes.length - depth) : depth - 1] = 0;
                    }
                }

                depth--;
            } else if (depth == expansion.length()) {
                var endNode = nodeTree[depth].removeLast();
                var rel = relTree[depth - 1].removeLast();
                rels[direction.isBackward() ? (rels.length - depth) : depth - 1] = rel;

                var nextNode = encounter(endNode, expansion.endState(direction), direction);

                switch (direction) {
                    case FORWARD -> {
                        var signpost = TwoWaySignpost.fromMultiRel(
                                mt,
                                startNode,
                                rels.clone(),
                                nodes.clone(),
                                expansion,
                                nextNode,
                                foundNodes.forwardDepth());
                        if (globalState.searchMode == SearchMode.Unidirectional
                                || !nextNode.hasSourceSignpost(signpost)) {
                            nextNode.addSourceSignpost(signpost, foundNodes.forwardDepth());
                        }
                    }
                    case BACKWARD -> {
                        var signpost = TwoWaySignpost.fromMultiRel(
                                mt, nextNode, rels.clone(), nodes.clone(), expansion, startNode);
                        if (!nextNode.hasTargetSignpost(signpost)) {
                            var addedSignpost = startNode.upsertSourceSignpost(signpost);
                            addedSignpost.setMinTargetDistance(
                                    foundNodes.backwardDepth(), PGPathPropagatingBFS.Phase.Expansion);
                        }
                    }
                }
            } else {
                var node = nodeTree[depth].removeLast();
                if (depth > 0) {
                    var rel = relTree[depth - 1].removeLast();
                    rels[direction.isBackward() ? (rels.length - depth) : depth - 1] = rel;

                    if (depth <= nodes.length) {
                        nodes[direction.isBackward() ? (nodes.length - depth) : depth - 1] = node;
                    }
                }

                var relHop = expansion.rel(depth, direction);
                var nodePredicate = expansion.nodePredicate(depth, direction);

                var sel = relHop.getSelection(direction);
                relCursor.setNode(node, sel);
                boolean canExpand = false;
                while (relCursor.nextRelationship()) {
                    if (relHop.predicate().test(relCursor) && nodePredicate.test(relCursor.otherNode())) {
                        // test for uniqueness
                        boolean isUnique = true;
                        switch (direction) {
                            case FORWARD -> {
                                for (int i = 0; i < depth && isUnique; i++) {
                                    isUnique = rels[i] != relCursor.relationshipReference();
                                }
                            }
                            case BACKWARD -> {
                                for (int i = rels.length - 1; i > rels.length - depth - 1 && isUnique; i--) {
                                    isUnique = rels[i] != relCursor.relationshipReference();
                                }
                            }
                        }

                        if (isUnique) {
                            if (nodeTree[depth + 1] == null) {
                                nodeTree[depth + 1] = HeapTrackingLongArrayList.newLongArrayList(mt);
                            }
                            nodeTree[depth + 1].add(relCursor.otherNode());

                            if (relTree[depth] == null) {
                                relTree[depth] = HeapTrackingLongArrayList.newLongArrayList(mt);
                            }
                            relTree[depth].add(relCursor.relationshipReference());
                            canExpand = true;
                        }
                    }
                }

                if (canExpand) {
                    depth++;
                }
            }
        }
    }

    public void expand() {
        foundNodes.openBuffer();
        var direction = foundNodes.getNextExpansionDirection();
        hooks.expand(direction, foundNodes);

        // look in the priority queue to see if there are any var-length transitions to expand at this depth.
        // if there are then run DFS on them and add the final node states to the collection
        for (var mre = foundNodes.dequeueScheduled(direction);
                mre != null;
                mre = foundNodes.dequeueScheduled(direction)) {
            multiHopDFS(mre.start(), mre.expansion(), direction);
        }

        for (var pair : foundNodes.frontier(direction).keyValuesView()) {
            var dbNodeId = pair.getOne();
            var statesById = pair.getTwo();

            statesList.clear();
            for (var nodeState : statesById) {
                if (nodeState != null) {
                    statesList.add(nodeState.state());

                    // iterate over any var-length transitions and add them to the appropriate priority queue at depth +
                    // length
                    for (var mre : nodeState.state().getMultiRelationshipExpansions(direction)) {
                        // here we subtract 1 to account for the initial source/target nodes being discovered at depth 0
                        var depth = foundNodes.depth(direction) - 1 + mre.length();
                        foundNodes.enqueueScheduled(depth, nodeState, mre, direction);
                    }
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
