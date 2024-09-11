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

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_ENTITY;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * This is the root of the product graph PPBFS algorithm. To learn more about how the algorithm works, read the PPBFS guide:
 * https://neo4j.atlassian.net/wiki/spaces/CYPHER/pages/180977665/Shortest+K+Implementation
 */
public final class PGPathPropagatingBFS<Row> extends PrefetchingIterator<Row> implements AutoCloseable {
    // dependencies
    private final GlobalState globalState;
    private final BFSExpander bfsExpander;
    private final NodeState sourceNodeState;
    private final long intoTarget;
    private final State finalState;
    private final SearchMode searchMode;
    private final PathTracer<Row> pathTracer;
    private final Function<SignpostStack, Row> toRow;
    private final Predicate<Row> nonInlinedPredicate;
    private final Boolean isGroupSelector;
    private final int maxDepth;
    private final MemoryTracker memoryTracker;
    private final PPBFSHooks hooks;
    private final AssertOpen assertOpen;
    private final Propagator propagator;
    private final FoundNodes foundNodes;
    private final TargetTracker targets;

    // iteration state
    private Iterator<NodeState> currentTargets = Collections.emptyIterator();
    private boolean targetSaturated = false;
    private boolean groupYielded = false;

    /**
     * Creates a new PathPropagatingBFS.
     *
     * @param source              The id of the starting node.
     * @param startState          The initial state of the NFA generated from the QPP
     * @param pathTracer          A PathTracer instance that will be reused for each new target node & length
     * @param toRow               a function converting a traced path to a row in the relevant runtime
     * @param nonInlinedPredicate the non inlined predicate, executed on the output row
     * @param isGroupSelector     a boolean indicating whether the K selector specifies GROUPS or not
     */
    public PGPathPropagatingBFS(
            long source,
            State startState,
            long intoTarget,
            State finalState,
            SearchMode searchMode,
            ProductGraphTraversalCursor.DataGraphRelationshipCursor graphCursor,
            PathTracer<Row> pathTracer,
            Function<SignpostStack, Row> toRow,
            Predicate<Row> nonInlinedPredicate,
            Boolean isGroupSelector,
            int maxDepth,
            int initialCountForTargetNodes,
            int nfaStateCount,
            MemoryTracker mt,
            PPBFSHooks hooks,
            AssertOpen assertOpen) {
        Preconditions.checkArgument(
                intoTarget != NO_SUCH_ENTITY || searchMode == SearchMode.Unidirectional,
                "Bidirectional search can only be performed with a target node");
        this.intoTarget = intoTarget;
        this.finalState = finalState;
        this.searchMode = searchMode;
        this.pathTracer = pathTracer;
        this.toRow = toRow;
        this.nonInlinedPredicate = nonInlinedPredicate;
        this.isGroupSelector = isGroupSelector;
        this.maxDepth = maxDepth;
        this.memoryTracker = mt.getScopedMemoryTracker();
        this.hooks = hooks;
        this.assertOpen = assertOpen;
        this.foundNodes = new FoundNodes(this.memoryTracker, searchMode, nfaStateCount);
        this.targets = new TargetTracker(this.memoryTracker, hooks);
        this.propagator = new Propagator(this.memoryTracker, hooks);
        this.globalState =
                new GlobalState(propagator, targets, searchMode, this.memoryTracker, hooks, initialCountForTargetNodes);
        var cursor = new ProductGraphTraversalCursor(graphCursor, this.memoryTracker);
        this.bfsExpander = new BFSExpander(foundNodes, globalState, cursor, graphCursor, intoTarget, nfaStateCount);

        this.sourceNodeState = new NodeState(globalState, source, startState, intoTarget);

        pathTracer.reset();

        this.hooks.newRow(source);
    }

    public static <Row> PGPathPropagatingBFS<Row> create(
            long source,
            State startState,
            long intoTarget,
            State finalState,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            PathTracer<Row> pathTracer,
            Function<SignpostStack, Row> toRow,
            Predicate<Row> nonInlinedPredicate,
            Boolean isGroupSelector,
            int maxDepth,
            int initialCountForTargetNodes,
            int numberOfNfaStates,
            MemoryTracker mt,
            PPBFSHooks hooks,
            AssertOpen assertOpen) {
        return new PGPathPropagatingBFS<>(
                source,
                startState,
                intoTarget,
                finalState,
                intoTarget == NO_SUCH_ENTITY ? SearchMode.Unidirectional : SearchMode.Bidirectional,
                new ProductGraphTraversalCursor.DataGraphRelationshipCursorImpl(read, nodeCursor, relCursor),
                pathTracer,
                toRow,
                nonInlinedPredicate,
                isGroupSelector,
                maxDepth,
                initialCountForTargetNodes,
                numberOfNfaStates,
                mt,
                hooks,
                assertOpen);
    }

    @Override
    protected Row fetchNextOrNull() {
        if (targetSaturated) {
            return null;
        }

        while (true) {
            if (pathTracer.ready()) {
                // exhaust the paths for the current target if there is one
                while (pathTracer.hasNext()) {
                    var row = pathTracer.next();
                    if (nonInlinedPredicate.test(row)) {
                        if (isGroupSelector) {
                            groupYielded = true;
                        } else {
                            pathTracer.decrementTargetCount();
                        }

                        if (intoTarget != NO_SUCH_ENTITY && pathTracer.isSaturated()) {
                            targetSaturated = true;
                        }
                        return row;
                    }
                }
            }

            if (groupYielded) {
                groupYielded = false;
                pathTracer.decrementTargetCount();

                if (intoTarget != NO_SUCH_ENTITY && pathTracer.isSaturated()) {
                    targetSaturated = true;
                    return null;
                }
            }

            // if we exhausted the current target set, expand & propagate until we find the next target set
            if (!currentTargets.hasNext()) {
                if (nextLevelWithTargets()) {
                    hooks.trace(globalState.depth());
                    currentTargets = targets.iterate();
                } else {
                    targetSaturated = true;
                    return null;
                }
            }

            pathTracer.reset();
            pathTracer.initialize(toRow, sourceNodeState, currentTargets.next(), globalState.depth());
        }
    }

    /**
     * Expand and propagate the PPBFS until it reaches a level that has targets.
     *
     * @return true if the PPBFS managed to find a level with targets, false if the PPBFS exhausted the component about
     * the source node.
     */
    private boolean nextLevelWithTargets() {
        if (initialLevel()) {
            return true;
        }
        do {
            if (shouldQuit()) {
                return false;
            }
            if (!nextLevel()) {
                return false;
            }
        } while (!targets.hasTargets());
        return true;
    }

    private boolean shouldQuit() {
        return targets.allKnownTargetsSaturated() && !foundNodes.hasMore();
    }

    /**
     * Expand nodes and propagate paths to nodes for the next level.
     *
     * @return true if we did any expansion/propagation, false if we've exhausted the component about the source node
     */
    private boolean nextLevel() {
        assertOpen.assertOpen();

        if (maxDepth != -1 && globalState.depth() == maxDepth) {
            return false;
        }

        globalState.nextDepth();

        hooks.nextLevel(globalState.depth());
        targets.clear();

        if (foundNodes.hasMore()) {
            bfsExpander.expand();
        } else if (!propagator.hasScheduled()) {
            hooks.noMoreNodes();
            return false;
        }

        propagator.propagate(globalState.depth());

        return true;
    }

    /**
     * In some cases the start node is also a target node, so before we begin to expand any relationships we expand all
     * node juxtapositions from the source node to see if we have found targets
     *
     * @return true if the zero-hop expansion was performed and targets were found
     */
    private boolean initialLevel() {
        if (foundNodes.totalDepth() > 0) {
            return false;
        }

        hooks.nextLevel(0);

        foundNodes.openBuffer();
        bfsExpander.discover(sourceNodeState, TraversalDirection.FORWARD);
        if (sourceNodeState.isTarget()) {
            targets.addTarget(sourceNodeState);
        }
        foundNodes.commitBuffer(TraversalDirection.FORWARD);

        if (searchMode == SearchMode.Bidirectional) {
            foundNodes.openBuffer();

            // we use encounter rather than discover in case the target NodeState was already discovered when flooding
            // node juxtapositions from the source
            bfsExpander.encounter(intoTarget, finalState, TraversalDirection.BACKWARD);

            // we don't add the targetNodeState to our set of targets for the 0th level; if it is accessible from the
            // source node then it will have been discovered when flooding node juxtapositions from the source
            foundNodes.commitBuffer(TraversalDirection.BACKWARD);
        }

        return targets.hasCurrentUnsaturatedTargets();
    }

    // TODO: call this to enable profiling
    // see https://trello.com/c/mB3RhJcA/5035-propper-db-hits
    public void setTracer(KernelReadTracer tracer) {
        bfsExpander.setTracer(tracer);
    }

    @Override
    public void close() throws Exception {
        foundNodes.close();
        bfsExpander.close();
        targets.close();
        propagator.close();
        // the scoped memory tracker means we don't need to manually release NodeState/TwoWaySignpost memory
        memoryTracker.close();
    }

    public enum Phase {
        Expansion,
        Propagation,
        Tracing
    }
}
