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
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * This is the root of the product graph PPBFS algorithm. It is provided with a single source node and a starting NFA
 * state.
 *
 * To learn more about how the algorithm works, read the PPBFS guide:
 * https://neo4j.atlassian.net/wiki/spaces/CYPHER/pages/180977665/Shortest+K+Implementation
 */
public final class PGPathPropagatingBFS implements AutoCloseable {
    private final DataManager dataManager;
    private final BFSExpander bfsExpander;
    private final NodeData sourceData;
    private final long intoTarget;
    private final PathTracer pathTracer;
    private final PPBFSHooks hooks;
    private int nextDepth = 0;
    private boolean isInitialLevel = true;

    /**
     * Creates a new PathPropagatingBFS.
     *
     * @param source The id of the starting node.
     * @param startState The initial state of the NFA generated from the QPP
     * @param pathTracer A PathTracer instance that will be reused for each new target node & length
     */
    public PGPathPropagatingBFS(
            long source,
            long intoTarget,
            State startState,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            PathTracer pathTracer,
            int initialCountForTargetNodes,
            int numberOfNfaStates,
            MemoryTracker mt,
            PPBFSHooks hooks) {
        this.intoTarget = intoTarget;
        this.pathTracer = pathTracer;
        this.hooks = hooks;
        this.dataManager = new DataManager(mt, hooks, this, initialCountForTargetNodes, numberOfNfaStates);
        this.bfsExpander = new BFSExpander(dataManager, read, nodeCursor, relCursor, mt, hooks, intoTarget);
        this.sourceData = new NodeData(mt, source, startState, 0, dataManager, intoTarget);

        dataManager.addToNextLevel(sourceData);

        this.hooks.newRow(source);
    }

    public int nextDepth() {
        return nextDepth;
    }

    /**
     * @param toRow a function converting a traced path to a row in the relevant runtime
     * @param nonInlinedPredicate the non inlined predicate, executed on the output row
     * @param isGroupSelector a boolean indicating whether the K selector specifies GROUPS or not
     * @return an iterator of new rows in ascending order of length
     */
    public <Row> Iterator<Row> iterate(
            Function<PathTracer.TracedPath, Row> toRow, Predicate<Row> nonInlinedPredicate, Boolean isGroupSelector) {

        pathTracer.setSourceNode(sourceData);

        return new PrefetchingIterator<>() {
            private Iterator<NodeData> currentTargets = Collections.emptyIterator();
            private boolean done = false;

            @Override
            protected Row fetchNextOrNull() {
                if (done) {
                    return null;
                }

                while (true) {
                    if (pathTracer.ready()) {
                        // exhaust the paths for the current target if there is one
                        while (pathTracer.hasNext()) {
                            var path = pathTracer.next();
                            var row = toRow.apply(path);
                            if (nonInlinedPredicate.test(row)) {
                                if (!isGroupSelector) {
                                    // if the selector is not grouped, we count each valid path
                                    pathTracer.decrementTargetCount();
                                } else if (!pathTracer.hasNext()) {
                                    // if it is grouped, we wait until the last path of the group has been yielded
                                    pathTracer.decrementTargetCount();
                                }

                                if (intoTarget != NO_SUCH_ENTITY && pathTracer.isSaturated()) {
                                    done = true;
                                }
                                return row;
                            }
                        }
                    }

                    // if we exhausted the current target set, expand & propagate until we find the next target set
                    if (!currentTargets.hasNext()) {
                        if (nextLevelWithTargets()) {
                            currentTargets = dataManager.targets().iterator();
                        } else {
                            done = true;
                            return null;
                        }
                    }

                    pathTracer.resetWithNewTargetNodeAndDGLength(currentTargets.next(), nextDepth);
                }
            }
        };
    }

    /**
     * Expand and propagate the PPBFS until it reaches a level that has targets.
     *
     * @return true if the PPBFS managed to find a level with targets, false if the PPBFS exhausted the component about
     * the source node.
     */
    private boolean nextLevelWithTargets() {
        if (zeroHopLevel()) {
            return true;
        }
        do {
            if (shouldQuit()) {
                return false;
            }
            if (!nextLevel()) {
                return false;
            }
        } while (!dataManager.hasTargets());
        return true;
    }

    private boolean shouldQuit() {
        return !dataManager.hasLiveTargets() && !dataManager.hasNodesToExpand();
    }

    /**
     * Expand nodes and propagate paths to nodes for the next level.
     *
     * @return true if we did any expansion/propagation, false if we've exhausted the component about the source node
     */
    private boolean nextLevel() {
        nextDepth += 1;

        hooks.nextLevel(nextDepth);
        dataManager.clearTargets();

        if (!dataManager.hasNodesToPropagateOrExpand()) {
            hooks.noMoreNodes();
            return false;
        }

        if (dataManager.hasNodesToExpand()) {
            bfsExpander.expandLevel(nextDepth);
            dataManager.allocateNextLevel();
        }

        dataManager.propagateAll(nextDepth);

        return true;
    }

    /**
     * In some cases the start node is also a target node, so before we begin to expand any relationships we expand all
     * node juxtapositions from the source node to see if we have found targets
     *
     * @return true if the zero-hop expansion was performed and targets were found
     */
    private boolean zeroHopLevel() {
        if (!isInitialLevel) {
            return false;
        }
        isInitialLevel = false;

        Preconditions.checkState(nextDepth == 0, "zeroHopLevel called for nonzero depth");
        hooks.nextLevel(0);
        this.bfsExpander.floodInitialNodeJuxtapositions();
        dataManager.allocateNextLevel();

        if (sourceData.isTarget()) {
            dataManager.addTarget(sourceData);
        }

        return dataManager.hasTargetsWithRemainingCount();
    }

    // TODO: call this to enable profiling
    // see https://trello.com/c/mB3RhJcA/5035-propper-db-hits
    public void setTracer(KernelReadTracer tracer) {
        bfsExpander.setTracer(tracer);
    }

    @Override
    public void close() throws Exception {
        // relCursor and nodeCursor are not owned by this class; they should be closed by the consumer
        bfsExpander.close();
        dataManager.close();
    }
}
