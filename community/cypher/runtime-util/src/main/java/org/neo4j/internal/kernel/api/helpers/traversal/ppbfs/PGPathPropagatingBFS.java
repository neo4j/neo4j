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

import java.util.Iterator;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
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
    private final PPBFSHooks hooks;
    private int nextDepth = 0;
    private boolean isInitialLevel = true;

    /**
     * Creates a new PathPropagatingBFS.
     *
     * @param source The id of the starting node.
     * @param startState The initial state of the NFA generated from the QPP
     */
    public PGPathPropagatingBFS(
            long source,
            State startState,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            int initialCountForTargetNodes,
            int numberOfNfaStates,
            MemoryTracker mt,
            PPBFSHooks hooks) {
        this.hooks = hooks;
        this.dataManager = new DataManager(mt, hooks, this, initialCountForTargetNodes, numberOfNfaStates);
        this.bfsExpander = new BFSExpander(dataManager, read, nodeCursor, relCursor, mt, hooks);
        this.sourceData = new NodeData(mt, source, startState, 0, dataManager);

        dataManager.addToNextLevel(sourceData);
    }

    public int nextDepth() {
        return nextDepth;
    }

    /**
     * Returns an iterator of PathTracer's for the next level. Each path tracer
     * corresponds to the path set towards a specific target node. This allows the user to
     * implement one-to-one semantics, by counting paths to each specific target node.
     *
     * @return Iterator of path trace trees for next level.
     *         Note that due to performance concerns, the PathTracer is reused for each iteration, so it should be
     *         fully consumed before calling next() on the iterator.
     */
    public Iterator<PathTracer> pathTracersForNextLevel(PathTracer pathTracer) {
        if (!nextLevelWithTargets()) {
            return null;
        }

        pathTracer.setSourceNode(sourceData);

        hooks.tracingPathsOfLength(nextDepth);

        return new Iterator<>() {
            private final Iterator<NodeData> targetIterator = targets().iterator();

            @Override
            public boolean hasNext() {
                return targetIterator.hasNext();
            }

            @Override
            public PathTracer next() {
                pathTracer.resetWithNewTargetNodeAndDGLength(targetIterator.next(), nextDepth);
                return pathTracer;
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
     * @return the set of targets for the current level
     */
    private HeapTrackingArrayList<NodeData> targets() {
        return dataManager.targets();
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

        dataManager.propagateToLength(nextDepth);

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
        hooks.zeroHopLevel();
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
