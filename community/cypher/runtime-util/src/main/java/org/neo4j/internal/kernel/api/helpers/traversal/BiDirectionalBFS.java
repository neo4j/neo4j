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
package org.neo4j.internal.kernel.api.helpers.traversal;

import java.util.Iterator;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.virtual.PathReference;

/**
 * Implementation details:
 * <p>
 * This class implements the bidirectional BFS algorithm to retrieve all/the shortest path/paths
 * between a source and a target. This is done by the use of two BFS's.
 * One BFS centered about the source node, and one BFS centered about the target node.
 * <p>
 * We are going to want to expand these BFS's one level at a time, and we will say
 * that a BFS is expanded to mean that we expand a BFS by one level.
 * <p>
 * The BFS's are expanded in an interleaved fashion until an intersection of the BFS's is found. At
 * this point we can retrace the paths from all nodes in the intersection to the source and target
 * node to retrieve the shortest path(s).
 * <p>
 * As we want to explore as few nodes/relationships as possible, we always expand the BFS
 * that sees the fewest amount of nodes (any node on a shortest path will be seen no matter which BFS is expanded,
 * so always expand the cheapest one in a greedy fashion).
 * <p>
 * In the case of allShortestPaths, we know that we want to retrieve all shortest paths,
 * and thus we compute the full intersection before we start retracing and returning paths.
 * This is not the case for shortestPath though. In the case of shortestPath we will stop
 * expanding a BFS immediately upon finding a node in the intersection. This node can then be used
 * to retrieve some of the shortest paths. Computation of
 * the intersection is resumed only if we want to find more paths after this. These two different behaviours
 * are encapsulated in EagerBFS, which computes the full intersection immediately, and LazyBFS,
 * which computes the intersection on demand. Note that both EagerBFS and LazyBFS behave the same way
 * until an intersection node is found though.
 * <p>
 * To retrace paths, each found node maintains a list of pathTraceSteps. A pathTraceStep
 * is a record consisting of a prevNodeId and relId. The pathTraceSteps are stored in a map,
 * where the owning nodeId (i.e. the target node of the relation in the pathTraceSteps) is the id,
 * and the corresponding list of pathTraceSteps is the value.
 * <p>
 * Iteration and retracing of paths is done with a PathsIterator which is described in more detail at its declaration.
 */
public class BiDirectionalBFS implements AutoCloseable {
    private final BiDirectionalBFSImpl<?> inner;

    private BiDirectionalBFS(BiDirectionalBFSImpl<?> inner) {
        this.inner = inner;
    }

    public static BiDirectionalBFS newEmptyBiDirectionalBFS(
            long sourceNodeId,
            long targetNodeId,
            int[] types,
            Direction direction,
            int maxDepth,
            boolean stopAsapAtIntersect,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            MemoryTracker memoryTracker,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalEntities> relFilter,
            boolean needOnlyOnePath,
            boolean allowZeroLength) {
        BiDirectionalBFSImpl<?> bfs;
        if (needOnlyOnePath) {
            bfs = new BiDirectionalBFSImpl.SinglePathBiDirectionalBFS(
                    sourceNodeId,
                    targetNodeId,
                    types,
                    direction,
                    maxDepth,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    allowZeroLength);
        } else if (stopAsapAtIntersect) {
            bfs = new BiDirectionalBFSImpl.LazyMultiPathBiDirectionalBFS(
                    sourceNodeId,
                    targetNodeId,
                    types,
                    direction,
                    maxDepth,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    allowZeroLength);
        } else {
            bfs = new BiDirectionalBFSImpl.EagerMultiPathBiDirectionalBFS(
                    sourceNodeId,
                    targetNodeId,
                    types,
                    direction,
                    maxDepth,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    allowZeroLength);
        }
        bfs.algorithmState = State.CAN_SEARCH_FOR_INTERSECTION;
        return new BiDirectionalBFS(bfs);
    }

    public static BiDirectionalBFS newEmptyBiDirectionalBFS(
            int[] types,
            Direction direction,
            int maxDepth,
            boolean stopAsapAtIntersect,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            MemoryTracker memoryTracker,
            boolean needOnlyOnePath,
            boolean allowZeroLength) {
        BiDirectionalBFSImpl<?> bfs;
        if (needOnlyOnePath) {
            bfs = new BiDirectionalBFSImpl.SinglePathBiDirectionalBFS(
                    StatementConstants.NO_SUCH_NODE,
                    StatementConstants.NO_SUCH_NODE,
                    types,
                    direction,
                    maxDepth,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    null,
                    null,
                    allowZeroLength);
        } else if (stopAsapAtIntersect) {
            bfs = new BiDirectionalBFSImpl.LazyMultiPathBiDirectionalBFS(
                    StatementConstants.NO_SUCH_NODE,
                    StatementConstants.NO_SUCH_NODE,
                    types,
                    direction,
                    maxDepth,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    null,
                    null,
                    allowZeroLength);
        } else {
            bfs = new BiDirectionalBFSImpl.EagerMultiPathBiDirectionalBFS(
                    StatementConstants.NO_SUCH_NODE,
                    StatementConstants.NO_SUCH_NODE,
                    types,
                    direction,
                    maxDepth,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    null,
                    null,
                    allowZeroLength);
        }
        bfs.algorithmState = State.NOT_INITIALIZED_WITH_NODES;
        return new BiDirectionalBFS(bfs);
    }

    @CalledFromGeneratedCode
    public static BiDirectionalBFS newEmptyBiDirectionalBFS(
            int[] types,
            Direction direction,
            int maxDepth,
            boolean stopAsapAtIntersect,
            Read read,
            MemoryTracker memoryTracker,
            boolean needOnlyOnePath,
            boolean allowZeroLength) {

        BiDirectionalBFSImpl<?> bfs;
        if (needOnlyOnePath) {
            bfs = new BiDirectionalBFSImpl.SinglePathBiDirectionalBFS(
                    StatementConstants.NO_SUCH_NODE,
                    StatementConstants.NO_SUCH_NODE,
                    types,
                    direction,
                    maxDepth,
                    read,
                    null,
                    null,
                    memoryTracker,
                    null,
                    null,
                    allowZeroLength);
        } else if (stopAsapAtIntersect) {
            bfs = new BiDirectionalBFSImpl.LazyMultiPathBiDirectionalBFS(
                    StatementConstants.NO_SUCH_NODE,
                    StatementConstants.NO_SUCH_NODE,
                    types,
                    direction,
                    maxDepth,
                    read,
                    null,
                    null,
                    memoryTracker,
                    null,
                    null,
                    allowZeroLength);
        } else {
            bfs = new BiDirectionalBFSImpl.EagerMultiPathBiDirectionalBFS(
                    StatementConstants.NO_SUCH_NODE,
                    StatementConstants.NO_SUCH_NODE,
                    types,
                    direction,
                    maxDepth,
                    read,
                    null,
                    null,
                    memoryTracker,
                    null,
                    null,
                    allowZeroLength);
        }
        bfs.algorithmState = State.NOT_INITIALIZED_WITH_NODES;
        return new BiDirectionalBFS(bfs);
    }

    /**
     * Reset the BiDirectionalBFS in preparation of computing the shortest path(s) between
     * a new source and target node pair. Compared to creating a new BiDirectionalBFS object,
     * doing this has the advantage of not needing us to reinitialize all the data structures
     * we keep on the heap.
     */
    public void resetForNewRow(
            long sourceNodeId,
            long targetNodeId,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalEntities> relFilter) {
        inner.resetForNewRow(sourceNodeId, targetNodeId, nodeFilter, relFilter);
    }

    public void resetForNewRow(
            long sourceNodeId,
            long targetNodeId,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalEntities> relFilter) {
        inner.resetForNewRow(sourceNodeId, targetNodeId, nodeCursor, relCursor, nodeFilter, relFilter);
    }

    public Iterator<PathReference> shortestPathIterator() {
        return inner.shortestPathIterator();
    }

    @Override
    public void close() {
        inner.close();
    }

    public void setTracer(KernelReadTracer tracer) {
        inner.setTracer(tracer);
    }
}
