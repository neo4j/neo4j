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
import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.memory.MemoryTracker;
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
abstract class BiDirectionalBFSImpl<STEPS> implements AutoCloseable {
    final int maxDepth;
    final BFS<STEPS> sourceBFS;
    final BFS<STEPS> targetBFS;
    State algorithmState;

    private final boolean allowZeroLength;
    final Direction direction;

    /**
     * Create a BiDirectionalBFS which can be used to compute the shortest path(s) between
     * a source and a target node.
     *
     * @param sourceNodeId        The source node id.
     * @param targetNodeId        The target node id.
     * @param types               The set of relationship types to traverse. A value of null means that all relationship types
     *                            are allowed.
     * @param direction           Specifies if we are interested in outgoing, incoming, or undirected paths from the source node
     * @param maxDepth            If set, stop searching for paths once we've reached maxDepth.
     * @param read                Kernel Read.
     * @param nodeCursor          node cursor. Will never be closed by BiDirectionalBFS.
     * @param relCursor           relationship cursor. Will never be closed by BiDirectionalBFS.
     * @param memoryTracker       memory tracker.
     * @param nodeFilter          We will compute the shortest path among the set of all paths where all nodes satisfy this node filter.
     * @param relFilter           We will compute the shortest path among the set of all paths where all relationships satisfy this relationship filter.
     */
    BiDirectionalBFSImpl(
            long sourceNodeId,
            long targetNodeId,
            int[] types,
            Direction direction,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            MemoryTracker memoryTracker,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            boolean allowZeroLength) {
        this.maxDepth = maxDepth;
        this.allowZeroLength = allowZeroLength;
        this.direction = direction;
        this.sourceBFS = createBFS(
                sourceNodeId, types, direction, read, nodeCursor, relCursor, memoryTracker, nodeFilter, relFilter);
        this.targetBFS = createBFS(
                targetNodeId,
                types,
                direction.reverse(),
                read,
                nodeCursor,
                relCursor,
                memoryTracker,
                nodeFilter,
                relFilter);
        sourceBFS.setOther(targetBFS);
        targetBFS.setOther(sourceBFS);
    }

    abstract BFS<STEPS> createBFS(
            long sourceNodeId,
            int[] types,
            Direction direction,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            MemoryTracker memoryTracker,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter);

    abstract Iterator<PathReference> pathTracingIterator(LongIterator iterator);

    /**
     * Reset the BiDirectionalBFS in preparation of computing the shortest path(s) between
     * a new source and target node pair. Compared to creating a new BiDirectionalBFS object,
     * doing this has the advantage of not needing us to reinitialize all the data structures
     * we keep on the heap.
     */
    void resetForNewRow(
            long sourceNodeId,
            long targetNodeId,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter) {
        sourceBFS.resetWithStartNode(sourceNodeId, nodeFilter, relFilter);
        targetBFS.resetWithStartNode(targetNodeId, nodeFilter, relFilter);
        algorithmState = State.CAN_SEARCH_FOR_INTERSECTION;
    }

    void resetForNewRow(
            long sourceNodeId,
            long targetNodeId,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter) {
        sourceBFS.resetWithStartNode(sourceNodeId, nodeCursor, relCursor, nodeFilter, relFilter);
        targetBFS.resetWithStartNode(targetNodeId, nodeCursor, relCursor, nodeFilter, relFilter);
        algorithmState = State.CAN_SEARCH_FOR_INTERSECTION;
    }

    /**
     * Computes the shortest paths between the source and target node, and returns an iterator over them.
     * An empty iterator is returned when the source and target nodes are disconnected, and when
     * the shortest path has a length greater than {@link BiDirectionalBFSImpl#maxDepth}
     *
     * @return an iterator over the set of shortest paths between the source and target nodes specified at instantiation.
     */
    Iterator<PathReference> shortestPathIterator() {
        assert (algorithmState == State.CAN_SEARCH_FOR_INTERSECTION);

        if (sourceBFS.startNodeId == targetBFS.startNodeId && allowZeroLength) {
            return pathTracingIterator(PrimitiveLongCollections.single(sourceBFS.startNodeId));
        }

        BFS<STEPS> bfsToAdvance = null;

        int depth = 0;
        while (algorithmState == State.CAN_SEARCH_FOR_INTERSECTION) {
            if (depth++ == maxDepth) {
                algorithmState = State.REACHED_MAX_DEPTH;
            } else {
                bfsToAdvance = pickBFSWithSmallestCurrentLevelSet(sourceBFS, targetBFS);
                algorithmState = bfsToAdvance.searchForIntersectionInNextLevel();
            }
        }

        if (algorithmState == State.THERE_IS_NO_INTERSECTION || algorithmState == State.REACHED_MAX_DEPTH) {
            return java.util.Collections.emptyIterator();
        }

        return pathTracingIterator(bfsToAdvance.intersectionIterator());
    }

    private BFS<STEPS> pickBFSWithSmallestCurrentLevelSet(BFS<STEPS> bfs1, BFS<STEPS> bfs2) {
        return bfs1.currentLevel.size() > bfs2.currentLevel.size() ? bfs2 : bfs1;
    }

    @Override
    public void close() {
        sourceBFS.close();
        targetBFS.close();
    }

    void setTracer(KernelReadTracer tracer) {
        sourceBFS.setTracer(tracer);
        targetBFS.setTracer(tracer);
    }

    static class SinglePathBiDirectionalBFS extends BiDirectionalBFSImpl<PathTraceStep> {
        SinglePathBiDirectionalBFS(
                long sourceNodeId,
                long targetNodeId,
                int[] types,
                Direction direction,
                int maxDepth,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                boolean allowZeroLength) {
            super(
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

        @Override
        BFS<PathTraceStep> createBFS(
                long sourceNodeId,
                int[] types,
                Direction direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter) {
            return new BFS.SinglePathBFS(
                    sourceNodeId, types, direction, read, nodeCursor, relCursor, memoryTracker, nodeFilter, relFilter);
        }

        @Override
        Iterator<PathReference> pathTracingIterator(LongIterator iterator) {
            return PathTracingIterator.singlePathTracingIterator(
                    iterator,
                    sourceBFS.currentDepth,
                    targetBFS.currentDepth,
                    sourceBFS.pathTraceData,
                    targetBFS.pathTraceData);
        }
    }

    static class LazyMultiPathBiDirectionalBFS extends BiDirectionalBFSImpl<HeapTrackingArrayList<PathTraceStep>> {
        LazyMultiPathBiDirectionalBFS(
                long sourceNodeId,
                long targetNodeId,
                int[] types,
                Direction direction,
                int maxDepth,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                boolean allowZeroLength) {
            super(
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

        @Override
        Iterator<PathReference> pathTracingIterator(LongIterator iterator) {
            return PathTracingIterator.multiePathTracingIterator(
                    iterator,
                    sourceBFS.currentDepth,
                    targetBFS.currentDepth,
                    sourceBFS.pathTraceData,
                    targetBFS.pathTraceData);
        }

        @Override
        BFS<HeapTrackingArrayList<PathTraceStep>> createBFS(
                long sourceNodeId,
                int[] types,
                Direction direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter) {
            return new BFS.LazyBFS(
                    sourceNodeId, types, direction, read, nodeCursor, relCursor, memoryTracker, nodeFilter, relFilter);
        }
    }

    static class EagerMultiPathBiDirectionalBFS extends BiDirectionalBFSImpl<HeapTrackingArrayList<PathTraceStep>> {
        EagerMultiPathBiDirectionalBFS(
                long sourceNodeId,
                long targetNodeId,
                int[] types,
                Direction direction,
                int maxDepth,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                boolean allowZeroLength) {
            super(
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

        @Override
        BFS<HeapTrackingArrayList<PathTraceStep>> createBFS(
                long sourceNodeId,
                int[] types,
                Direction direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter) {
            return new BFS.EagerBFS(
                    sourceNodeId, types, direction, read, nodeCursor, relCursor, memoryTracker, nodeFilter, relFilter);
        }

        @Override
        Iterator<PathReference> pathTracingIterator(LongIterator iterator) {
            return PathTracingIterator.multiePathTracingIterator(
                    iterator,
                    sourceBFS.currentDepth,
                    targetBFS.currentDepth,
                    sourceBFS.pathTraceData,
                    targetBFS.pathTraceData);
        }
    }
}
