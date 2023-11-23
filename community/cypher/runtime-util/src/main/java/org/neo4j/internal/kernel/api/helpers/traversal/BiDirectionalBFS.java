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
import java.util.Objects;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongHashSet;
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.cypher.internal.expressions.SemanticDirection;
import org.neo4j.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
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
 * where the owning nodeId (I.e the target node of the relation in the pathTraceSteps) is the id,
 * and the corresponding list of pathTraceSteps is the value.
 * <p>
 * Iteration and retracing of paths is done with a PathsIterator which is described in more detail at its declaration.
 */
public class BiDirectionalBFS implements AutoCloseable {
    final int maxDepth;
    private final BFS sourceBFS;
    private final BFS targetBFS;
    private State algorithmState;

    private final boolean allowZeroLength;

    private enum State {
        NOT_INITIALIZED_WITH_NODES,
        CAN_SEARCH_FOR_INTERSECTION,
        FOUND_INTERSECTION,
        EXHAUSTED_INTERSECTION,
        THERE_IS_NO_INTERSECTION,
        REACHED_MAX_DEPTH
    }

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
     * @param stopAsapAtIntersect If we should compute the intersection between the BFS's lazily. See implementation
     *                            details at the top of the class definition.
     * @param read                Kernel Read.
     * @param nodeCursor          node cursor. Will never be closed by BiDirectionalBFS.
     * @param relCursor           relationship cursor. Will never be closed by BiDirectionalBFS.
     * @param memoryTracker       memory tracker.
     * @param nodeFilter          We will compute the shortest path among the set of all paths where all nodes satisfy this node filter.
     * @param relFilter           We will compute the shortest path among the set of all paths where all relationships satisfy this relationship filter.
     */
    public BiDirectionalBFS(
            long sourceNodeId,
            long targetNodeId,
            int[] types,
            SemanticDirection direction,
            int maxDepth,
            boolean stopAsapAtIntersect,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            MemoryTracker memoryTracker,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            boolean needOnlyOnePath,
            boolean allowZeroLength) {
        this.maxDepth = maxDepth;
        this.allowZeroLength = allowZeroLength;

        if (needOnlyOnePath) {
            this.sourceBFS = new SinglePathBFS(
                    sourceNodeId,
                    types,
                    direction,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    needOnlyOnePath);
            this.targetBFS = new SinglePathBFS(
                    targetNodeId,
                    types,
                    direction.reversed(),
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    needOnlyOnePath);
        } else if (stopAsapAtIntersect) {
            this.sourceBFS = new LazyBFS(
                    sourceNodeId,
                    types,
                    direction,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    needOnlyOnePath);
            this.targetBFS = new LazyBFS(
                    targetNodeId,
                    types,
                    direction.reversed(),
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    needOnlyOnePath);
        } else {
            this.sourceBFS = new EagerBFS(
                    sourceNodeId,
                    types,
                    direction,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    needOnlyOnePath);
            this.targetBFS = new EagerBFS(
                    targetNodeId,
                    types,
                    direction.reversed(),
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    needOnlyOnePath);
        }
        sourceBFS.setOther(targetBFS);
        targetBFS.setOther(sourceBFS);

        algorithmState = State.CAN_SEARCH_FOR_INTERSECTION;
    }

    private BiDirectionalBFS(
            int[] types,
            SemanticDirection direction,
            int maxDepth,
            boolean stopAsapAtIntersect,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            MemoryTracker memoryTracker,
            Boolean needOnlyOnePath,
            boolean allowZeroLength) {
        this(
                StatementConstants.NO_SUCH_NODE,
                StatementConstants.NO_SUCH_NODE,
                types,
                direction,
                maxDepth,
                stopAsapAtIntersect,
                read,
                nodeCursor,
                relCursor,
                memoryTracker,
                null,
                null,
                needOnlyOnePath,
                allowZeroLength);
        algorithmState = State.NOT_INITIALIZED_WITH_NODES;
    }

    /**
     * Should be used when we know the shortest path pattern but not the specific row/context. To set row and
     * context information use {@link BiDirectionalBFS#resetForNewRow(long, long, LongPredicate, Predicate)}
     *
     * @param types               The set of relationship types to traverse. A value of null means that all relationship types
     *                            are allowed.
     * @param direction           Specifies if we are interested in outgoing, incoming, or undirected paths from the source node
     * @param maxDepth            If set, stop searching for paths once we've reached maxDepth.
     * @param stopAsapAtIntersect If we should compute the intersection between the BFS's lazily. See implementation
     *                            details at the top of the class definition.
     * @param read                Kernel Read.
     * @param nodeCursor          node cursor. Will never be closed by ByDirectional BFS.
     * @param relCursor           relationship cursor. Will never be closed by ByDirectional BFS.
     * @param memoryTracker       memory tracker.
     * @return A new empty BiDirectionalBFS
     */
    public static BiDirectionalBFS newEmptyBiDirectionalBFS(
            int[] types,
            SemanticDirection direction,
            int maxDepth,
            boolean stopAsapAtIntersect,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            MemoryTracker memoryTracker,
            boolean needOnlyOnePath,
            boolean allowZeroLength) {
        return new BiDirectionalBFS(
                types,
                direction,
                maxDepth,
                stopAsapAtIntersect,
                read,
                nodeCursor,
                relCursor,
                memoryTracker,
                needOnlyOnePath,
                allowZeroLength);
    }

    @CalledFromGeneratedCode
    public static BiDirectionalBFS newEmptyBiDirectionalBFS(
            int[] types,
            SemanticDirection direction,
            int maxDepth,
            boolean stopAsapAtIntersect,
            Read read,
            MemoryTracker memoryTracker,
            boolean needOnlyOnePath,
            boolean allowZeroLength) {
        return new BiDirectionalBFS(
                types,
                direction,
                maxDepth,
                stopAsapAtIntersect,
                read,
                null,
                null,
                memoryTracker,
                needOnlyOnePath,
                allowZeroLength);
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
            Predicate<RelationshipTraversalCursor> relFilter) {
        sourceBFS.resetWithStartNode(sourceNodeId, nodeFilter, relFilter);
        targetBFS.resetWithStartNode(targetNodeId, nodeFilter, relFilter);
        algorithmState = State.CAN_SEARCH_FOR_INTERSECTION;
    }

    public void resetForNewRow(
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
     * the shortest path has a length greater than {@link BiDirectionalBFS#maxDepth}
     *
     * @return an iterator over the set of shortest paths between the source and target nodes specified at instantiation.
     */
    public Iterator<PathReference> shortestPathIterator() {
        assert (algorithmState == State.CAN_SEARCH_FOR_INTERSECTION);

        if (sourceBFS.startNodeId == targetBFS.startNodeId && allowZeroLength) {
            return new PathTracingIterator(
                    PrimitiveLongCollections.single(sourceBFS.startNodeId),
                    sourceBFS.currentDepth,
                    targetBFS.currentDepth,
                    sourceBFS.pathTraceData,
                    targetBFS.pathTraceData,
                    sourceBFS.needOnlyOnePath);
        }

        BFS bfsToAdvance = null;

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

        return new PathTracingIterator(
                bfsToAdvance.intersectionIterator(),
                sourceBFS.currentDepth,
                targetBFS.currentDepth,
                sourceBFS.pathTraceData,
                targetBFS.pathTraceData,
                sourceBFS.needOnlyOnePath);
    }

    private static BFS pickBFSWithSmallestCurrentLevelSet(BFS bfs1, BFS bfs2) {
        return bfs1.currentLevel.size() > bfs2.currentLevel.size() ? bfs2 : bfs1;
    }

    @Override
    public void close() {
        sourceBFS.close();
        targetBFS.close();
    }

    public void setTracer(KernelReadTracer tracer) {
        sourceBFS.setTracer(tracer);
        targetBFS.setTracer(tracer);
    }

    private abstract static class BFS<STEPS> implements AutoCloseable {

        protected static final int PATHS_TO_NODE_INIT_SIZE = 4;
        protected static final int LEVEL_INIT_CAPACITY = 16;
        protected static final int PATH_TRACE_DATA_INIT_CAPACITY = LEVEL_INIT_CAPACITY * 4;

        // While we use the terminology source/target node when talking about the end nodes
        // of a path, we will use 'start node' to refer to the start (source) of a BFS. This is done to not
        // confuse the start node of a BFS with the source node of the path (the startNode will admit both the value the
        // sourceNode
        // in the case of the sourceBFS, and the value of the targetNode in the case of the targetBFS).
        long startNodeId;
        protected int currentDepth;
        protected final int[] types;
        protected final Read read;
        protected NodeCursor nodeCursor;
        protected RelationshipTraversalCursor relCursor;
        RelationshipTraversalCursor selectionCursor;
        protected final MemoryTracker memoryTracker;
        protected LongPredicate nodeFilter;
        protected Predicate<RelationshipTraversalCursor> relFilter;
        protected HeapTrackingLongHashSet currentLevel;
        protected LongIterator currentLevelItr;
        protected HeapTrackingLongHashSet nextLevel;
        protected BFS other;
        protected LongIterator intersectionIterator = null;
        protected HeapTrackingLongObjectHashMap<STEPS> pathTraceData;

        protected SemanticDirection direction;
        protected RelationshipTraversalCursorRetriever retriever;
        boolean closed = false;
        final boolean needOnlyOnePath;

        @FunctionalInterface
        private interface RelationshipTraversalCursorRetriever {
            RelationshipTraversalCursor selectionCursor(
                    RelationshipTraversalCursor traversalCursor, NodeCursor node, int[] types);
        }

        public BFS(
                long startNodeId,
                int[] types,
                SemanticDirection direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                boolean needOnlyOnePath) {
            this.needOnlyOnePath = needOnlyOnePath;
            this.startNodeId = startNodeId;
            this.types = types;
            this.read = read;
            this.nodeCursor = nodeCursor;
            this.relCursor = relCursor;
            this.currentLevel = HeapTrackingCollections.newLongSet(memoryTracker, LEVEL_INIT_CAPACITY);
            this.memoryTracker = memoryTracker;
            this.nodeFilter = nodeFilter;
            this.relFilter = relFilter;
            this.currentLevel.add(startNodeId);
            this.currentLevelItr = currentLevel.longIterator();
            this.nextLevel = HeapTrackingCollections.newLongSet(memoryTracker, LEVEL_INIT_CAPACITY);
            this.currentDepth = 0;
            this.pathTraceData = HeapTrackingCollections.newLongObjectMap(memoryTracker, PATH_TRACE_DATA_INIT_CAPACITY);

            if (direction.equals(SemanticDirection.BOTH$.MODULE$)) {
                this.retriever = RelationshipSelections::allCursor;
            } else if (direction.equals(SemanticDirection.OUTGOING$.MODULE$)) {
                this.retriever = RelationshipSelections::outgoingCursor;
            } else {
                this.retriever = RelationshipSelections::incomingCursor;
            }
        }

        public abstract BiDirectionalBFS.State searchForIntersectionInNextLevel();

        public abstract LongIterator intersectionIterator();

        protected abstract boolean addNodeToNextLevelIfQualifies(long currentNode, long foundNode);

        public boolean hasSeenNode(long nodeId) {
            return pathTraceData.containsKey(nodeId);
        }

        public void resetWithStartNode(
                long startNodeId, LongPredicate nodeFilter, Predicate<RelationshipTraversalCursor> relFilter) {
            this.startNodeId = startNodeId;
            this.nodeFilter = nodeFilter;
            this.relFilter = relFilter;
            currentLevel.clear();
            nextLevel.clear();
            pathTraceData.clear();
            currentLevel.add(startNodeId);
            this.currentLevelItr = currentLevel.longIterator();
            this.currentDepth = 0;
        }

        public void resetWithStartNode(
                long startNodeId,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter) {
            this.nodeCursor = nodeCursor;
            this.relCursor = relCursor;
            resetWithStartNode(startNodeId, nodeFilter, relFilter);
        }

        public void setOther(BFS other) {
            this.other = other;
        }

        @Override
        public void close() {
            assert (!closed);
            pathTraceData.close();
            currentLevel.close();
            nextLevel.close();
            if (selectionCursor != relCursor && selectionCursor != null) {
                selectionCursor.close();
                selectionCursor = null;
            }
            closed = true;
        }

        public void setTracer(KernelReadTracer tracer) {
            if (nodeCursor != null) {
                nodeCursor.setTracer(tracer);
            }
            if (relCursor != null) {
                relCursor.setTracer(tracer);
            }
            if (selectionCursor != null) {
                selectionCursor.setTracer(tracer);
            }
        }
    }

    private static class SinglePathBFS extends BFS<PathTraceStep> {

        private long foundIntersectionNode = StatementConstants.NO_SUCH_NODE;

        public SinglePathBFS(
                long startNodeId,
                int[] types,
                SemanticDirection direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                boolean needOnlyOnePath) {
            super(
                    startNodeId,
                    types,
                    direction,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    needOnlyOnePath);
        }

        @Override
        public State searchForIntersectionInNextLevel() {
            if (currentLevel.size() == 0) {
                return State.THERE_IS_NO_INTERSECTION;
            }

            populateNextLevelOrStopWhenFoundFirstIntersectionNode();

            if (this.foundIntersectionNode != StatementConstants.NO_SUCH_NODE) {
                currentDepth++;
                return State.FOUND_INTERSECTION;
            }

            advanceLevel();

            return State.CAN_SEARCH_FOR_INTERSECTION;
        }

        @Override
        public LongIterator intersectionIterator() {
            return new LongIterator() {
                boolean consumedFirst = false;

                @Override
                public long next() {
                    return foundIntersectionNode;
                }

                @Override
                public boolean hasNext() {
                    if (!consumedFirst) {
                        consumedFirst = true;
                        return true;
                    }

                    return false;
                }
            };
        }

        @Override
        protected boolean addNodeToNextLevelIfQualifies(long currentNode, long foundNode) {
            if (hasSeenNode(foundNode) || !nodeFilter.test(foundNode)) {
                return false;
            }

            nextLevel.add(foundNode);

            pathTraceData.put(foundNode, new PathTraceStep(selectionCursor.reference(), currentNode));
            return true;
        }

        private void populateNextLevelOrStopWhenFoundFirstIntersectionNode() {
            while (currentLevelItr.hasNext()) {
                long currentNode = currentLevelItr.next();
                read.singleNode(currentNode, nodeCursor);
                if (!nodeCursor.next()) {
                    throw new EntityNotFoundException("Node " + currentNode + " was unexpectedly deleted");
                }
                selectionCursor = retriever.selectionCursor(relCursor, nodeCursor, types);
                while (selectionCursor.next()) {
                    if (relFilter.test(selectionCursor)) {
                        long foundNode = selectionCursor.otherNodeReference();
                        if (addNodeToNextLevelIfQualifies(currentNode, foundNode)
                                && other.currentLevel.contains(foundNode)) {
                            this.foundIntersectionNode = foundNode;
                            return;
                        }
                    }
                }
            }
        }

        private void advanceLevel() {
            var tmp = currentLevel;
            currentLevel = nextLevel;
            currentLevelItr = currentLevel.longIterator();
            nextLevel = tmp;
            nextLevel.clear();
            currentDepth++;
        }

        @Override
        public void resetWithStartNode(
                long startNodeId, LongPredicate nodeFilter, Predicate<RelationshipTraversalCursor> relFilter) {
            this.foundIntersectionNode = StatementConstants.NO_SUCH_NODE;
            super.resetWithStartNode(startNodeId, nodeFilter, relFilter);
        }
    }

    private abstract static class ManyPathsBFS extends BFS<HeapTrackingArrayList<PathTraceStep>> {
        // Some data that helps us in reusing the lists of PathTraceSteps between rows
        protected HeapTrackingArrayList<HeapTrackingArrayList<PathTraceStep>> availableArrayLists;
        protected int availableArrayListsCurrentIndex = 0;
        protected int availableArrayListsEnd = 0;

        public ManyPathsBFS(
                long startNodeId,
                int[] types,
                SemanticDirection direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                boolean needOnlyOnePath) {
            super(
                    startNodeId,
                    types,
                    direction,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    needOnlyOnePath);
            this.availableArrayLists =
                    HeapTrackingCollections.newArrayList(PATH_TRACE_DATA_INIT_CAPACITY, memoryTracker);
        }

        @Override
        protected boolean addNodeToNextLevelIfQualifies(long currentNode, long foundNode) {
            if (!hasSeenNode(foundNode) && nodeFilter.test(foundNode)) {
                nextLevel.add(foundNode);
                HeapTrackingArrayList<PathTraceStep> pathsToHere;

                // Can we reuse an arraylist which we initialized for an earlier input row?
                if (availableArrayListsCurrentIndex < availableArrayListsEnd) {
                    pathsToHere = availableArrayLists.get(availableArrayListsCurrentIndex++);
                    pathsToHere.clear();
                } else {
                    pathsToHere = HeapTrackingCollections.newArrayList(PATHS_TO_NODE_INIT_SIZE, memoryTracker);
                    availableArrayLists.add(pathsToHere);
                }
                pathsToHere.add(new PathTraceStep(selectionCursor.reference(), currentNode));
                pathTraceData.put(foundNode, pathsToHere);
                return true;

            } else if (!needOnlyOnePath && nextLevel.contains(foundNode)) {
                // foundNode has already been seen, but it was seen at this level with a different currentNode, so we
                // have multiple shortest paths to foundNode from startNode.
                pathTraceData.get(foundNode).add(new PathTraceStep(selectionCursor.reference(), currentNode));
                return true;
            }
            return false;
        }

        @Override
        public void resetWithStartNode(
                long startNodeId,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter) {
            super.resetWithStartNode(startNodeId, nodeCursor, relCursor, nodeFilter, relFilter);
            this.availableArrayListsCurrentIndex = 0;
            this.availableArrayListsEnd = this.availableArrayLists.size();
        }

        @Override
        public void resetWithStartNode(
                long startNodeId, LongPredicate nodeFilter, Predicate<RelationshipTraversalCursor> relFilter) {
            super.resetWithStartNode(startNodeId, nodeFilter, relFilter);
            this.availableArrayListsCurrentIndex = 0;
            this.availableArrayListsEnd = this.availableArrayLists.size();
        }

        @Override
        public void close() {
            availableArrayLists.forEach(HeapTrackingArrayList::close);
            availableArrayLists.close();
            super.close();
        }
    }

    private static class EagerBFS extends ManyPathsBFS {
        public EagerBFS(
                long startNodeId,
                int[] types,
                SemanticDirection direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                boolean withFallback) {
            super(
                    startNodeId,
                    types,
                    direction,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    withFallback);
        }

        private void fullyPopulateNextLevel() {
            while (currentLevelItr.hasNext()) {
                long currentNode = currentLevelItr.next();
                read.singleNode(currentNode, nodeCursor);
                if (!nodeCursor.next()) {
                    throw new EntityNotFoundException("Node " + currentNode + " was unexpectedly deleted");
                }
                selectionCursor = retriever.selectionCursor(relCursor, nodeCursor, types);
                while (selectionCursor.next()) {
                    if (relFilter.test(selectionCursor)) {
                        long foundNode = selectionCursor.otherNodeReference();
                        addNodeToNextLevelIfQualifies(currentNode, foundNode);
                    }
                }
            }
        }

        private void advanceLevel() {
            var tmp = currentLevel;
            currentLevel = nextLevel;
            nextLevel = tmp;
            nextLevel.clear();
            currentDepth++;
        }

        @Override
        public BiDirectionalBFS.State searchForIntersectionInNextLevel() {
            if (currentLevel.isEmpty()) {
                return State.THERE_IS_NO_INTERSECTION;
            }

            fullyPopulateNextLevel();

            advanceLevel();
            currentLevelItr = currentLevel.longIterator();

            MutableLongSet intersection = currentLevel.intersect(other.currentLevel);

            if (intersection.notEmpty()) {
                this.intersectionIterator = intersection.toImmutable().longIterator();
                return State.FOUND_INTERSECTION;
            }

            return State.CAN_SEARCH_FOR_INTERSECTION;
        }

        @Override
        public LongIterator intersectionIterator() {
            assert (this.intersectionIterator != null);
            return this.intersectionIterator;
        }
    }

    private static class LazyBFS extends ManyPathsBFS {

        private long foundIntersectionNode = StatementConstants.NO_SUCH_NODE; // A node that has been found by both BFSs
        private long currentNode = StatementConstants.NO_SUCH_NODE;

        public LazyBFS(
                long startNodeId,
                int[] types,
                SemanticDirection direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                boolean withFallback) {
            super(
                    startNodeId,
                    types,
                    direction,
                    read,
                    nodeCursor,
                    relCursor,
                    memoryTracker,
                    nodeFilter,
                    relFilter,
                    withFallback);
        }

        @Override
        public void resetWithStartNode(
                long startNodeId, LongPredicate nodeFilter, Predicate<RelationshipTraversalCursor> relFilter) {
            this.foundIntersectionNode = StatementConstants.NO_SUCH_NODE;
            this.currentNode = StatementConstants.NO_SUCH_NODE;
            super.resetWithStartNode(startNodeId, nodeFilter, relFilter);
        }

        private void populateNextLevelOrStopWhenFoundFirstIntersectionNode() {
            while (currentLevelItr.hasNext()) {
                currentNode = currentLevelItr.next();
                read.singleNode(currentNode, nodeCursor);
                if (!nodeCursor.next()) {
                    throw new EntityNotFoundException("Node " + currentNode + " was unexpectedly deleted");
                }
                selectionCursor = retriever.selectionCursor(relCursor, nodeCursor, types);
                while (selectionCursor.next()) {
                    if (relFilter.test(selectionCursor)) {
                        long foundNode = selectionCursor.otherNodeReference();
                        if (addNodeToNextLevelIfQualifies(currentNode, foundNode)
                                && other.currentLevel.contains(foundNode)) {
                            this.foundIntersectionNode = foundNode;
                            return;
                        }
                    }
                }
            }
        }

        private void advanceLevel() {
            var tmp = currentLevel;
            currentLevel = nextLevel;
            currentLevelItr = currentLevel.longIterator();
            nextLevel = tmp;
            nextLevel.clear();
            currentDepth++;
        }

        @Override
        public State searchForIntersectionInNextLevel() {
            if (currentLevel.isEmpty()) {
                return State.THERE_IS_NO_INTERSECTION;
            }

            populateNextLevelOrStopWhenFoundFirstIntersectionNode();

            if (this.foundIntersectionNode != StatementConstants.NO_SUCH_NODE) {
                currentDepth++;
                return State.FOUND_INTERSECTION;
            }

            advanceLevel();

            return State.CAN_SEARCH_FOR_INTERSECTION;
        }

        private State findNextIntersectionNode() {
            // When we enter this method we've already found the first intersection node, and want to find the next one.
            // In this state, currentNode (the node which had an outgoing relationship to the previous intersection
            // node)
            // might have more neighbors which we haven't visited. Thus, we need to start by iterating through whatever
            // remains in selectionCursor before we find the next currentNode.
            while (true) {
                while (selectionCursor.next()) {
                    if (relFilter.test(selectionCursor)) {
                        long foundNode = selectionCursor.otherNodeReference();
                        if (addNodeToNextLevelIfQualifies(currentNode, foundNode)
                                && other.currentLevel.contains(foundNode)) {
                            this.foundIntersectionNode = foundNode;
                            return State.FOUND_INTERSECTION;
                        }
                    }
                }
                if (!currentLevelItr.hasNext()) {
                    return State.EXHAUSTED_INTERSECTION;
                }
                currentNode = currentLevelItr.next();
                read.singleNode(currentNode, nodeCursor);
                if (!nodeCursor.next()) {
                    throw new EntityNotFoundException("Node " + currentNode + " was unexpectedly deleted");
                }
                selectionCursor = retriever.selectionCursor(relCursor, nodeCursor, types);
            }
        }

        @Override
        public LongIterator intersectionIterator() {
            return new LongIterator() {
                boolean consumedFirst = false;

                @Override
                public long next() {
                    return foundIntersectionNode;
                }

                @Override
                public boolean hasNext() {
                    if (!consumedFirst) {
                        consumedFirst = true;
                        return true;
                    }
                    // At this stage, we will have processed all the paths that we've currently found to
                    // foundIntersectionNode. But, we can still find more paths to foundIntersectionNode later on,
                    // and if we do so, we don't want to re-process the paths to it which we've already found. A quick
                    // but perhaps confusing way to accomplish this is to remove all the pathTraceSteps that we've found
                    // to it thus far.
                    pathTraceData.remove(foundIntersectionNode);

                    return findNextIntersectionNode() != State.EXHAUSTED_INTERSECTION;
                }
            };
        }
    }

    public static final class PathTraceStep {
        public final long relId;
        public final long prevNodeId;

        public PathTraceStep(long relId, long prevNodeId) {
            this.relId = relId;
            this.prevNodeId = prevNodeId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PathTraceStep that = (PathTraceStep) o;
            return relId == that.relId && prevNodeId == that.prevNodeId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(relId, prevNodeId);
        }

        @Override
        public String toString() {
            return "PathTraceStep[" + "relId=" + relId + ", " + "prevNodeId=" + prevNodeId + ']';
        }
    }
}
