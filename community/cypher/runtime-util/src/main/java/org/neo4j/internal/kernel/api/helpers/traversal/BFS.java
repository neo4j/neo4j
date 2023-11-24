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

import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongHashSet;
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.exceptions.EntityNotFoundException;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.memory.MemoryTracker;

abstract class BFS<STEPS> implements AutoCloseable {

    static final int PATHS_TO_NODE_INIT_SIZE = 4;
    static final int LEVEL_INIT_CAPACITY = 16;
    static final int PATH_TRACE_DATA_INIT_CAPACITY = LEVEL_INIT_CAPACITY * 4;

    // While we use the terminology source/target node when talking about the end nodes
    // of a path, we will use 'start node' to refer to the start (source) of a BFS. This is done to not
    // confuse the start node of a BFS with the source node of the path (the startNode will admit both the value the
    // sourceNode
    // in the case of the sourceBFS, and the value of the targetNode in the case of the targetBFS).
    long startNodeId;
    int currentDepth;
    final int[] types;
    final Read read;
    NodeCursor nodeCursor;
    RelationshipTraversalCursor relCursor;
    RelationshipTraversalCursor selectionCursor;
    final MemoryTracker memoryTracker;
    LongPredicate nodeFilter;
    Predicate<RelationshipTraversalCursor> relFilter;
    HeapTrackingLongHashSet currentLevel;
    LongIterator currentLevelItr;
    HeapTrackingLongHashSet nextLevel;
    BFS<STEPS> other;
    LongIterator intersectionIterator = null;
    final HeapTrackingLongObjectHashMap<STEPS> pathTraceData;

    final RelationshipTraversalCursorRetriever retriever;
    boolean closed = false;
    final boolean needOnlyOnePath;

    @FunctionalInterface
    interface RelationshipTraversalCursorRetriever {
        RelationshipTraversalCursor selectionCursor(
                RelationshipTraversalCursor traversalCursor, NodeCursor node, int[] types);
    }

    BFS(
            long startNodeId,
            int[] types,
            Direction direction,
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

        this.retriever = switch (direction) {
            case OUTGOING -> RelationshipSelections::outgoingCursor;
            case INCOMING -> RelationshipSelections::incomingCursor;
            case BOTH -> RelationshipSelections::allCursor;};
    }

    abstract State searchForIntersectionInNextLevel();

    abstract LongIterator intersectionIterator();

    boolean hasSeenNode(long nodeId) {
        return pathTraceData.containsKey(nodeId);
    }

    void resetWithStartNode(
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

    void resetWithStartNode(
            long startNodeId,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter) {
        this.nodeCursor = nodeCursor;
        this.relCursor = relCursor;
        resetWithStartNode(startNodeId, nodeFilter, relFilter);
    }

    void setOther(BFS<STEPS> other) {
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

    void setTracer(KernelReadTracer tracer) {
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

    static class SinglePathBFS extends BFS<PathTraceStep> {

        private long foundIntersectionNode = StatementConstants.NO_SUCH_NODE;

        SinglePathBFS(
                long startNodeId,
                int[] types,
                Direction direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter) {
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
                    true);
        }

        @Override
        State searchForIntersectionInNextLevel() {
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
        LongIterator intersectionIterator() {
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

        private boolean addNodeToNextLevelIfQualifies(long currentNode, long foundNode) {
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
        void resetWithStartNode(
                long startNodeId, LongPredicate nodeFilter, Predicate<RelationshipTraversalCursor> relFilter) {
            this.foundIntersectionNode = StatementConstants.NO_SUCH_NODE;
            super.resetWithStartNode(startNodeId, nodeFilter, relFilter);
        }
    }

    abstract static class ManyPathsBFS extends BFS<HeapTrackingArrayList<PathTraceStep>> {
        // Some data that helps us in reusing the lists of PathTraceSteps between rows
        final HeapTrackingArrayList<HeapTrackingArrayList<PathTraceStep>> availableArrayLists;
        int availableArrayListsCurrentIndex = 0;
        int availableArrayListsEnd = 0;

        ManyPathsBFS(
                long startNodeId,
                int[] types,
                Direction direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter) {
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
                    false);
            this.availableArrayLists =
                    HeapTrackingCollections.newArrayList(PATH_TRACE_DATA_INIT_CAPACITY, memoryTracker);
        }

        boolean addNodeToNextLevelIfQualifies(long currentNode, long foundNode) {
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
        void resetWithStartNode(
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

    static class EagerBFS extends ManyPathsBFS {
        public EagerBFS(
                long startNodeId,
                int[] types,
                Direction direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter) {
            super(startNodeId, types, direction, read, nodeCursor, relCursor, memoryTracker, nodeFilter, relFilter);
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
        public State searchForIntersectionInNextLevel() {
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

    static class LazyBFS extends ManyPathsBFS {

        private long foundIntersectionNode = StatementConstants.NO_SUCH_NODE; // A node that has been found by both BFSs
        private long currentNode = StatementConstants.NO_SUCH_NODE;

        public LazyBFS(
                long startNodeId,
                int[] types,
                Direction direction,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                MemoryTracker memoryTracker,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter) {
            super(startNodeId, types, direction, read, nodeCursor, relCursor, memoryTracker, nodeFilter, relFilter);
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
}
