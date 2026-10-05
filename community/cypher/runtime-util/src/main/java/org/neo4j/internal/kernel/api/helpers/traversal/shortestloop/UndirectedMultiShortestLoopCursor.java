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
package org.neo4j.internal.kernel.api.helpers.traversal.shortestloop;

import static org.neo4j.memory.HeapEstimator.sizeOfLongArray;
import static org.neo4j.values.virtual.VirtualValues.pathReference;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.collection.trackable.HeapTrackingArrayDeque;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongHashSet;
import org.neo4j.collection.trackable.HeapTrackingLongIntHashMap;
import org.neo4j.collection.trackable.HeapTrackingLongLongHashMap;
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.exceptions.EntityNotFoundException;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.internal.kernel.api.helpers.traversal.ShortestPathBFS;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.virtual.PathReference;

/**
 * Cursor that finds all shortest loops up to a given max depth.
 * <p>
 * The algorithm has two phases, first a BFS which is almost identical to the single loop version, but that exhausts all
 * paths up to the first shortest loop length instead of stopping when it first finds an intersection.
 * <p>
 * The second phase is a DFS that iterates over the possible shortest paths in the subgraph found by the BFS.
 */
public final class UndirectedMultiShortestLoopCursor extends UndirectedShortestLoopCursor implements ShortestPathBFS {
    private final MemoryTracker memoryTracker;
    private final long startNode;
    private final Read read;
    private final NodeCursor nodeCursor;
    private final RelationshipTraversalCursor relCursor;
    private final int[] types;
    private final int maxDepth;
    private int intersectionDepth;
    private final LongPredicate nodeFilter;
    private final Predicate<RelationshipTraversalEntities> relationshipsFilter;
    private boolean isClosed;
    private HeapTrackingLongHashSet currentFrontier;
    private LongIterator currentFrontierIterator;
    private int currentDepth;
    private HeapTrackingLongHashSet nextFrontier;
    private final PathTracer pathTracer;
    private final HeapTrackingLongHashSet intersections;
    private DFSIterator dfs;
    private PathReference pathReference;
    private boolean intersectionFoundEarly;

    public UndirectedMultiShortestLoopCursor(
            long startNode,
            int[] types,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalEntities> relationshipFilter,
            MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
        this.startNode = startNode;
        this.types = types;
        this.maxDepth = maxDepth;
        this.intersectionDepth = maxDepth;
        this.read = read;
        this.nodeCursor = nodeCursor;
        this.relCursor = relCursor;
        this.nodeFilter = nodeFilter;
        this.relationshipsFilter = relationshipFilter;
        this.isClosed = false;
        this.dfs = null;
        this.pathTracer = new PathTracer(memoryTracker, startNode);
        pathTracer.depths.put(startNode, 0);
        this.nextFrontier = HeapTrackingCollections.newLongSet(memoryTracker);
        this.currentFrontier = HeapTrackingCollections.newLongSet(memoryTracker);
        currentFrontier.add(startNode);
        this.currentFrontierIterator = currentFrontier.longIterator();
        this.intersections = HeapTrackingCollections.newLongSet(memoryTracker);
        this.currentDepth = 0;
        this.intersectionFoundEarly = false;
    }

    @Override
    public boolean next() {
        if (dfs != null) {
            if (dfs.hasNext()) {
                pathReference = dfs.next();
                return true;
            } else {
                closeInternal();
                return false;
            }
        }
        if (bfs()) {
            pathReference = dfs.next();
            return true;
        }
        return false;
    }

    private boolean bfs() {
        int pathLength = Integer.MAX_VALUE;
        HeapTrackingLongHashSet currentRoots = HeapTrackingCollections.newLongSet(memoryTracker);
        // For even maxDepth, level maxDepth / 2 can find no loop within the bound, but it records the
        // traces the DFS walks upwards for an even loop of length maxDepth found one level earlier.
        while (2 * currentDepth <= maxDepth
                && currentDepth <= intersectionDepth) { // We only check current frontier for loops so this is fine
            currentRoots.clear();
            while (currentFrontierIterator.hasNext()) {
                long origin = currentFrontierIterator.next();
                currentRoots.add(pathTracer.originRelationship(origin));
                read.singleNode(origin, nodeCursor);
                if (!nodeCursor.next()) {
                    throw EntityNotFoundException.nodeUnexpectedlyDeleted(origin);
                }
                var selectionCursor = RelationshipSelections.allCursor(relCursor, nodeCursor, types);
                var relCount = 0;
                while (selectionCursor.next()) {
                    relCount++;
                    if (relationshipsFilter.test(selectionCursor)) {
                        long foundNode = selectionCursor.otherNodeReference();
                        Loop loop = checkForLoop(pathTracer, currentFrontier, nextFrontier, foundNode, origin);
                        int temp = handleLoop(
                                loop, foundNode, intersectionFoundEarly, origin, selectionCursor.reference());
                        // Only after handleLoop does a newly found node carry its root; before, it read as -1.
                        currentRoots.add(pathTracer.originRelationship(foundNode));
                        if (temp != -1) {
                            pathLength = Math.min(pathLength, temp);
                        }
                    }
                }
                if (relCount == 1 && startNode == origin && intersections.isEmpty()) {
                    // A start node with a single relationship has no trail loop, unless that relationship is a
                    // self-relationship, which handleLoop has just recorded as an intersection.
                    currentRoots.close();
                    return false;
                }
            }
            // Judged once per level over every origin and neighbour: a level that hangs off a single start
            // relationship cannot produce a meet between two roots at any later level.
            // An odd loop is complete when its level ends: every trace the DFS walks exists by then, and the
            // next level could only record longer loops as intersections. Only an even loop needs one more level.
            if (nextFrontier.isEmpty()
                    || (!intersections.isEmpty() && pathLength % 2 == 1)
                    || (currentDepth > 1 && currentRoots.size() < 2)) {
                break;
            }
            var tmp = currentFrontier;
            currentFrontier = nextFrontier;
            currentFrontierIterator = currentFrontier.longIterator();
            nextFrontier = tmp;
            nextFrontier.clear();
            currentDepth++;
        }
        currentRoots.close();
        if (intersections.isEmpty()) {
            closeInternal();
            return false;
        }
        if (pathLength == 2) {
            dfs = new ShortestLoopLengthTwoIterator();
        } else {
            dfs = new MultiShortestLoopIterator(startNode, pathLength);
        }
        return dfs.hasNext();
    }

    private int handleLoop(Loop loop, long foundNode, boolean intersectionFoundEarly, long origin, long relationship) {
        // At the level after an even loop, only traces to shallower nodes are wanted. The one exception is a
        // second self-relationship on the start node at depth zero; a trace between two intersections, or from
        // an intersection to itself, can only lengthen a loop.
        if (intersectionDepth <= currentDepth
                && foundNode != startNode
                && (intersectionDepth != currentDepth || intersectionFoundEarly)
                && pathTracer.depth(foundNode) >= pathTracer.depth(origin)) {
            return -1;
        }
        pathTracer.add(foundNode, relationship, origin, currentDepth + 1, memoryTracker);
        switch (loop) {
            case LOOP_IN_CURRENT_FRONTIER -> {
                int pathLength = loop.loopLength(currentDepth);
                if (pathLength <= maxDepth) {
                    // When we find a loop in the current frontier we know
                    // that the loop will include two "intersections"
                    intersections.add(foundNode);
                    intersections.add(origin);
                    if (origin == foundNode) {
                        intersectionDepth = currentDepth;
                    } else {
                        intersectionDepth = currentDepth + 1;
                    }
                }
                return pathLength;
            }
            case LOOP_IN_NEXT_FRONTIER -> {
                if (pathTracer.depth(foundNode) > intersectionDepth) {
                    return -1;
                }
                int pathLength = loop.loopLength(currentDepth);
                if (pathLength <= maxDepth && nodeFilter.test(foundNode)) {
                    nextFrontier.add(foundNode);
                    intersections.add(foundNode);
                    this.intersectionFoundEarly = true;
                    intersectionDepth = currentDepth + 1;
                }
                return pathLength;
            }
            case NO_LOOP -> {
                if (nodeFilter.test(foundNode)) {
                    nextFrontier.add(foundNode);
                }
                return -1;
            }
        }
        return -1;
    }

    // we need to get into a state where we traverse all
    // possible traces to reach a node.
    private Loop checkForLoop(
            PathTracer pathTracing,
            HeapTrackingLongHashSet currentFrontier,
            HeapTrackingLongHashSet nextFrontier,
            long foundNode,
            long origin) {
        if (foundNode == startNode) {
            if (origin == foundNode) {
                return Loop.LOOP_IN_CURRENT_FRONTIER;
            } else {
                return Loop.ALREADY_SEEN;
            }
        } else {
            if (origin == foundNode) {
                return Loop.ALREADY_SEEN;
            }

            HeapTrackingArrayList<Trace> traces = pathTracing.get(foundNode);
            if (traces != null) {
                if (origin != startNode && traces.stream().anyMatch(trace -> trace.prevNode() == origin)) {
                    return Loop.ALREADY_SEEN;
                } else if (currentFrontier.contains(foundNode)) {
                    if (pathTracing.originRelationship(foundNode) == pathTracing.originRelationship(origin)) {
                        // Same root at the same depth: a longer route to a node this level already
                        // expands. NO_LOOP would re-insert it and expand it twice, duplicating its traces.
                        return Loop.ALREADY_SEEN;
                    }
                    return Loop.LOOP_IN_CURRENT_FRONTIER;
                } else if (nextFrontier.contains(foundNode)) {
                    if (pathTracing.originRelationship(foundNode) == pathTracing.originRelationship(origin)) {
                        return Loop.NO_LOOP; // There are no trail-mode intersections on the same root.
                    }
                    return Loop.LOOP_IN_NEXT_FRONTIER;
                } else {
                    return Loop.ALREADY_SEEN;
                }
            }
            return Loop.NO_LOOP;
        }
    }

    @Override
    public PathReference path() {
        if (pathReference == null) {
            throw new NoSuchElementException();
        } else {
            return pathReference;
        }
    }

    @Override
    public void closeInternal() {
        if (!isClosed) {
            for (long key : pathTracer.pathTracing.keySet().toArray()) {
                pathTracer.pathTracing.get(key).close();
            }
            pathTracer.close();
            currentFrontier.close();
            nextFrontier.close();
            intersections.close();
            if (dfs != null) {
                dfs.close();
            }
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        if (nodeCursor != null) {
            nodeCursor.setTracer(tracer);
        }
        if (relCursor != null) {
            relCursor.setTracer(tracer);
        }
    }

    @Override
    public Iterator<PathReference> shortestPathIterator() {
        if (dfs == null) {
            if (!bfs()) { // Initialize without advancing
                dfs = new EmptyIterator();
            }
        }
        return dfs;
    }

    private abstract static class DFSIterator extends PrefetchingIterator<PathReference> implements Closeable {
        @Override
        public void close() {}
    }

    private static class EmptyIterator extends DFSIterator {
        @Override
        protected PathReference fetchNextOrNull() {
            return null;
        }
    }

    private final class ShortestLoopLengthTwoIterator extends DFSIterator {
        private final LongIterator intersectionIterator;
        private long intersection;
        private HeapTrackingArrayList<Trace> traces;
        private int outIndex;
        private int backIndex;

        public ShortestLoopLengthTwoIterator() {
            this.intersectionIterator = intersections.longIterator();
            this.intersection = -1;
            this.traces = null;
            this.outIndex = 0;
            this.backIndex = 0;
        }

        @Override
        protected PathReference fetchNextOrNull() {
            while (true) {
                while (traces != null && outIndex < traces.size()) {
                    if (backIndex >= traces.size()) {
                        outIndex++;
                        backIndex = 0;
                        continue;
                    }
                    Trace out = traces.get(outIndex);
                    Trace back = traces.get(backIndex);
                    backIndex++;
                    if (out.relId() != back.relId() && out.prevNode() == startNode && back.prevNode() == startNode) {
                        return pathReference(
                                new long[] {startNode, intersection, startNode},
                                new long[] {out.relId(), back.relId()});
                    }
                }
                if (!intersectionIterator.hasNext()) {
                    return null;
                }
                intersection = intersectionIterator.next();
                traces = pathTracer.get(intersection);
                outIndex = 0;
                backIndex = 0;
            }
        }
    }

    private final class MultiShortestLoopIterator extends DFSIterator {
        private final long start;
        private final long[] currentNodes;
        private final long[] currentRelationships;
        private final HeapTrackingArrayDeque<State> stack;

        public MultiShortestLoopIterator(long start, int pathLength) {
            this.start = start;
            this.stack = HeapTrackingCollections.newArrayDeque(memoryTracker);

            // Initialize with HeapTracking
            memoryTracker.allocateHeap(
                    sizeOfLongArray(2 * pathLength + 1)); // Memory required for nodes and relationships
            currentNodes = new long[pathLength + 1];
            currentNodes[0] = startNode;
            currentRelationships = new long[pathLength];

            for (Trace trace : pathTracer.get(start)) {
                State state =
                        new State(trace, intersections.contains(trace.prevNode()), pathTracer.depth(trace.prevNode()));
                stack.push(state);
            }
        }

        @Override
        protected PathReference fetchNextOrNull() {
            while (!stack.isEmpty()) {
                State current = stack.pop();
                currentNodes[current.depth + 1] = current.currentNode();
                currentRelationships[current.depth] = current.trace.relId();

                if (current.currentNode() == startNode && current.direction == State.Direction.REACHED_INTERSECTION) {
                    return pathReference(currentNodes.clone(), currentRelationships.clone());
                } // Special case for self relations on start node

                List<Trace> traces = pathTracer.get(current.currentNode());
                HeapTrackingArrayDeque<State> stackAddition = HeapTrackingCollections.newArrayDeque(memoryTracker);

                for (Trace trace : traces) {
                    if (!visited(trace, current.depth) && trace.prevNode() == start) {
                        currentNodes[currentNodes.length - 1] = trace.prevNode();
                        currentRelationships[currentRelationships.length - 1] = trace.relId();
                        return pathReference(currentNodes.clone(), currentRelationships.clone());
                    }
                    if (current.valid(currentRelationships.length, pathTracer.depth(trace.prevNode()))
                            && !visited(trace, current.depth)) {
                        State newState = new State(
                                current,
                                trace,
                                intersections.contains(trace.prevNode()),
                                pathTracer.depth(trace.prevNode()));
                        stackAddition.push(newState);
                    }
                }
                for (State newState : stackAddition) {
                    stack.push(newState);
                } // Add all new states to the stack (we don't want to add any if we instead find a loop)
            }
            return null;
        }

        private boolean visited(Trace trace, int depth) {
            for (int i = 0; i <= depth; i++) {
                if (currentRelationships[i] == trace.relId()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void close() {
            stack.close();
        }
    }

    private static class State {
        private final Trace trace;
        private final int depth;
        private final Direction direction;
        private final int nodeDepth;

        // Creates an initial state
        public State(Trace trace, boolean isIntersection, int nodeDepth) {
            this.depth = 0;
            this.trace = trace;
            this.nodeDepth = nodeDepth;
            if (isIntersection) {
                this.direction = Direction.REACHED_INTERSECTION;
            } else {
                this.direction = Direction.TOWARDS_INTERSECTION;
            }
        }

        // Creates a new state based on a parent state
        public State(State oldState, Trace trace, boolean isIntersection, int traceDepth) {
            this.depth = oldState.depth + 1;
            this.nodeDepth = traceDepth;
            if (depth == 0) {
                if (isIntersection) {
                    this.direction = Direction.REACHED_INTERSECTION;
                } else {
                    this.direction = Direction.TOWARDS_INTERSECTION;
                }
            } else {
                if (oldState.nodeDepth < traceDepth && isIntersection) {
                    this.direction = Direction.REACHED_INTERSECTION;
                } else if (oldState.nodeDepth < traceDepth) {
                    this.direction = Direction.TOWARDS_INTERSECTION;
                } else {
                    this.direction = Direction.TOWARDS_SOURCE;
                }
            }
            this.trace = trace;
        }

        public boolean valid(int pathLength, int traceDepth) {
            if (direction == Direction.TOWARDS_SOURCE && traceDepth < nodeDepth) {
                return true;
            } else if (direction == Direction.TOWARDS_INTERSECTION && traceDepth > nodeDepth) {
                return true;
            } else if (direction == Direction.REACHED_INTERSECTION && (traceDepth == nodeDepth)) {
                return true;
            } else {
                return direction == Direction.REACHED_INTERSECTION && pathLength % 2 == 0;
            }
        }

        public long currentNode() {
            return trace.prevNode();
        }

        private enum Direction {
            TOWARDS_INTERSECTION,
            TOWARDS_SOURCE,
            REACHED_INTERSECTION
        }
    }
    /*
    Extension of path tracing that can keep multiple paths to the same node.
     */
    private static class PathTracer {
        private final long start;
        private final HeapTrackingLongObjectHashMap<HeapTrackingArrayList<Trace>> pathTracing;
        private final HeapTrackingLongIntHashMap depths;
        private final HeapTrackingLongLongHashMap originRelationships;

        public PathTracer(MemoryTracker memoryTracker, long start) {
            this.pathTracing = HeapTrackingCollections.newLongObjectMap(memoryTracker);
            this.depths = HeapTrackingCollections.newLongIntMap(memoryTracker);
            this.originRelationships = HeapTrackingCollections.newLongLongMap(memoryTracker);
            this.start = start;
        }

        public int depth(long node) {
            if (!depths.containsKey(node)) {
                return Integer.MAX_VALUE;
            }
            return depths.get(node);
        }

        public long originRelationship(long node) {
            if (!originRelationships.containsKey(node)) {
                return -1;
            }
            return originRelationships.get(node);
        }

        public void add(long node, long relationship, long origin, int depth, MemoryTracker memoryTracker) {
            long originRelationship;
            if (origin == start) {
                originRelationship = relationship;
            } else {
                originRelationship = originRelationships.get(origin);
            }
            if (!depths.containsKey(node)) {
                depths.put(node, depth);
            }
            if (!originRelationships.containsKey(node)) {
                originRelationships.put(node, originRelationship);
            }
            if (!pathTracing.containsKey(node)) {
                pathTracing.put(node, HeapTrackingCollections.newArrayList(memoryTracker));
            }
            HeapTrackingArrayList<Trace> traceList = pathTracing.get(node);
            traceList.add(new Trace(relationship, origin));
            pathTracing.put(node, traceList);
        }

        public HeapTrackingArrayList<Trace> get(long node) {
            return pathTracing.get(node);
        }

        public void close() {
            for (HeapTrackingArrayList<Trace> traces : pathTracing.values()) {
                traces.close();
            }
            pathTracing.close();
            depths.close();
            originRelationships.close();
        }
    }
}
