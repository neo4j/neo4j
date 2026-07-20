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

import static org.neo4j.values.virtual.VirtualValues.pathReference;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongHashSet;
import org.neo4j.collection.trackable.HeapTrackingLongLongHashMap;
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.exceptions.EntityNotFoundException;
import org.neo4j.internal.helpers.collection.Iterators;
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
 * The algorithm for the UndirectedSingleShortestLoopCursor works as follows:
 * <ol>
 * <li>Perform a BFS from the start node.</li>
 * <li>For each frontier (i.e. set of nodes to expand BFS to) check if the frontier (or the next frontier)
 *     has a node we have already visited. If we are trying to expand into an already
 *     visited node then we have found a intersection => we have found a loop.</li>
 * </ol>
 */
public final class UndirectedSingleShortestLoopCursor extends UndirectedShortestLoopCursor implements ShortestPathBFS {
    private final MemoryTracker memoryTracker;
    private final long startNode;
    private final Read read;
    private final NodeCursor nodeCursor;
    private final RelationshipTraversalCursor relCursor;
    private final int[] types;
    private final int maxDepth;
    private final LongPredicate nodeFilter;
    private final Predicate<RelationshipTraversalEntities> relationshipsFilter;
    private PathReference pathReference;

    public UndirectedSingleShortestLoopCursor(
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
        this.read = read;
        this.nodeCursor = nodeCursor;
        this.relCursor = relCursor;
        this.nodeFilter = nodeFilter;
        this.relationshipsFilter = relationshipFilter;
    }

    @Override
    public boolean next() {
        HeapTrackingLongObjectHashMap<Trace> pathTracing = HeapTrackingCollections.newLongObjectMap(memoryTracker);
        HeapTrackingLongHashSet nextFrontier = HeapTrackingCollections.newLongSet(memoryTracker);
        HeapTrackingLongHashSet currentFrontier = HeapTrackingCollections.newLongSet(memoryTracker);
        HeapTrackingLongLongHashMap roots = HeapTrackingCollections.newLongLongMap(memoryTracker);
        HeapTrackingLongHashSet currentRoots = HeapTrackingCollections.newLongSet(memoryTracker);
        try {
            currentFrontier.add(startNode);
            LongIterator currentFrontierIterator = currentFrontier.longIterator();
            int currentDepth = 0;

            if (pathReference != null) {
                pathReference = null;
                return false;
            }
            while (2 * (currentDepth + 1) <= maxDepth) {
                currentRoots.clear();
                while (currentFrontierIterator.hasNext()) {
                    long origin = currentFrontierIterator.next();
                    currentRoots.add(roots.get(origin));
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
                            if (!roots.containsKey(foundNode)) {
                                if (origin == startNode) {
                                    roots.put(foundNode, selectionCursor.reference());
                                } else {
                                    roots.put(foundNode, roots.get(origin));
                                }
                            }
                            currentRoots.add(roots.get(foundNode));
                            Loop loop = checkForLoop(pathTracing, currentFrontier, nextFrontier, foundNode, origin);
                            switch (loop) {
                                case LOOP_IN_CURRENT_FRONTIER, LOOP_IN_NEXT_FRONTIER -> {
                                    if (roots.get(foundNode) == roots.get(origin) && foundNode != origin) {
                                        if (nodeFilter.test(foundNode)) {
                                            nextFrontier.add(foundNode);
                                            pathTracing.put(foundNode, new Trace(selectionCursor.reference(), origin));
                                        }
                                        continue;
                                    }
                                    int pathLength = loop.loopLength(currentDepth);
                                    if (pathLength <= maxDepth) {
                                        this.pathReference = createPath(
                                                pathTracing,
                                                foundNode,
                                                selectionCursor.relationshipReference(),
                                                origin,
                                                pathLength,
                                                currentDepth);
                                        closeInternal();
                                        return true;
                                    }
                                }
                                case NO_LOOP -> {
                                    if (nodeFilter.test(foundNode)) {
                                        nextFrontier.add(foundNode);
                                        pathTracing.put(foundNode, new Trace(selectionCursor.reference(), origin));
                                    }
                                }
                            }
                        }
                    }
                    if (relCount == 1 && startNode == origin) {
                        return false;
                    } // If there is just one relation from startNode then we cannot find a loop (in trail mode)
                }
                if (nextFrontier.isEmpty()) {
                    return false;
                }
                if (currentDepth > 1 && currentRoots.size() < 2) {
                    return false;
                }
                var tmp = currentFrontier;
                currentFrontier = nextFrontier;
                currentFrontierIterator = currentFrontier.longIterator();
                nextFrontier = tmp;
                nextFrontier.clear();
                currentDepth++;
            }
            return false;
        } finally {
            currentRoots.close();
            pathTracing.close();
            currentFrontier.close();
            nextFrontier.close();
            roots.close();
        }
    }

    /**
     * Check if we have found a loop/intersection using simple rules:
     * <ol>
     * <li>Is the newly found node the origin/target?</li>
     * <ol type="a">
     * <li>If it is the same node we started on, then we have found a (self) loop!</li>
     * <li>Else, we have already used this relationship (otherwise it would have been found in the frontier).</li>
     * </ol>
     * <li>The newly found node is not the same as the origin.</li>
     * <ol type="a">
     * <li>if we did not start this relationship on the origin and the current trace leading to the found
     *                node is only related to the origin, then we must have already found an equally long loop and we only
     *                need one, so we discard it.</li>
     * <li>if the newly found node is also reachable in the current frontier, then we have found a loop/intersection.</li>
     * <li>if the newly found node is also reachable in the next frontier we have found a loop/intersection.</li>
     * <ul>
     * <li>If it is the same node we started on, then we have found a (self) loop!</li>
     * <li>Else, we have already used this relationship (otherwise it would have been found in the frontier).</li>
     * </ol>
     * <li>No loop is yet found and this is the first time we see this node; we need to continue the search on this branch.</li>
     * </ol>
     * </ol>
     */
    private Loop checkForLoop(
            HeapTrackingLongObjectHashMap<Trace> pathTracing,
            HeapTrackingLongHashSet currentFrontier,
            HeapTrackingLongHashSet nextFrontier,
            long foundNode,
            long origin) {
        if (foundNode == startNode) {
            if (origin == foundNode) {
                return Loop.LOOP_IN_CURRENT_FRONTIER; // self-loop
            } else {
                return Loop.ALREADY_SEEN;
            }
        } else {
            if (origin == foundNode) {
                return Loop.ALREADY_SEEN;
            }
            Trace trace = pathTracing.get(foundNode);
            if (trace != null) {
                if (origin != startNode && trace.prevNode() == origin) {
                    // NOTE we are only looking for a single path
                    // so we can skip this one
                    return Loop.ALREADY_SEEN;
                } else if (currentFrontier.contains(foundNode)) {
                    return Loop.LOOP_IN_CURRENT_FRONTIER;
                } else if (nextFrontier.contains(foundNode)) {
                    return Loop.LOOP_IN_NEXT_FRONTIER;
                } else {
                    return Loop.ALREADY_SEEN;
                }
            }
            return Loop.NO_LOOP;
        }
    }

    private PathReference createPath(
            HeapTrackingLongObjectHashMap<Trace> pathTracing,
            long intersectionNode,
            long intersectionRelationship,
            long originNode,
            int pathLength,
            int currentDepth) {
        if (intersectionNode == originNode) {
            // we have found a self-node to the start node
            assert currentDepth == 0;
            return pathReference(
                    new long[] {intersectionNode, intersectionNode}, new long[] {intersectionRelationship});
        } else {
            long[] relationships = new long[pathLength];
            long[] nodes = new long[pathLength + 1];
            int intersectionIndex = nodes.length / 2;
            nodes[intersectionIndex] = intersectionNode;
            nodes[intersectionIndex - 1] = originNode;
            relationships[intersectionIndex - 1] = intersectionRelationship;

            // go upwards
            for (int i = intersectionIndex + 1; i < nodes.length; i++) {
                var trace = pathTracing.get(nodes[i - 1]);
                nodes[i] = trace.prevNode();
                relationships[i - 1] = trace.relId();
            }
            // go downwards
            for (int i = intersectionIndex - 1; i > 0; i--) {
                var trace = pathTracing.get(nodes[i]);
                nodes[i - 1] = trace.prevNode();
                relationships[i - 1] = trace.relId();
            }

            return pathReference(nodes, relationships);
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
    public void closeInternal() {}

    @Override
    public boolean isClosed() {
        return false;
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
        next();
        return Iterators.iterator(pathReference);
    }
}
