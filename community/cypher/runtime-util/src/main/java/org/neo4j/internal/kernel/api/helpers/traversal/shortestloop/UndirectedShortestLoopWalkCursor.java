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
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongArrayList;
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.exceptions.EntityNotFoundException;
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
 * <li>If it's a self-relationship we have a 1-length loop</li>
 * <li>Else we can use the same relationship to create a 2-length loop by going back and forth. </li>
 * </ol>
 * This means that we can iterate over all relationships leading out from the source node,
 * save them all in a list, and then either return the 1-length loops or combine all the others
 * (that end in the same node) into 2-length loops.
 */
public class UndirectedShortestLoopWalkCursor extends UndirectedShortestLoopCursor implements ShortestPathBFS {
    private final long startNode;
    private final Read read;
    private final NodeCursor nodeCursor;
    private final RelationshipTraversalCursor relCursor;
    private final int[] types;
    private final LongPredicate nodeFilter;
    private final Predicate<RelationshipTraversalEntities> relationshipsFilter;
    private final HeapTrackingLongObjectHashMap<HeapTrackingLongArrayList> pathTracing;
    private PathReference path;
    private final MemoryTracker memoryTracker;
    private final int maxDepth;
    private boolean closed;
    private Iterator<PathReference> iterator;

    public UndirectedShortestLoopWalkCursor(
            long startNode,
            int[] types,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalEntities> relationshipFilter,
            MemoryTracker memoryTracker) {
        this.startNode = startNode;
        this.read = read;
        this.types = types;
        this.maxDepth = maxDepth;
        this.nodeCursor = nodeCursor;
        this.relCursor = relCursor;
        this.nodeFilter = nodeFilter;
        this.relationshipsFilter = relationshipFilter;
        this.pathTracing = HeapTrackingCollections.newLongObjectMap(memoryTracker);
        this.path = null;
        this.memoryTracker = memoryTracker;
        this.closed = false;
        this.iterator = null;
    }

    @Override
    public boolean next() {
        if (iterator != null) {
            if (iterator.hasNext()) {
                path = iterator.next();
                return true;
            }
            return false;
        }
        if (search()) {
            if (pathTracing.containsKey(startNode)) {
                iterator = new MultiShortestLoopLengthOneIterator();
            } else {
                iterator = new MultiShortestLoopIterator();
            }
            path = iterator.next(); // We know there is at least one path since bfs returned true
            return true;
        } else {
            return false;
        }
    }

    private boolean search() {
        read.singleNode(startNode, nodeCursor);
        if (!nodeCursor.next()) {
            throw EntityNotFoundException.nodeUnexpectedlyDeleted(startNode);
        }
        RelationshipTraversalCursor selectionCursor = RelationshipSelections.allCursor(relCursor, nodeCursor, types);
        while (selectionCursor.next()) {
            if (relationshipsFilter.test(selectionCursor)) {
                long foundNode = selectionCursor.otherNodeReference();
                if (nodeFilter.test(foundNode)) {
                    if (maxDepth == 1 && foundNode != startNode) {
                        continue;
                    } // If maxDepth is 1 we cannot find any loops longer than self-loops
                    long foundRelationship = selectionCursor.reference();
                    if (pathTracing.containsKey(foundNode)) {
                        var traces = pathTracing.get(foundNode);
                        traces.add(foundRelationship);
                        pathTracing.put(foundNode, traces);
                    } else {
                        var traces = HeapTrackingLongArrayList.newLongArrayList(memoryTracker);
                        traces.add(foundRelationship);
                        pathTracing.put(foundNode, traces);
                    }
                }
            }
        }
        return !pathTracing.isEmpty();
    }

    @Override
    public PathReference path() {
        return path;
    }

    @Override
    public void closeInternal() {
        if (!closed) {
            for (long key : pathTracing.keySet().toArray()) {
                pathTracing.get(key).close();
            }
            pathTracing.close();
            closed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private class MultiShortestLoopLengthOneIterator implements Iterator<PathReference> {
        private int rel;

        public MultiShortestLoopLengthOneIterator() {
            this.rel = 0;
        }

        @Override
        public boolean hasNext() {
            return !pathTracing.isEmpty() && rel < pathTracing.get(startNode).size();
        }

        @Override
        public PathReference next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return pathReference(
                    new long[] {startNode, startNode},
                    new long[] {pathTracing.get(startNode).get(rel++)});
        }
    }

    private class MultiShortestLoopIterator implements Iterator<PathReference> {
        private int startRel;
        private int endRel;
        private int target;
        private final long[] targets;

        public MultiShortestLoopIterator() {
            this.startRel = 0;
            this.target = 0;
            this.endRel = -1;
            targets = pathTracing.keySet().toArray();
        }

        @Override
        public boolean hasNext() {
            if (pathTracing.isEmpty()) {
                return false;
            }
            if (pathTracing.get(targets[target]).size() - 1 > startRel) {
                return true; // We can return on a different rel
            } else if (pathTracing.get(targets[target]).size() - 1 > endRel) {
                return true; // We can start with another rel
            } else {
                return targets.length - 1 > target; // We can move on to the next target node
            }
        }

        @Override
        public PathReference next() {
            if (pathTracing.get(targets[target]).size() - 1 > endRel) {
                endRel++;
            } else if (pathTracing.get(targets[target]).size() - 1 > startRel) {
                endRel = 0;
                startRel++;
            } else if (targets.length - 1 > target) {
                target++;
                startRel = 0;
                endRel = 0;
            }
            return pathReference(new long[] {startNode, targets[target], startNode}, new long[] {
                pathTracing.get(targets[target]).get(startRel),
                pathTracing.get(targets[target]).get(endRel)
            });
        }
    }

    @Override
    public Iterator<PathReference> shortestPathIterator() {
        search(); // Initialize the paths
        if (pathTracing.containsKey(startNode)) {
            return new MultiShortestLoopLengthOneIterator();
        } else {
            return new MultiShortestLoopIterator();
        }
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
}
