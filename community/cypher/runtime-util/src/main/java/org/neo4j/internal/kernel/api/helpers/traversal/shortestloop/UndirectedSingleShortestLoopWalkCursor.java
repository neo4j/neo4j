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
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongArrayList;
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

public final class UndirectedSingleShortestLoopWalkCursor extends UndirectedShortestLoopCursor
        implements ShortestPathBFS {
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

    public UndirectedSingleShortestLoopWalkCursor(
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
    }

    @Override
    public boolean next() {
        if (path == null) {
            return search();
        } else {
            return false;
        }
    }

    public boolean search() {
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
                    }
                    if (foundNode == startNode) {
                        var traces = HeapTrackingLongArrayList.newLongArrayList(memoryTracker);
                        traces.add(selectionCursor.reference());
                        pathTracing.put(startNode, traces);
                        path = pathReference(
                                new long[] {startNode, startNode}, new long[] {selectionCursor.reference()});
                    } else {

                        var traces = HeapTrackingLongArrayList.newLongArrayList(memoryTracker);
                        traces.add(selectionCursor.reference());
                        pathTracing.put(foundNode, traces);
                        path = pathReference(
                                new long[] {startNode, foundNode, startNode},
                                new long[] {selectionCursor.reference(), selectionCursor.reference()});
                    }
                    return true;
                }
            }
        }
        return false;
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

    @Override
    public Iterator<PathReference> shortestPathIterator() {
        search(); // Initialize the paths
        return Iterators.iterator(path);
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
