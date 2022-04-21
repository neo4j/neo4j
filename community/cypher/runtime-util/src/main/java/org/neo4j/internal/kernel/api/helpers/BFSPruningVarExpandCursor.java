/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api.helpers;

import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingCursor;
import static org.neo4j.kernel.impl.newapi.Cursors.emptyTraversalCursor;

import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.neo4j.collection.trackable.HeapTrackingArrayDeque;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongHashSet;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;

/**
 * Cursor that performs breadth-first search without ever revisiting the same node multiple times.
 * <p>
 * A BFSPruningVarExpandCursor will not find all paths but is guaranteed to find all distinct end-nodes given the provided start-node and max-depth. Only works
 * for directed searches when we don't need to keep track of relationship uniqueness along the path.
 * <p>
 * Usage:
 * <p>
 * To find all distinct connected nodes with outgoing paths of length 1 to <code>max</code>, with relationship-types <code>types</code> starting from
 * <code>start</code>.
 * <pre>
 * {@code
 *     val cursor = BFSPruningVarExpandCursor.outgoingExpander( start,
 *                                                              types,
 *                                                              max,
 *                                                              read,
 *                                                              nodeCursor,
 *                                                              relCursor,
 *                                                              nodePred,
 *                                                              relPred,
 *                                                              tracker );
 *     while( cursor.next() )
 *     {
 *         System.out.println( cursor.otherNodeReference() );
 *     }
 * }
 * </pre>
 */
public abstract class BFSPruningVarExpandCursor extends DefaultCloseListenable implements RelationshipTraversalCursor {
    private final int[] types;
    private final int maxDepth;
    private final Read read;
    private final NodeCursor nodeCursor;
    private final RelationshipTraversalCursor relCursor;
    private RelationshipTraversalCursor selectionCursor;
    private int currentDepth;
    private final HeapTrackingArrayDeque<NodeState> queue;
    private final HeapTrackingLongHashSet seen;
    private final LongPredicate nodeFilter;
    private final Predicate<RelationshipTraversalCursor> relFilter;

    public static BFSPruningVarExpandCursor outgoingExpander(
            long startNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            MemoryTracker memoryTracker) {
        return outgoingExpander(
                startNode,
                null,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                LongPredicates.alwaysTrue(),
                alwaysTrue(),
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor outgoingExpander(
            long startNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        return outgoingExpander(
                startNode, null, maxDepth, read, nodeCursor, cursor, nodeFilter, relFilter, memoryTracker);
    }

    public static BFSPruningVarExpandCursor outgoingExpander(
            long startNode,
            int[] types,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            MemoryTracker memoryTracker) {
        return outgoingExpander(
                startNode,
                types,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                LongPredicates.alwaysTrue(),
                alwaysTrue(),
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor outgoingExpander(
            long startNode,
            int[] types,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        return new OutgoingBFSPruningVarExpandCursor(
                startNode, types, maxDepth, read, nodeCursor, cursor, nodeFilter, relFilter, memoryTracker);
    }

    public static BFSPruningVarExpandCursor incomingExpander(
            long startNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            MemoryTracker memoryTracker) {
        return incomingExpander(
                startNode,
                null,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                LongPredicates.alwaysTrue(),
                alwaysTrue(),
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor incomingExpander(
            long startNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        return incomingExpander(
                startNode, null, maxDepth, read, nodeCursor, cursor, nodeFilter, relFilter, memoryTracker);
    }

    public static BFSPruningVarExpandCursor incomingExpander(
            long startNode,
            int[] types,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            MemoryTracker memoryTracker) {
        return incomingExpander(
                startNode,
                types,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                LongPredicates.alwaysTrue(),
                alwaysTrue(),
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor incomingExpander(
            long startNode,
            int[] types,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        return new IncomingBFSPruningVarExpandCursor(
                startNode, types, maxDepth, read, nodeCursor, cursor, nodeFilter, relFilter, memoryTracker);
    }

    /**
     * Construct a BFSPruningVarExpandCursor.
     * <p>
     * Note that the lifecycle of the provided cursors should be maintained outside this class. They will never be closed form within this class.
     * This is useful if when cursors are pooled and reused.
     *
     * @param startNode     the node to start from
     * @param types         the types of the relationships to follow
     * @param maxDepth      the maximum depth of the search
     * @param read          a read instance
     * @param nodeCursor    a nodeCursor, will NOT be maintained and closed by this class
     * @param relCursor     a relCursor, will NOT be maintained and closed by this class
     * @param nodeFilter    must be true for all nodes along the path, NOTE not checked on startNode
     * @param relFilter     must be true for all relationships along the path
     * @param memoryTracker the memory tracker to use
     */
    private BFSPruningVarExpandCursor(
            long startNode,
            int[] types,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        this.types = types;
        this.maxDepth = maxDepth;
        this.read = read;
        this.nodeCursor = nodeCursor;
        this.relCursor = relCursor;
        this.nodeFilter = nodeFilter;
        this.relFilter = relFilter;
        // start with empty cursor and will expand from the start node
        // that is added at the top of the queue
        this.selectionCursor = emptyTraversalCursor(read);
        queue = HeapTrackingCollections.newArrayDeque(memoryTracker);
        seen = HeapTrackingCollections.newLongSet(memoryTracker);
        if (currentDepth < maxDepth) {
            queue.offer(new NodeState(startNode, currentDepth));
        }
    }

    protected abstract RelationshipTraversalCursor selectionCursor(
            RelationshipTraversalCursor relCursor, NodeCursor nodeCursor, int[] types);

    public final boolean next() {
        while (true) {
            while (selectionCursor.next()) {
                if (relFilter.test(selectionCursor)) {
                    long other = selectionCursor.otherNodeReference();
                    if (seen.add(other) && nodeFilter.test(other)) {
                        if (currentDepth < maxDepth) {
                            queue.offer(new NodeState(other, currentDepth));
                        }
                        return true;
                    }
                }
            }

            var next = queue.poll();
            if (next == null || !expand(next)) {
                return false;
            }
        }
    }

    private boolean expand(NodeState next) {
        read.singleNode(next.nodeId(), nodeCursor);
        if (nodeCursor.next()) {
            selectionCursor = selectionCursor(relCursor, nodeCursor, types);
            currentDepth = next.depth() + 1;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        nodeCursor.setTracer(tracer);
        relCursor.setTracer(tracer);
    }

    @Override
    public void removeTracer() {
        nodeCursor.removeTracer();
        relCursor.removeTracer();
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            if (selectionCursor != relCursor) {
                selectionCursor.close();
            }
            seen.close();
            queue.close();
            selectionCursor = null;
        }
    }

    @Override
    public boolean isClosed() {
        return selectionCursor == null;
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        selectionCursor.properties(cursor, selection);
    }

    @Override
    public Reference propertiesReference() {
        return selectionCursor.propertiesReference();
    }

    @Override
    public long relationshipReference() {
        return selectionCursor.relationshipReference();
    }

    @Override
    public int type() {
        return selectionCursor.type();
    }

    @Override
    public void source(NodeCursor cursor) {
        selectionCursor.source(cursor);
    }

    @Override
    public void target(NodeCursor cursor) {
        selectionCursor.target(cursor);
    }

    @Override
    public long sourceNodeReference() {
        return selectionCursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference() {
        return selectionCursor.targetNodeReference();
    }

    @Override
    public void otherNode(NodeCursor cursor) {
        selectionCursor.otherNode(cursor);
    }

    @Override
    public long otherNodeReference() {
        return selectionCursor.otherNodeReference();
    }

    @Override
    public long originNodeReference() {
        return selectionCursor.originNodeReference();
    }

    private record NodeState(long nodeId, int depth) {}

    private static class OutgoingBFSPruningVarExpandCursor extends BFSPruningVarExpandCursor {
        private OutgoingBFSPruningVarExpandCursor(
                long startNode,
                int[] types,
                int maxDepth,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor cursor,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                MemoryTracker memoryTracker) {
            super(startNode, types, maxDepth, read, nodeCursor, cursor, nodeFilter, relFilter, memoryTracker);
        }

        @Override
        protected RelationshipTraversalCursor selectionCursor(
                RelationshipTraversalCursor relCursor, NodeCursor nodeCursor, int[] types) {
            return outgoingCursor(relCursor, nodeCursor, types);
        }
    }

    private static class IncomingBFSPruningVarExpandCursor extends BFSPruningVarExpandCursor {
        private IncomingBFSPruningVarExpandCursor(
                long startNode,
                int[] types,
                int maxDepth,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor cursor,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                MemoryTracker memoryTracker) {
            super(startNode, types, maxDepth, read, nodeCursor, cursor, nodeFilter, relFilter, memoryTracker);
        }

        @Override
        protected RelationshipTraversalCursor selectionCursor(
                RelationshipTraversalCursor relCursor, NodeCursor nodeCursor, int[] types) {
            return incomingCursor(relCursor, nodeCursor, types);
        }
    }
}
