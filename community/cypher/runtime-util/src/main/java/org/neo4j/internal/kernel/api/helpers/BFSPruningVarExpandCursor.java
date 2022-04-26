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
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingCursor;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.impl.newapi.Cursors.emptyTraversalCursor;

import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.neo4j.collection.trackable.HeapTrackingArrayDeque;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongHashSet;
import org.neo4j.collection.trackable.HeapTrackingLongLongHashMap;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.memory.MemoryTracker;

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
 *         System.out.println( cursor.endNode() );
 *     }
 * }
 * </pre>
 */
public abstract class BFSPruningVarExpandCursor extends DefaultCloseListenable implements Cursor {
    final int[] types;
    final Read read;
    final int maxDepth;
    final NodeCursor nodeCursor;
    final RelationshipTraversalCursor relCursor;
    RelationshipTraversalCursor selectionCursor;
    final LongPredicate nodeFilter;
    final Predicate<RelationshipTraversalCursor> relFilter;

    public static BFSPruningVarExpandCursor outgoingExpander(
            long startNode,
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            MemoryTracker memoryTracker) {
        return outgoingExpander(
                startNode,
                null,
                includeStartNode,
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
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        return outgoingExpander(
                startNode,
                null,
                includeStartNode,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                nodeFilter,
                relFilter,
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor outgoingExpander(
            long startNode,
            int[] types,
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            MemoryTracker memoryTracker) {
        return outgoingExpander(
                startNode,
                types,
                includeStartNode,
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
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        return new OutgoingBFSPruningVarExpandCursor(
                startNode,
                types,
                includeStartNode,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                nodeFilter,
                relFilter,
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor incomingExpander(
            long startNode,
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            MemoryTracker memoryTracker) {
        return incomingExpander(
                startNode,
                null,
                includeStartNode,
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
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        return incomingExpander(
                startNode,
                null,
                includeStartNode,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                nodeFilter,
                relFilter,
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor incomingExpander(
            long startNode,
            int[] types,
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            MemoryTracker memoryTracker) {
        return incomingExpander(
                startNode,
                types,
                includeStartNode,
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
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        return new IncomingBFSPruningVarExpandCursor(
                startNode,
                types,
                includeStartNode,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                nodeFilter,
                relFilter,
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor allExpander(
            long startNode,
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            MemoryTracker memoryTracker) {
        return allExpander(
                startNode,
                null,
                includeStartNode,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                LongPredicates.alwaysTrue(),
                alwaysTrue(),
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor allExpander(
            long startNode,
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        return allExpander(
                startNode,
                null,
                includeStartNode,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                nodeFilter,
                relFilter,
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor allExpander(
            long startNode,
            int[] types,
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            MemoryTracker memoryTracker) {
        return allExpander(
                startNode,
                types,
                includeStartNode,
                maxDepth,
                read,
                nodeCursor,
                cursor,
                LongPredicates.alwaysTrue(),
                alwaysTrue(),
                memoryTracker);
    }

    public static BFSPruningVarExpandCursor allExpander(
            long startNode,
            int[] types,
            boolean includeStartNode,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor cursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter,
            MemoryTracker memoryTracker) {
        if (includeStartNode) {
            return new AllBFSPruningVarExpandCursorIncludingStartNode(
                    startNode, types, maxDepth, read, nodeCursor, cursor, nodeFilter, relFilter, memoryTracker);
        } else {
            return new AllBFSPruningVarExpandCursor(
                    startNode, types, maxDepth, read, nodeCursor, cursor, nodeFilter, relFilter, memoryTracker);
        }
    }

    /**
     * Construct a BFSPruningVarExpandCursor.
     * <p>
     * Note that the lifecycle of the provided cursors should be maintained outside this class. They will never be closed form within this class.
     * This is useful if when cursors are pooled and reused.
     *
     * @param types         the types of the relationships to follow
     * @param maxDepth      the maximum depth of the search
     * @param read          a read instance
     * @param nodeCursor    a nodeCursor, will NOT be maintained and closed by this class
     * @param relCursor     a relCursor, will NOT be maintained and closed by this class
     * @param nodeFilter    must be true for all nodes along the path, NOTE not checked on startNode
     * @param relFilter     must be true for all relationships along the path
     */
    private BFSPruningVarExpandCursor(
            int[] types,
            int maxDepth,
            Read read,
            NodeCursor nodeCursor,
            RelationshipTraversalCursor relCursor,
            LongPredicate nodeFilter,
            Predicate<RelationshipTraversalCursor> relFilter) {
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
    }

    public abstract long endNode();

    protected abstract void closeMore();

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
            closeMore();
            selectionCursor = null;
        }
    }

    @Override
    public boolean isClosed() {
        return selectionCursor == null;
    }

    private record NodeState(long nodeId, int depth) {}

    private enum EmitState {
        NO,
        SHOULD_EMIT,
        EMIT,
        EMITTED
    }

    private abstract static class DirectedBFSPruningVarExpandCursor extends BFSPruningVarExpandCursor {
        private int currentDepth;
        private final long startNode;
        private final HeapTrackingLongHashSet seen;
        private final HeapTrackingArrayDeque<NodeState> queue;
        private EmitState state;

        private DirectedBFSPruningVarExpandCursor(
                long startNode,
                int[] types,
                boolean includeStartNode,
                int maxDepth,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor relCursor,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                MemoryTracker memoryTracker) {
            super(types, maxDepth, read, nodeCursor, relCursor, nodeFilter, relFilter);
            this.startNode = startNode;
            queue = HeapTrackingCollections.newArrayDeque(memoryTracker);
            seen = HeapTrackingCollections.newLongSet(memoryTracker);
            if (currentDepth < maxDepth) {
                queue.offer(new NodeState(startNode, currentDepth));
            }

            state = includeStartNode ? EmitState.SHOULD_EMIT : EmitState.NO;
        }

        public final boolean next() {
            if (shouldIncludeStartNode()) {
                return true;
            }

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

        @Override
        public long endNode() {
            return state == EmitState.EMIT ? startNode : selectionCursor.otherNodeReference();
        }

        @Override
        protected void closeMore() {
            seen.close();
            queue.close();
        }

        protected abstract RelationshipTraversalCursor selectionCursor(
                RelationshipTraversalCursor relCursor, NodeCursor nodeCursor, int[] types);

        private boolean shouldIncludeStartNode() {
            if (state == EmitState.SHOULD_EMIT) {
                seen.add(startNode);
                state = EmitState.EMIT;
                return true;
            } else if (state == EmitState.EMIT) {
                state = EmitState.EMITTED;
            }
            return false;
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
    }

    private static class OutgoingBFSPruningVarExpandCursor extends DirectedBFSPruningVarExpandCursor {
        private OutgoingBFSPruningVarExpandCursor(
                long startNode,
                int[] types,
                boolean includeStartNode,
                int maxDepth,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor cursor,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                MemoryTracker memoryTracker) {
            super(
                    startNode,
                    types,
                    includeStartNode,
                    maxDepth,
                    read,
                    nodeCursor,
                    cursor,
                    nodeFilter,
                    relFilter,
                    memoryTracker);
        }

        @Override
        protected RelationshipTraversalCursor selectionCursor(
                RelationshipTraversalCursor relCursor, NodeCursor nodeCursor, int[] types) {
            return outgoingCursor(relCursor, nodeCursor, types);
        }
    }

    private static class IncomingBFSPruningVarExpandCursor extends DirectedBFSPruningVarExpandCursor {
        private IncomingBFSPruningVarExpandCursor(
                long startNode,
                int[] types,
                boolean includeStartNode,
                int maxDepth,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor cursor,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                MemoryTracker memoryTracker) {
            super(
                    startNode,
                    types,
                    includeStartNode,
                    maxDepth,
                    read,
                    nodeCursor,
                    cursor,
                    nodeFilter,
                    relFilter,
                    memoryTracker);
        }

        @Override
        protected RelationshipTraversalCursor selectionCursor(
                RelationshipTraversalCursor relCursor, NodeCursor nodeCursor, int[] types) {
            return incomingCursor(relCursor, nodeCursor, types);
        }
    }

    /**
     * Used for undirected pruning expands where we are not including the start node.
     *
     * The main algorithm uses two frontiers making sure we never back-track in the graph.
     * However, the fact that the start node is not included adds an extra complexity if there
     * are loops in the graph, in which case we need to include the start node at the correct
     * depth in the BFS search (this is not required for correctness, but for future optimizations that make use of depth order). For loop detection we keep track of the parent of each seen node,
     * if we encounter a node, and we are coming from a node that is not the same as the seen parent
     * means we have detected a loop.
     */
    private static class AllBFSPruningVarExpandCursor extends BFSPruningVarExpandCursor {
        // These constants define a small state machine where we start in state NO_LOOP, and if we find a loop we set
        // the counter to the depth from the loop back to the start node. At each call to next we decrease the counter
        // until we hit 0 at which point we set the counter to EMIT_START_NODE to indicate that we should emit the start
        // node. On the next call to next we then set the counter to START_NODE_EMITTED.
        private static final int START_NODE_EMITTED = -1;
        private static final int EMIT_START_NODE = -2;
        private static final int NO_LOOP = -3;
        // used to keep track if a loop has been encountered. If we find a loop we set this counter to the depth at
        // which it was discovered so that we can emit the start-node at the correct depth.
        private int loopCounter = NO_LOOP;
        private int currentDepth;
        private HeapTrackingLongHashSet prevFrontier;
        private HeapTrackingLongHashSet currFrontier;
        // Keeps track of all seen nodes and their parent nodes. The parent is used for loop detection.
        private final HeapTrackingLongLongHashMap seenNodesWithParent;
        // Used to filter out the case where there are multiple relationships between nodes.
        private final HeapTrackingLongHashSet checkUniqueEndNodes;

        private LongIterator currentExpand;
        private final long startNode;

        private AllBFSPruningVarExpandCursor(
                long startNode,
                int[] types,
                int maxDepth,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor cursor,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                MemoryTracker memoryTracker) {
            super(types, maxDepth, read, nodeCursor, cursor, nodeFilter, relFilter);
            this.startNode = startNode;
            this.prevFrontier = HeapTrackingCollections.newLongSet(memoryTracker);
            this.currFrontier = HeapTrackingCollections.newLongSet(memoryTracker);
            this.seenNodesWithParent = HeapTrackingCollections.newLongLongMap(memoryTracker);
            this.checkUniqueEndNodes = HeapTrackingCollections.newLongSet(memoryTracker);
            expand(startNode);
        }

        @Override
        public final boolean next() {
            while (currentDepth < maxDepth) {
                clearLoopCount();
                while (nextRelationship()) {
                    if (relFilter.test(selectionCursor)) {
                        long origin = selectionCursor.originNodeReference();
                        long other = selectionCursor.otherNodeReference();
                        // in this loop we consider startNode as seen
                        // and only retrace later if a loop has been detected
                        if (other == startNode) {
                            // special case, self-loop for start node
                            if (origin == other && currentDepth == 0) {
                                loopCounter = 0;
                            }
                            continue;
                        }

                        long parentOfOther = seenNodesWithParent.getIfAbsent(other, NO_SUCH_NODE);

                        if (parentOfOther == NO_SUCH_NODE && nodeFilter.test(other)) {
                            seenNodesWithParent.put(other, origin);
                            currFrontier.add(other);
                            return true;
                        } else if (parentOfOther != NO_SUCH_NODE
                                && // make sure nodeFilter passed
                                origin != other
                                && // ignore self loops
                                shouldCheckForLoops()) // if we already found a shorter loop, don't bother
                        {
                            long parentOfOrigin = seenNodesWithParent.getIfAbsent(origin, NO_SUCH_NODE);
                            if (parentOfOrigin == NO_SUCH_NODE && currentDepth == 0) {
                                // we are in the very fist layer and have a loop to the start node
                                loopCounter = 1;
                            } else if (parentOfOrigin != other && parentOfOrigin != NO_SUCH_NODE) {
                                // By already checking, shouldCheckForLoop we know that we either have no loop
                                // or we have found a loop into a different BFS layer (not in prevFrontier)
                                // so overwriting loopCounter is always safe, and we don't need a Math.min(old, new)
                                loopCounter = prevFrontier.contains(other) ? currentDepth : currentDepth + 1;
                            }
                        }
                    }
                }

                if (currentExpand != null && currentExpand.hasNext()) {
                    if (!expand(currentExpand.next())) {
                        return false;
                    }
                } else {
                    if (checkAndDecreaseLoopCount()) {
                        return true;
                    }

                    swapFrontiers();
                    currentDepth++;
                }
            }

            return false;
        }

        @Override
        public long endNode() {
            return loopCounter == EMIT_START_NODE ? startNode : selectionCursor.otherNodeReference();
        }

        /*
         *We only need to check for loops if we haven't found one yet
         * or if there is still a possibility to find a shorter one
         */
        private boolean shouldCheckForLoops() {
            return !loopDetected() || loopCounter > currentDepth;
        }

        private void swapFrontiers() {
            var tmp = prevFrontier;
            prevFrontier = currFrontier;
            currentExpand = prevFrontier.longIterator();
            currFrontier = tmp;
            currFrontier.clear();
        }

        private boolean checkAndDecreaseLoopCount() {
            if (loopCounter == 0) {
                loopCounter = EMIT_START_NODE;
                return true;
            } else if (loopCounter > 0) {
                loopCounter--;
            }
            return false;
        }

        private void clearLoopCount() {
            if (loopCounter == EMIT_START_NODE) {
                loopCounter = START_NODE_EMITTED;
            }
        }

        private boolean loopDetected() {
            return loopCounter >= START_NODE_EMITTED;
        }

        /**
         * NOTE: we are eliminating duplicated end nodes except from the first layer here in order to simplify the loop detection later
         */
        private boolean nextRelationship() {
            while (selectionCursor.next()) {
                if (currentDepth == 0) {
                    return true;
                } else if (checkUniqueEndNodes.add(selectionCursor.otherNodeReference())) {
                    return true;
                }
            }
            return false;
        }

        private boolean expand(long nodeId) {
            checkUniqueEndNodes.clear();
            read.singleNode(nodeId, nodeCursor);
            if (nodeCursor.next()) {
                selectionCursor = allCursor(relCursor, nodeCursor, types);
                return true;
            } else {
                return false;
            }
        }

        @Override
        protected void closeMore() {
            seenNodesWithParent.close();
            prevFrontier.close();
            currFrontier.close();
            checkUniqueEndNodes.close();
        }
    }

    private static class AllBFSPruningVarExpandCursorIncludingStartNode extends BFSPruningVarExpandCursor {
        private int currentDepth;
        private HeapTrackingLongHashSet prevFrontier;
        private HeapTrackingLongHashSet currFrontier;
        private final HeapTrackingLongHashSet seen;
        private LongIterator currentExpand;
        private final long startNode;
        private EmitState state = EmitState.SHOULD_EMIT;

        private AllBFSPruningVarExpandCursorIncludingStartNode(
                long startNode,
                int[] types,
                int maxDepth,
                Read read,
                NodeCursor nodeCursor,
                RelationshipTraversalCursor cursor,
                LongPredicate nodeFilter,
                Predicate<RelationshipTraversalCursor> relFilter,
                MemoryTracker memoryTracker) {
            super(types, maxDepth, read, nodeCursor, cursor, nodeFilter, relFilter);
            this.startNode = startNode;
            this.prevFrontier = HeapTrackingCollections.newLongSet(memoryTracker);
            this.currFrontier = HeapTrackingCollections.newLongSet(memoryTracker);
            this.seen = HeapTrackingCollections.newLongSet(memoryTracker);
        }

        @Override
        public final boolean next() {
            if (state == EmitState.SHOULD_EMIT) {
                expand(startNode);
                seen.add(startNode);
                state = EmitState.EMIT;
                return true;
            }
            if (state == EmitState.EMIT) {
                state = EmitState.EMITTED;
            }

            while (currentDepth < maxDepth) {
                while (selectionCursor.next()) {
                    if (relFilter.test(selectionCursor)) {
                        long other = selectionCursor.otherNodeReference();
                        if (seen.add(other) && nodeFilter.test(other)) {
                            currFrontier.add(other);
                            return true;
                        }
                    }
                }

                if (currentExpand != null && currentExpand.hasNext()) {
                    if (!expand(currentExpand.next())) {
                        return false;
                    }
                } else {
                    swapFrontiers();
                    currentDepth++;
                }
            }

            return false;
        }

        @Override
        public long endNode() {
            return state == EmitState.EMIT ? startNode : selectionCursor.otherNodeReference();
        }

        private void swapFrontiers() {
            var tmp = prevFrontier;
            prevFrontier = currFrontier;
            currentExpand = prevFrontier.longIterator();
            currFrontier = tmp;
            currFrontier.clear();
        }

        private boolean expand(long nodeId) {
            read.singleNode(nodeId, nodeCursor);
            if (nodeCursor.next()) {
                selectionCursor = allCursor(relCursor, nodeCursor, types);
                return true;
            } else {
                return false;
            }
        }

        @Override
        protected void closeMore() {
            seen.close();
            prevFrontier.close();
            currFrontier.close();
        }
    }
}
