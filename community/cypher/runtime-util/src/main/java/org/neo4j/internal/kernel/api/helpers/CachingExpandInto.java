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
package org.neo4j.internal.kernel.api.helpers;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.relationshipsCursor;
import static org.neo4j.memory.HeapEstimator.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

import java.util.Iterator;
import org.eclipse.collections.api.block.function.primitive.LongFunction0;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.github.jamm.Unmetered;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingUnifiedMap;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.lang.CloseListener;
import org.neo4j.memory.DefaultScopedMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

/**
 * Utility for performing Expand(Into)
 * <p>
 * Expand(Into) is the operation of given two nodes, find all interconnecting relationships of a given type and direction.
 * This is often a computationally heavy operation so that given direction and types an instance of this class can be reused
 * and previously found connections will be cached and can significantly speed up traversals.
 */
@SuppressWarnings({"unused"})
public class CachingExpandInto extends DefaultCloseListenable {
    static final long CACHING_EXPAND_INTO_SHALLOW_SIZE =
            shallowSizeOfInstance(CachingExpandInto.class) + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;

    static final long EXPAND_INTO_SELECTION_CURSOR_SHALLOW_SIZE =
            shallowSizeOfInstance(ExpandIntoSelectionCursor.class);
    static final long FROM_CACHE_SELECTION_CURSOR_SHALLOW_SIZE = shallowSizeOfInstance(FromCachedSelectionCursor.class);

    private final RelationshipCache relationshipCache;
    private final NodeDegreeCache degreeCache;

    @Unmetered
    private final Read read;

    @Unmetered
    private final ReadableTransactionState txState;

    @Unmetered
    private final Direction direction;

    private MemoryTracker scopedMemoryTracker;

    public CachingExpandInto(QueryContext context, Direction direction, MemoryTracker memoryTracker) {
        this(context, direction, memoryTracker, DEFAULT_CAPACITY);
    }

    public CachingExpandInto(QueryContext context, Direction direction, MemoryTracker memoryTracker, int capacity) {
        this.scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        this.scopedMemoryTracker.allocateHeap(CACHING_EXPAND_INTO_SHALLOW_SIZE);
        this.read = context.getRead();
        this.txState = context.getTransactionStateOrNull();
        this.direction = direction;
        this.relationshipCache = new RelationshipCache(capacity, scopedMemoryTracker);
        this.degreeCache = new NodeDegreeCache(capacity, scopedMemoryTracker);
    }

    @Override
    public void closeInternal() {
        if (scopedMemoryTracker != null) {
            scopedMemoryTracker.close();
            scopedMemoryTracker = null;
        }
    }

    @Override
    public boolean isClosed() {
        return scopedMemoryTracker == null;
    }

    public RelationshipTraversalCursor connectingRelationships(
            CursorFactory cursors,
            NodeCursor fromCursor,
            NodeCursor toCursor,
            int[] types,
            CursorContext cursorContext) {
        return connectingRelationships(
                fromCursor,
                toCursor,
                cursors.allocateRelationshipTraversalCursor(cursorContext, scopedMemoryTracker),
                types);
    }

    /**
     * @return The interconnecting relationships in the given direction with any of the given types, or `null` if
     *              nodes can't be found.
     */
    public RelationshipTraversalCursor connectingRelationships(
            long firstNode,
            NodeCursor firstCursor,
            long secondNode,
            NodeCursor secondCursor,
            RelationshipTraversalCursor traversalCursor,
            int[] types) {
        read.singleNode(firstNode, firstCursor);
        if (!firstCursor.next()) {
            return null;
        }
        // if we don't have anything in tx state, and we're on block format we only need to read the start node
        if ((txState == null || !txState.hasChanges()) && firstCursor.supportsFastRelationshipsTo()) {
            firstCursor.relationshipsTo(traversalCursor, selection(types, direction), secondNode);
            return traversalCursor;
        }
        read.singleNode(secondNode, secondCursor);
        if (secondCursor.next()) {
            return connectingRelationships(firstCursor, secondCursor, traversalCursor, types);
        } else {
            return null;
        }
    }

    /**
     * Creates a cursor for all connecting relationships given a first and a second node.
     * <p>
     * NOTE: Ownership of traversalCursor is _not_ transferred, so the caller is responsible
     *       for closing it when applicable
     * <p>
     * NOTE: In case nodeCursor supports fast relationships, the given traversalCursor could be returned.
     *       Otherwise, a specialized relationship selection cursor will be created and returned, and
     *       in this case it is important that this specialized cursor does not get reused as a general
     *       relationship traversal cursor just because it implements the RelationshipTraversalCursor interface.
     *
     * @param firstCursor Node cursor pointing at the "first" node
     * @param secondCursor Node cursor pointing at the "second" node
     * @param traversalCursor Traversal cursor used in traversal
     * @param types The relationship types to traverse
     * @return The interconnecting relationships in the given direction with any of the given types, or `null` if
     *         nodes can't be found.
     */
    public RelationshipTraversalCursor connectingRelationships(
            NodeCursor firstCursor, NodeCursor secondCursor, RelationshipTraversalCursor traversalCursor, int[] types) {
        Direction reverseDirection = direction.reverse();
        // First of all check if the cursor can do this efficiently itself and if so make use of that faster path
        boolean firstSupportsFastRelationshipsTo = firstCursor.supportsFastRelationshipsTo();
        boolean secondSupportsFastRelationshipsTo = secondCursor.supportsFastRelationshipsTo();
        long firstNode = firstCursor.nodeReference();
        long secondNode = secondCursor.nodeReference();
        if (firstSupportsFastRelationshipsTo && secondSupportsFastRelationshipsTo) {
            // The operation is fast on the store level, however if we have a high degree in the tx state it may still
            // pay off to start on the node with the lesser degree.
            long txStateDegreeFirst = calculateDegreeInTxState(firstNode, selection(types, direction));
            long txStateDegreeSecond = calculateDegreeInTxState(secondNode, selection(types, reverseDirection));
            if (txStateDegreeSecond >= txStateDegreeFirst) {
                firstCursor.relationshipsTo(traversalCursor, selection(types, direction), secondNode);
            } else {
                secondCursor.relationshipsTo(traversalCursor, selection(types, reverseDirection), firstNode);
            }
            return traversalCursor;
        } else if (firstSupportsFastRelationshipsTo) {
            firstCursor.relationshipsTo(traversalCursor, selection(types, direction), secondNode);
            return traversalCursor;
        } else if (secondSupportsFastRelationshipsTo) {
            secondCursor.relationshipsTo(traversalCursor, selection(types, reverseDirection), firstNode);
            return traversalCursor;
        } else {
            // Check if we've already done this before for these two nodes in this query
            Iterator<Relationship> connections = relationshipCache.get(firstNode, secondNode, direction);
            if (connections != null) {
                return new FromCachedSelectionCursor(connections, read, firstNode, secondNode);
            } else {
                return expandFromNodeWithLesserDegree(
                        firstCursor,
                        secondCursor,
                        traversalCursor,
                        types,
                        getAndCacheDegree(firstNode, firstCursor, types, direction)
                                <= getAndCacheDegree(secondNode, secondCursor, types, reverseDirection));
            }
        }
    }

    private long getAndCacheDegree(long node, NodeCursor nodeCursor, int[] types, Direction direction) {
        return degreeCache.getIfAbsentPut(node, direction, () -> {
            if (nodeCursor.supportsFastDegreeLookup()) {
                return nodeCursor.degree(selection(types, direction));
            } else {
                return calculateDegreeInTxState(node, selection(types, direction));
            }
        });
    }

    private RelationshipTraversalCursor expandFromNodeWithLesserDegree(
            NodeCursor firstCursor,
            NodeCursor secondCursor,
            RelationshipTraversalCursor traversalCursor,
            int[] types,
            boolean startOnFirstNode) {

        if (startOnFirstNode) {
            return connectingRelationshipsCursor(
                    relationshipsCursor(traversalCursor, firstCursor, types, direction),
                    secondCursor.nodeReference(),
                    firstCursor.nodeReference(),
                    secondCursor.nodeReference(),
                    direction);
        } else {
            Direction reverseDirection = direction.reverse();
            return connectingRelationshipsCursor(
                    relationshipsCursor(traversalCursor, secondCursor, types, reverseDirection),
                    firstCursor.nodeReference(),
                    firstCursor.nodeReference(),
                    secondCursor.nodeReference(),
                    reverseDirection);
        }
    }

    private long calculateDegreeInTxState(long node, RelationshipSelection selection) {
        if (txState == null) {
            return 0;
        } else {
            return txState.calculateDegreeInTxState(node, selection);
        }
    }

    private RelationshipTraversalCursor connectingRelationshipsCursor(
            final RelationshipTraversalCursor allRelationships,
            final long toNode,
            final long firstNode,
            final long secondNode,
            final Direction expandDirection) {
        return new ExpandIntoSelectionCursor(
                allRelationships, scopedMemoryTracker, toNode, firstNode, secondNode, expandDirection);
    }

    private class FromCachedSelectionCursor implements RelationshipTraversalCursor {
        @Unmetered
        private Iterator<Relationship> relationships;

        private Relationship currentRelationship;

        @Unmetered
        private final Read read;

        private int trackingHandle = UNTRACKED;

        private final long firstNode;
        private final long secondNode;

        FromCachedSelectionCursor(Iterator<Relationship> relationships, Read read, long firstNode, long secondNode) {
            this.relationships = relationships;
            this.read = read;
            this.firstNode = firstNode;
            this.secondNode = secondNode;
            scopedMemoryTracker.allocateHeap(FROM_CACHE_SELECTION_CURSOR_SHALLOW_SIZE);
        }

        @Override
        public boolean next() {
            if (relationships != null && relationships.hasNext()) {
                this.currentRelationship = relationships.next();
                return true;
            } else {
                close();
                return false;
            }
        }

        @Override
        public void removeTracer() {}

        @Override
        public void otherNode(NodeCursor cursor) {
            read.singleNode(otherNodeReference(), cursor);
        }

        @Override
        public long originNodeReference() {
            return firstNode;
        }

        @Override
        public void setTracer(KernelReadTracer tracer) {
            // these are cached no need to trace anything
        }

        @Override
        public void close() {
            if (relationships != null && scopedMemoryTracker != null) {
                relationships = null;
                scopedMemoryTracker.releaseHeap(FROM_CACHE_SELECTION_CURSOR_SHALLOW_SIZE);
            }
        }

        @Override
        public void closeInternal() {
            // nothing to close
        }

        @Override
        public boolean isClosed() {
            return relationships == null;
        }

        @Override
        public void setCloseListener(CloseListener closeListener) {
            // nothing close, just hand ourselves back to the closeListener so that
            // any tracking of this resource can be removed.
            if (closeListener != null) {
                closeListener.onClosed(this);
            }
        }

        @Override
        public void setTrackingHandle(int handle) {
            this.trackingHandle = handle;
        }

        @Override
        public int getTrackingHandle() {
            return trackingHandle;
        }

        @Override
        public long relationshipReference() {
            return currentRelationship.id;
        }

        @Override
        public int type() {
            return currentRelationship.type;
        }

        @Override
        public long otherNodeReference() {
            return secondNode;
        }

        @Override
        public long sourceNodeReference() {
            return currentRelationship.from;
        }

        @Override
        public long targetNodeReference() {
            return currentRelationship.to;
        }

        @Override
        public Reference propertiesReference() {
            return currentRelationship.properties;
        }

        @Override
        public void properties(PropertyCursor cursor, PropertySelection selection) {
            read.relationshipProperties(
                    currentRelationship.id,
                    currentRelationship.type,
                    currentRelationship.properties,
                    selection,
                    cursor);
        }

        @Override
        public void source(NodeCursor nodeCursor) {
            read.singleNode(sourceNodeReference(), nodeCursor);
        }

        @Override
        public void target(NodeCursor nodeCursor) {
            read.singleNode(targetNodeReference(), nodeCursor);
        }
    }

    private class ExpandIntoSelectionCursor extends DefaultCloseListenable implements RelationshipTraversalCursor {
        @Unmetered
        private final RelationshipTraversalCursor allRelationships;

        private final long otherNode;

        private final long firstNode;
        private final long secondNode;

        @Unmetered
        private final Direction expandDirection;

        private int degree;

        private HeapTrackingArrayList<Relationship> connections;
        private final ScopedMemoryTracker innerMemoryTracker;

        /**
         * @param otherNode the node we are expanding into
         * @param firstNode the first node given to connectingRelationships
         * @param secondNode the second node given to connectingRelationships
         * @param expandDirection the direction in which we perform the expand
         */
        ExpandIntoSelectionCursor(
                RelationshipTraversalCursor allRelationships,
                MemoryTracker outerMemoryTracker,
                long otherNode,
                long firstNode,
                long secondNode,
                Direction expandDirection) {
            this.allRelationships = allRelationships;
            this.otherNode = otherNode;
            this.firstNode = firstNode;
            this.secondNode = secondNode;
            this.expandDirection = expandDirection;
            this.innerMemoryTracker = new DefaultScopedMemoryTracker(outerMemoryTracker);
            this.connections = HeapTrackingArrayList.newArrayListWithInitialTrackedSize(
                    innerMemoryTracker, EXPAND_INTO_SELECTION_CURSOR_SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE);
        }

        @Override
        public void otherNode(NodeCursor cursor) {
            allRelationships.otherNode(cursor);
        }

        @Override
        public long originNodeReference() {
            return firstNode;
        }

        @Override
        public void removeTracer() {
            allRelationships.removeTracer();
        }

        @Override
        public void closeInternal() {
            degree = 0;
            connections = null;
            innerMemoryTracker.close();
        }

        @Override
        public long relationshipReference() {
            return allRelationships.relationshipReference();
        }

        @Override
        public int type() {
            return allRelationships.type();
        }

        @Override
        public long otherNodeReference() {
            return secondNode;
        }

        @Override
        public long sourceNodeReference() {
            return allRelationships.sourceNodeReference();
        }

        @Override
        public long targetNodeReference() {
            return allRelationships.targetNodeReference();
        }

        @Override
        public boolean next() {
            while (allRelationships.next()) {
                degree++;
                if (allRelationships.otherNodeReference() == otherNode) {
                    innerMemoryTracker.allocateHeap(Relationship.RELATIONSHIP_SHALLOW_SIZE);
                    connections.add(relationship(allRelationships));

                    return true;
                }
            }

            if (connections == null) {
                // This cursor is already closed
                return false;
            }

            // We hand over both the inner memory tracker (via connections) and the connection to the cache. Only the
            // shallow size of this cursor is discarded.
            long diff = innerMemoryTracker.estimatedHeapMemory() - EXPAND_INTO_SELECTION_CURSOR_SHALLOW_SIZE;
            long startNode = otherNode == secondNode ? firstNode : secondNode;
            degreeCache.put(startNode, expandDirection, degree);
            relationshipCache.add(firstNode, secondNode, direction, connections, diff);
            return false;
        }

        @Override
        public Reference propertiesReference() {
            return allRelationships.propertiesReference();
        }

        @Override
        public void properties(PropertyCursor cursor, PropertySelection selection) {
            allRelationships.properties(cursor, selection);
        }

        @Override
        public void setTracer(KernelReadTracer tracer) {
            allRelationships.setTracer(tracer);
        }

        @Override
        public void source(NodeCursor nodeCursor) {
            allRelationships.source(nodeCursor);
        }

        @Override
        public void target(NodeCursor nodeCursor) {
            allRelationships.target(nodeCursor);
        }

        @Override
        public boolean isClosed() {
            return connections == null || scopedMemoryTracker == null;
        }
    }

    private static final int DEFAULT_CAPACITY = 100000;

    static class NodeDegreeCache {
        private static final long FLIP_HIGH_BIT_MASK = 1L << 63;
        static final long DEGREE_CACHE_SHALLOW_SIZE = shallowSizeOfInstance(NodeDegreeCache.class);

        private final int capacity;
        private final MutableLongLongMap degreeCache;

        NodeDegreeCache(MemoryTracker memoryTracker) {
            this(DEFAULT_CAPACITY, memoryTracker);
        }

        NodeDegreeCache(int capacity, MemoryTracker memoryTracker) {
            this.capacity = capacity;
            memoryTracker.allocateHeap(DEGREE_CACHE_SHALLOW_SIZE);
            this.degreeCache = HeapTrackingCollections.newLongLongMap(memoryTracker);
        }

        public long getIfAbsentPut(long node, Direction direction, LongFunction0 update) {
            assert node >= 0;
            // if incoming we flip the highest bit in the node id
            long nodeWithDirection = direction == INCOMING ? FLIP_HIGH_BIT_MASK | node : node;

            if (degreeCache.size() >= capacity) {
                if (degreeCache.containsKey(nodeWithDirection)) {
                    return degreeCache.get(nodeWithDirection);
                } else {
                    return update.getAsLong();
                }
            } else {
                if (degreeCache.containsKey(nodeWithDirection)) {
                    return degreeCache.get(nodeWithDirection);
                } else {
                    long value = update.getAsLong();
                    degreeCache.put(nodeWithDirection, value);
                    return value;
                }
            }
        }

        public void put(long node, Direction direction, int degree) {
            assert node >= 0;
            // if incoming we flip the highest bit in the node id
            long nodeWithDirection = direction == INCOMING ? FLIP_HIGH_BIT_MASK | node : node;

            if (degreeCache.size() >= capacity) {
                if (degreeCache.containsKey(nodeWithDirection)) {
                    degreeCache.put(nodeWithDirection, degree);
                }
            } else {
                degreeCache.put(nodeWithDirection, degree);
            }
        }
    }

    static class RelationshipCache {
        static final long REL_CACHE_SHALLOW_SIZE = shallowSizeOfInstance(RelationshipCache.class);

        private final HeapTrackingUnifiedMap<Key, HeapTrackingArrayList<Relationship>> map;
        private final int capacity;
        private final MemoryTracker memoryTracker;

        RelationshipCache(int capacity, MemoryTracker memoryTracker) {
            this.capacity = capacity;
            this.memoryTracker = memoryTracker;
            this.memoryTracker.allocateHeap(REL_CACHE_SHALLOW_SIZE);
            this.map = HeapTrackingCollections.newMap(memoryTracker);
        }

        public void add(
                long start,
                long end,
                Direction direction,
                HeapTrackingArrayList<Relationship> relationships,
                long heapSizeOfRelationships) {
            if (map.size() < capacity) {
                map.put(key(start, end, direction), relationships);
                memoryTracker.allocateHeap(heapSizeOfRelationships);
                memoryTracker.allocateHeap(Key.KEY_SHALLOW_SIZE);
            }
        }

        /**
         * Read the relationships from the cache. Returns `null` if not cached.
         */
        public Iterator<Relationship> get(long start, long end, Direction direction) {
            HeapTrackingArrayList<Relationship> cachedValue = map.get(key(start, end, direction));
            return cachedValue == null ? null : cachedValue.iterator();
        }

        public static Key key(long startNode, long endNode, Direction direction) {
            long a, b;
            // if direction is BOTH than we keep the key sorted, otherwise direction is
            // important and we keep key as is
            if (direction == BOTH && startNode > endNode) {
                a = endNode;
                b = startNode;
            } else {
                a = startNode;
                b = endNode;
            }
            return new Key(a, b);
        }

        static class Key {
            static final long KEY_SHALLOW_SIZE = shallowSizeOfInstance(Key.class);

            private final long a, b;

            Key(long a, long b) {
                this.a = a;
                this.b = b;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                Key key = (Key) o;

                if (a != key.a) {
                    return false;
                }
                return b == key.b;
            }

            @Override
            public int hashCode() {
                int result = (int) (a ^ (a >>> 32));
                result = 31 * result + (int) (b ^ (b >>> 32));
                return result;
            }
        }
    }

    private static Relationship relationship(RelationshipTraversalCursor allRelationships) {
        return new Relationship(
                allRelationships.relationshipReference(),
                allRelationships.sourceNodeReference(),
                allRelationships.targetNodeReference(),
                allRelationships.propertiesReference(),
                allRelationships.type());
    }

    static class Relationship {
        static final long RELATIONSHIP_SHALLOW_SIZE = shallowSizeOfInstance(Relationship.class);

        private final long id, from, to;
        private final int type;
        private final Reference properties;

        private Relationship(long id, long from, long to, Reference properties, int type) {
            this.id = id;
            this.from = from;
            this.to = to;
            this.properties = properties;
            this.type = type;
        }
    }
}
