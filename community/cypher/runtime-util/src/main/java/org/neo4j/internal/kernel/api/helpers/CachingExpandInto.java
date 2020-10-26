/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.block.function.primitive.IntFunction0;
import org.eclipse.collections.api.map.primitive.MutableLongIntMap;
import org.github.jamm.Unmetered;

import java.util.Iterator;

import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingUnifiedMap;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CloseListener;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.newapi.Cursors;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryTracker;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.relationshipsCursor;
import static org.neo4j.memory.HeapEstimator.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

/**
 * Utility for performing Expand(Into)
 *
 * Expand(Into) is the operation of given two nodes, find all interconnecting relationships of a given type and direction.
 * This is often a computationally heavy operation so that given direction and types an instance of this class can be reused
 * and previously found connections will be cached and can significantly speed up traversals.
 */
@SuppressWarnings( {"unused", "UnnecessaryLocalVariable"} )
public class CachingExpandInto extends DefaultCloseListenable
{
    static final long CACHING_EXPAND_INTO_SHALLOW_SIZE =
            shallowSizeOfInstance( CachingExpandInto.class )
            + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;

    static final long EXPAND_INTO_SELECTION_CURSOR_SHALLOW_SIZE = shallowSizeOfInstance( ExpandIntoSelectionCursor.class );
    static final long FROM_CACHE_SELECTION_CURSOR_SHALLOW_SIZE = shallowSizeOfInstance( FromCachedSelectionCursor.class );

    private static final int EXPENSIVE_DEGREE = -1;

    private final RelationshipCache relationshipCache;
    private final NodeDegreeCache degreeCache;

    @Unmetered
    private final Read read;
    @Unmetered
    private final Direction direction;

    private final MemoryTracker scopedMemoryTracker;

    public CachingExpandInto( Read read, Direction direction, MemoryTracker memoryTracker )
    {
        this( read, direction, memoryTracker, DEFAULT_CAPACITY );
    }

    public CachingExpandInto( Read read, Direction direction, MemoryTracker memoryTracker, int capacity )
    {
        this.scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        this.scopedMemoryTracker.allocateHeap( CACHING_EXPAND_INTO_SHALLOW_SIZE );
        this.read = read;
        this.direction = direction;
        this.relationshipCache = new RelationshipCache( capacity, scopedMemoryTracker );
        this.degreeCache = new NodeDegreeCache( capacity, scopedMemoryTracker );
    }

    @Override
    public void closeInternal()
    {
        scopedMemoryTracker.close();
    }

    @Override
    public boolean isClosed()
    {
        return false;
    }

    /**
     * Creates a cursor for all connecting relationships given a first and a second node.
     *
     * @param nodeCursor Node cursor used in traversal
     * @param traversalCursor Traversal cursor used in traversal
     * @param firstNode The first node
     * @param secondNode The second node
     * @return The interconnecting relationships in the given direction with any of the given types.
     */
    public RelationshipTraversalCursor connectingRelationships(
            NodeCursor nodeCursor,
            RelationshipTraversalCursor traversalCursor,
            long firstNode,
            int[] types,
            long secondNode )
    {
        Iterator<Relationship> connections = relationshipCache.get( firstNode, secondNode, direction );

        if ( connections != null )
        {
            return new FromCachedSelectionCursor( connections, read, firstNode, secondNode );
        }
        Direction reverseDirection = direction.reverse();
        //Check secondNode, will position nodeCursor at secondNode
        int secondDegree = degreeCache.getIfAbsentPut( secondNode,
                () -> calculateTotalDegreeIfCheap( read, secondNode, nodeCursor, reverseDirection, types ));

        if ( secondDegree == 0 )
        {
            return Cursors.emptyTraversalCursor( read );
        }
        boolean secondNodeHasCheapDegrees = secondDegree != EXPENSIVE_DEGREE;

        //Check firstNode, note that nodeCursor is now pointing at firstNode
        if ( !singleNode( read, nodeCursor, firstNode ) )
        {
            return Cursors.emptyTraversalCursor( read );
        }
        boolean firstNodeHasCheapDegrees = nodeCursor.supportsFastDegreeLookup();

        //Both can determine degree cheaply, start with the one with the lesser degree
        if ( firstNodeHasCheapDegrees && secondNodeHasCheapDegrees )
        {
            //Note that we have already position the cursor at firstNode
            int firstDegree = degreeCache.getIfAbsentPut( firstNode, () -> calculateTotalDegree( nodeCursor, direction, types ));
            long toNode;
            Direction relDirection;
            if ( firstDegree < secondDegree )
            {
                // Everything is correctly positioned
                toNode = secondNode;
                relDirection = direction;
            }
            else
            {
                //cursor is already pointing at firstNode
                singleNode( read, nodeCursor, secondNode );
                toNode = firstNode;
                relDirection = reverseDirection;
            }

            return connectingRelationshipsCursor( relationshipsCursor( traversalCursor, nodeCursor, types, relDirection ), toNode, firstNode, secondNode );
        }
        else if ( secondNodeHasCheapDegrees )
        {
            long toNode = secondNode;
            return connectingRelationshipsCursor( relationshipsCursor( traversalCursor, nodeCursor, types, direction ), toNode, firstNode, secondNode );
        }
        else if ( firstNodeHasCheapDegrees )
        {
            //must move to secondNode
            singleNode( read, nodeCursor, secondNode );
            long toNode = firstNode;
            return connectingRelationshipsCursor( relationshipsCursor( traversalCursor, nodeCursor, types, reverseDirection ), toNode, firstNode, secondNode );
        }
        else
        {
            //Both are sparse
            long toNode = secondNode;
            return connectingRelationshipsCursor( relationshipsCursor( traversalCursor, nodeCursor, types, direction ), toNode, firstNode, secondNode );
        }
    }

    public RelationshipTraversalCursor connectingRelationships(
            CursorFactory cursors,
            NodeCursor nodeCursor,
            long fromNode,
            int[] types,
            long toNode,
            PageCursorTracer cursorTracer )
    {
        return connectingRelationships( nodeCursor, cursors.allocateRelationshipTraversalCursor( cursorTracer ), fromNode, types, toNode );
    }

    private int calculateTotalDegreeIfCheap( Read read, long node, NodeCursor nodeCursor, Direction direction,
            int[] types )
    {
        if ( !singleNode( read, nodeCursor, node ) )
        {
            return 0;
        }
        if ( !nodeCursor.supportsFastDegreeLookup() )
        {
            return EXPENSIVE_DEGREE;
        }
        return calculateTotalDegree( nodeCursor, direction, types );
    }

    private int calculateTotalDegree( NodeCursor nodeCursor, Direction direction, int[] types )
    {
        return nodeCursor.degree( selection( types, direction ) );
    }

    private static boolean singleNode( Read read, NodeCursor nodeCursor, long node )
    {
        read.singleNode( node, nodeCursor );
        return nodeCursor.next();
    }

    private RelationshipTraversalCursor connectingRelationshipsCursor(
            final RelationshipTraversalCursor allRelationships,
            final long toNode,
            final long firstNode,
            final long secondNode )
    {
        return new ExpandIntoSelectionCursor( allRelationships, scopedMemoryTracker, toNode, firstNode, secondNode );
    }

    private class FromCachedSelectionCursor implements RelationshipTraversalCursor
    {
        @Unmetered
        private Iterator<Relationship> relationships;
        private Relationship currentRelationship;
        @Unmetered
        private final Read read;
        private int token;

        private final long firstNode;
        private final long secondNode;

        FromCachedSelectionCursor( Iterator<Relationship> relationships, Read read, long firstNode, long secondNode )
        {
            this.relationships = relationships;
            this.read = read;
            this.firstNode = firstNode;
            this.secondNode = secondNode;
            scopedMemoryTracker.allocateHeap( FROM_CACHE_SELECTION_CURSOR_SHALLOW_SIZE );
        }

        @Override
        public boolean next()
        {
            if ( relationships != null && relationships.hasNext() )
            {
                this.currentRelationship = relationships.next();
                return true;
            }
            else
            {
                close();
                return false;
            }
        }

        @Override
        public void removeTracer()
        {
        }

        @Override
        public void otherNode( NodeCursor cursor )
        {
            read.singleNode( otherNodeReference(), cursor );
        }

        @Override
        public long originNodeReference()
        {
            return currentRelationship.from;
        }

        @Override
        public void setTracer( KernelReadTracer tracer )
        {
            //these are cached no need to trace anything
        }

        @Override
        public void close()
        {
            if ( relationships != null )
            {
                relationships = null;
                scopedMemoryTracker.releaseHeap( FROM_CACHE_SELECTION_CURSOR_SHALLOW_SIZE );
            }
        }

        @Override
        public void closeInternal()
        {
            //nothing to close
        }

        @Override
        public boolean isClosed()
        {
            return false;
        }

        @Override
        public void setCloseListener( CloseListener closeListener )
        {
            //nothing close, just hand ourselves back to the closeListener so that
            //any tracking of this resource can be removed.
            if ( closeListener != null )
            {
                closeListener.onClosed( this );
            }
        }

        @Override
        public void setToken( int token )
        {
            this.token = token;
        }

        @Override
        public int getToken()
        {
            return token;
        }

        @Override
        public long relationshipReference()
        {
            return currentRelationship.id;
        }

        @Override
        public int type()
        {
            return currentRelationship.type;
        }

        @Override
        public long otherNodeReference()
        {
            return currentRelationship.from == firstNode ? secondNode : firstNode;
        }

        @Override
        public long sourceNodeReference()
        {
            return currentRelationship.from;
        }

        @Override
        public long targetNodeReference()
        {
            return currentRelationship.to;
        }

        @Override
        public long propertiesReference()
        {
            return currentRelationship.properties;
        }

        @Override
        public void properties( PropertyCursor cursor )
        {
           read.relationshipProperties( currentRelationship.id, currentRelationship.properties, cursor );
        }

        @Override
        public void source( NodeCursor nodeCursor )
        {
            read.singleNode( sourceNodeReference(), nodeCursor );
        }

        @Override
        public void target( NodeCursor nodeCursor )
        {
            read.singleNode( targetNodeReference(), nodeCursor );
        }
    }

    private class ExpandIntoSelectionCursor extends DefaultCloseListenable implements RelationshipTraversalCursor
    {
        @Unmetered
        private final RelationshipTraversalCursor allRelationships;
        private final long otherNode;

        private final long firstNode;
        private final long secondNode;

        private HeapTrackingArrayList<Relationship> connections;
        private final ScopedMemoryTracker innerMemoryTracker;

        /**
         * @param otherNode the node we are expanding into
         * @param firstNode the first node given to connectingRelationships
         * @param secondNode the second node given to connectingRelationships
         */
        ExpandIntoSelectionCursor( RelationshipTraversalCursor allRelationships,
                                   MemoryTracker outerMemoryTracker,
                                   long otherNode,
                                   long firstNode,
                                   long secondNode )
        {
            this.allRelationships = allRelationships;
            this.otherNode = otherNode;
            this.firstNode = firstNode;
            this.secondNode = secondNode;
            this.innerMemoryTracker = new ScopedMemoryTracker( outerMemoryTracker );
            this.connections = HeapTrackingArrayList.newArrayList( innerMemoryTracker );
            innerMemoryTracker.allocateHeap( EXPAND_INTO_SELECTION_CURSOR_SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE );
        }

        @Override
        public void otherNode( NodeCursor cursor )
        {
            allRelationships.otherNode( cursor );
        }

        @Override
        public long originNodeReference()
        {
            return allRelationships.originNodeReference();
        }

        @Override
        public void removeTracer()
        {
            allRelationships.removeTracer();
        }

        @Override
        public void closeInternal()
        {
            allRelationships.close();
            connections = null;
            innerMemoryTracker.close();
        }

        @Override
        public long relationshipReference()
        {
            return allRelationships.relationshipReference();
        }

        @Override
        public int type()
        {
            return allRelationships.type();
        }

        @Override
        public long otherNodeReference()
        {
            return allRelationships.otherNodeReference();
        }

        @Override
        public long sourceNodeReference()
        {
            return allRelationships.sourceNodeReference();
        }

        @Override
        public long targetNodeReference()
        {
            return allRelationships.targetNodeReference();
        }

        @Override
        public boolean next()
        {
            while ( allRelationships.next() )
            {
                if ( allRelationships.otherNodeReference() == otherNode )
                {
                    innerMemoryTracker.allocateHeap( Relationship.RELATIONSHIP_SHALLOW_SIZE );
                    connections.add( relationship( allRelationships ) );

                    return true;
                }
            }

            if ( connections == null )
            {
                // This cursor is already closed
                return false;
            }

            // We hand over both the inner memory tracker (via connections) and the connection to the cache. Only the shallow size of this cursor is discarded.
            long diff = innerMemoryTracker.estimatedHeapMemory() - EXPAND_INTO_SELECTION_CURSOR_SHALLOW_SIZE;
            relationshipCache.add( firstNode, secondNode, direction, connections, diff );
            close();
            return false;
        }

        @Override
        public long propertiesReference()
        {
            return allRelationships.propertiesReference();
        }

        @Override
        public void properties( PropertyCursor cursor )
        {
            allRelationships.properties( cursor );
        }

        @Override
        public void setTracer( KernelReadTracer tracer )
        {
            allRelationships.setTracer( tracer );
        }

        @Override
        public void source( NodeCursor nodeCursor )
        {
            allRelationships.source( nodeCursor );
        }

        @Override
        public void target( NodeCursor nodeCursor )
        {
            allRelationships.target( nodeCursor );
        }

        @Override
        public boolean isClosed()
        {
            return allRelationships.isClosed();
        }
    }

    private static final int DEFAULT_CAPACITY = 100000;

    static class NodeDegreeCache
    {
        static final long DEGREE_CACHE_SHALLOW_SIZE = shallowSizeOfInstance( NodeDegreeCache.class );

        private final int capacity;
        private final MutableLongIntMap degreeCache;

        NodeDegreeCache( MemoryTracker memoryTracker )
        {
            this( DEFAULT_CAPACITY, memoryTracker );
        }

        NodeDegreeCache( int capacity, MemoryTracker memoryTracker )
        {
            this.capacity = capacity;
            memoryTracker.allocateHeap( DEGREE_CACHE_SHALLOW_SIZE );
            this.degreeCache = HeapTrackingCollections.newLongIntMap( memoryTracker );
        }

        public int getIfAbsentPut( long node, IntFunction0 update )
        {
            //cache is full, but must check if node has already been cached
            if ( degreeCache.size() >= capacity )
            {
                if ( degreeCache.containsKey( node ) )
                {
                    return degreeCache.get( node );
                }
                else
                {
                    return update.getAsInt();
                }
            }
            else
            {
                if ( degreeCache.containsKey( node ) )
                {
                    return degreeCache.get( node );
                }
                else
                {
                    int value = update.getAsInt();
                    degreeCache.put( node, value );
                    return value;
                }
            }
        }
    }

    static class RelationshipCache
    {
        static final long REL_CACHE_SHALLOW_SIZE = shallowSizeOfInstance( RelationshipCache.class );

        private final HeapTrackingUnifiedMap<Key,HeapTrackingArrayList<Relationship>> map;
        private final int capacity;
        private final MemoryTracker memoryTracker;

        RelationshipCache( int capacity, MemoryTracker memoryTracker )
        {
            this.capacity = capacity;
            this.memoryTracker = memoryTracker;
            this.memoryTracker.allocateHeap( REL_CACHE_SHALLOW_SIZE );
            this.map = HeapTrackingCollections.newMap( memoryTracker );
        }

        public void add( long start, long end, Direction direction, HeapTrackingArrayList<Relationship> relationships, long heapSizeOfRelationships )
        {
            if ( map.size() < capacity )
            {
                map.put( key( start, end, direction ), relationships );
                memoryTracker.allocateHeap( heapSizeOfRelationships );
                memoryTracker.allocateHeap( Key.KEY_SHALLOW_SIZE );
            }
        }

        /**
         * Read the relationships from the cache. Returns `null` if not cached.
         */
        public Iterator<Relationship> get( long start, long end, Direction direction )
        {
            HeapTrackingArrayList<Relationship> cachedValue = map.get( key( start, end, direction ) );
            return cachedValue == null ? null : cachedValue.iterator();
        }

        public Key key( long startNode, long endNode, Direction direction )
        {
            long a, b;
            // if direction is BOTH than we keep the key sorted, otherwise direction is
            // important and we keep key as is
            if ( direction == BOTH && startNode > endNode )
            {
                a = endNode;
                b = startNode;
            }
            else
            {
                a = startNode;
                b = endNode;
            }
            return new Key( a, b );
        }

        static class Key
        {
            static long KEY_SHALLOW_SIZE = shallowSizeOfInstance(Key.class);

            private final long a, b;

            Key( long a, long b )
            {
                this.a = a;
                this.b = b;
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }

                Key key = (Key) o;

                if ( a != key.a )
                {
                    return false;
                }
                return b == key.b;

            }

            @Override
            public int hashCode()
            {
                int result = (int) (a ^ (a >>> 32));
                result = 31 * result + (int) (b ^ (b >>> 32));
                return result;
            }
        }
    }

    private static Relationship relationship( RelationshipTraversalCursor allRelationships )
    {
        return new Relationship(
                allRelationships.relationshipReference(),
                allRelationships.sourceNodeReference(),
                allRelationships.targetNodeReference(),
                allRelationships.propertiesReference(),
                allRelationships.type() );
    }

    private static class Relationship
    {
        static final long RELATIONSHIP_SHALLOW_SIZE = shallowSizeOfInstance( Relationship.class );

        private final long id, from, to, properties;
        private final int type;

        private Relationship( long id, long from, long to, long properties, int type )
        {
            this.id = id;
            this.from = from;
            this.to = to;
            this.properties = properties;
            this.type = type;
        }
    }
}
