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
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.map.primitive.MutableLongIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryTracker;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.relationshipsCursor;
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
public class CachingExpandInto implements AutoCloseable
{
    private static final long RELATIONSHIP_SIZE = shallowSizeOfInstance( Relationship.class );

    private static final int EXPENSIVE_DEGREE = -1;

    private final RelationshipCache relationshipCache;
    private final NodeDegreeCache degreeCache;

    private final Read read;
    private final Direction direction;

    private final ScopedMemoryTracker scopedMemoryTracker;

    //NOTE: this constructor is here for legacy compiled runtime where we don't track memory
    //when we remove the legacy_compiled this should go as well.
    @Deprecated
    public CachingExpandInto( Read read, Direction direction )
    {
        this( read, direction, EmptyMemoryTracker.INSTANCE );
    }

    public CachingExpandInto( Read read, Direction direction, MemoryTracker memoryTracker )
    {
        this( read, direction, memoryTracker, DEFAULT_CAPACITY );
    }

    public CachingExpandInto( Read read, Direction direction, MemoryTracker memoryTracker, int capacity )
    {
        this.scopedMemoryTracker = new ScopedMemoryTracker( memoryTracker );
        this.read = read;
        this.direction = direction;
        this.relationshipCache = new RelationshipCache( capacity, scopedMemoryTracker );
        this.degreeCache = new NodeDegreeCache( capacity, scopedMemoryTracker );
    }

    @Override
    public void close() throws Exception
    {
        scopedMemoryTracker.close();
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
        List<Relationship> connections = relationshipCache.get( firstNode, secondNode, direction );

        if ( connections != null )
        {
            return new FromCachedSelectionCursor( connections.iterator(), read, firstNode, secondNode );
        }
        Direction reverseDirection = direction.reverse();
        //Check secondNode, will position nodeCursor at secondNode
        int secondDegree = degreeCache.getIfAbsentPut( secondNode,
                () -> calculateTotalDegreeIfCheap( read, secondNode, nodeCursor, reverseDirection, types ));

        if ( secondDegree == 0 )
        {
            return RelationshipTraversalCursor.EMPTY;
        }
        boolean secondNodeHasCheapDegrees = secondDegree != EXPENSIVE_DEGREE;

        //Check firstNode, note that nodeCursor is now pointing at firstNode
        if ( !singleNode( read, nodeCursor, firstNode ) )
        {
            return RelationshipTraversalCursor.EMPTY;
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
        return new ExpandIntoSelectionCursor( allRelationships, toNode, firstNode, secondNode );
    }

    private static class FromCachedSelectionCursor implements RelationshipTraversalCursor
    {
        private final Iterator<Relationship> relationships;
        private Relationship currentRelationship;
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
        }

        @Override
        public boolean next()
        {
            if ( relationships.hasNext() )
            {
                this.currentRelationship = relationships.next();
                return true;
            }
            else
            {
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
          //nothing to close
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
        public CloseListener getCloseListener()
        {
            return null;
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
        private final RelationshipTraversalCursor allRelationships;
        private final long otherNode;

        private final long firstNode;
        private final long secondNode;

        private final List<Relationship> connections = new ArrayList<>( 2 );

        /**
         * @param otherNode the node we are expanding into
         * @param firstNode the first node given to connectingRelationships
         * @param secondNode the second node given to connectingRelationships
         */
        ExpandIntoSelectionCursor( RelationshipTraversalCursor allRelationships, long otherNode, long firstNode, long secondNode )
        {
            this.allRelationships = allRelationships;
            this.otherNode = otherNode;
            this.firstNode = firstNode;
            this.secondNode = secondNode;
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
        public void close()
        {
            closeInternal();
            if ( closeListener != null )
            {
                closeListener.onClosed( this );
            }
        }

        @Override
        public void closeInternal()
        {
            allRelationships.close();
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
                    connections.add( relationship( allRelationships ) );

                    return true;
                }
            }

            relationshipCache.add( firstNode, secondNode, direction, connections );
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
        private final int capacity;
        private final MemoryTracker memoryTracker;
        private final MutableLongIntMap degreeCache = new LongIntHashMap();

        NodeDegreeCache( MemoryTracker memoryTracker )
        {
            this( DEFAULT_CAPACITY, memoryTracker );
        }

        NodeDegreeCache( int capacity, MemoryTracker memoryTracker )
        {
            this.capacity = capacity;
            this.memoryTracker = memoryTracker;
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
                    memoryTracker.allocateHeap( Long.BYTES + Integer.BYTES );
                    return value;
                }
            }
        }
    }

    static class RelationshipCache
    {
        private final MutableMap<Key,List<Relationship>> map = Maps.mutable.withInitialCapacity( 8 );
        private final int capacity;
        private final MemoryTracker memoryTracker;

        RelationshipCache( int capacity, MemoryTracker memoryTracker )
        {
            this.capacity = capacity;
            this.memoryTracker = memoryTracker;
        }

        public void add( long start, long end, Direction direction, List<Relationship> relationships )
        {
            if ( map.size() < capacity )
            {
                map.put( key( start, end, direction ), relationships );
                memoryTracker.allocateHeap( 2 * Long.BYTES + //two longs for the key
                        relationships.size() * RELATIONSHIP_SIZE ); //relationship.size * RELATIONSHIP_SIZE for the value
            }
        }

        public List<Relationship> get( long start, long end, Direction direction )
        {
            return map.get( key( start, end, direction ) );
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
