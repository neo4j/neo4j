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

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;

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

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.relationshipsCursor;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

/**
 * Utility for performing Expand(Into)
 *
 * Expand(Into) is the operation of given two nodes, find all interconnecting relationships of a given type and direction.
 * This is often a computationally heavy operation so that given direction and types an instance of this class can be reused
 * and previously found connections will be cached and can significantly speed up traversals.
 */
@SuppressWarnings( "unused" )
public class CachingExpandInto
{
    private static final int EXPENSIVE_DEGREE = -1;

    private RelationshipCache relationshipCache = new RelationshipCache();
    private long fromNode = -1L;
    private long toNode = -1L;

    private final Read read;
    private final Direction direction;

    public CachingExpandInto( Read read, Direction direction )
    {
        this.read = read;
        this.direction = direction;
    }

    /**
     * Creates a cursor for all connecting relationships given a start- and an endnode.
     *
     * @param nodeCursor Node cursor used in traversal
     * @param traversalCursor Traversal cursor used in traversal
     * @param fromNode The start node
     * @param toNode The end node
     * @return The interconnecting relationships in the given direction with any of the given types.
     */
    public RelationshipTraversalCursor connectingRelationships(
            NodeCursor nodeCursor,
            RelationshipTraversalCursor traversalCursor,
            long fromNode,
            int[] types,
            long toNode )
    {
        assert this.fromNode < 0L && this.toNode < 0L;
        List<Relationship> connections = relationshipCache.get( fromNode, toNode, direction );

        this.fromNode = fromNode;
        this.toNode = toNode;

        if ( connections != null )
        {
            return new FromCachedSelectionCursor( connections.iterator(), read );
        }
        Direction reverseDirection = direction.reverse();
        //Check toNode, will position nodeCursor at toNode
        int toDegree = calculateTotalDegreeIfCheap( read, toNode, nodeCursor, reverseDirection, types );
        if ( toDegree == 0 )
        {
            return RelationshipTraversalCursor.EMPTY;
        }
        boolean toNodeHasCheapDegrees = toDegree != EXPENSIVE_DEGREE;

        //Check fromNode, note that nodeCursor is now pointing at fromNode
        if ( !singleNode( read, nodeCursor, fromNode ) )
        {
            return RelationshipTraversalCursor.EMPTY;
        }
        boolean fromNodeHasCheapDegrees = nodeCursor.hasCheapDegrees();

        //Both can determine degree cheaply, start with the one with the lesser degree
        if ( fromNodeHasCheapDegrees && toNodeHasCheapDegrees )
        {
            //Note that we have already position the cursor at fromNode
            int fromDegree = calculateTotalDegree( nodeCursor, direction, types );
            long startNode;
            long endNode;
            Direction relDirection;
            if ( fromDegree < toDegree )
            {
                // Everything is correctly positioned
                endNode = toNode;
                relDirection = direction;
            }
            else
            {
                //cursor is already pointing at fromNode
                singleNode( read, nodeCursor, toNode );
                endNode = fromNode;
                relDirection = reverseDirection;
            }

            return connectingRelationshipsCursor( relationshipsCursor( traversalCursor, nodeCursor, types, relDirection ), endNode );
        }
        else if ( toNodeHasCheapDegrees )
        {
            return connectingRelationshipsCursor( relationshipsCursor( traversalCursor, nodeCursor, types, direction ), toNode );
        }
        else if ( fromNodeHasCheapDegrees )
        {
            //must move to toNode
            singleNode( read, nodeCursor, toNode );
            return connectingRelationshipsCursor( relationshipsCursor( traversalCursor, nodeCursor, types, reverseDirection ), fromNode );
        }
        else
        {
            //Both are sparse
            return connectingRelationshipsCursor( relationshipsCursor( traversalCursor, nodeCursor, types, direction ), toNode );
        }
    }

    private void done()
    {
        this.toNode = this.fromNode = -1L;
    }

    public RelationshipTraversalCursor connectingRelationships(
            CursorFactory cursors,
            NodeCursor nodeCursor,
            long fromNode,
            int[] types,
            long toNode,
            PageCursorTracer cursorTracer )
    {
        return connectingRelationships( nodeCursor, cursors.allocateRelationshipTraversalCursor(), fromNode, types, toNode );
    }

    private int calculateTotalDegreeIfCheap( Read read, long node, NodeCursor nodeCursor, Direction direction,
            int[] types )
    {
        if ( !singleNode( read, nodeCursor, node ) )
        {
            return 0;
        }
        if ( !nodeCursor.hasCheapDegrees() )
        {
            return EXPENSIVE_DEGREE;
        }
        return calculateTotalDegree( nodeCursor, direction, types );
    }

    private int calculateTotalDegree( NodeCursor nodeCursor, Direction direction, int[] types )
    {
        return nodeCursor.degrees( selection( types, direction ) ).degree( direction );
    }

    private static boolean singleNode( Read read, NodeCursor nodeCursor, long node )
    {
        read.singleNode( node, nodeCursor );
        return nodeCursor.next();
    }

    private RelationshipTraversalCursor connectingRelationshipsCursor(
            final RelationshipTraversalCursor allRelationships, final long toNode )
    {
        return new ExpandIntoSelectionCursor( allRelationships, toNode );
    }

    private class FromCachedSelectionCursor implements RelationshipTraversalCursor
    {
        private final Iterator<Relationship> relationships;
        private Relationship currentRelationship;
        private final Read read;
        private int token;

        FromCachedSelectionCursor( Iterator<Relationship> relationships, Read read )
        {
            this.relationships = relationships;
            this.read = read;
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
                done();
                return false;
            }
        }

        @Override
        public void setTracer( KernelReadTracer tracer )
        {
            //these are cached no need to trace anything
        }

        @Override
        public void removeTracer()
        {
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
            //nothing close, nothing to listen to
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
        public void otherNode( NodeCursor cursor )
        {
            read.singleNode( otherNodeReference(), cursor );
        }

        @Override
        public long otherNodeReference()
        {
            return currentRelationship.from == fromNode ? toNode : fromNode;
        }

        @Override
        public long originNodeReference()
        {
            return currentRelationship.from;
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

        private List<Relationship> connections = new ArrayList<>( 2 );

        ExpandIntoSelectionCursor( RelationshipTraversalCursor allRelationships, long otherNode )
        {
            this.allRelationships = allRelationships;
            this.otherNode = otherNode;
        }

        @Override
        public void close()
        {
            closeInternal();
            CloseListener closeListener = allRelationships.getCloseListener();
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
        public void otherNode( NodeCursor cursor )
        {
            allRelationships.otherNode( cursor );
        }

        @Override
        public long otherNodeReference()
        {
            return allRelationships.otherNodeReference();
        }

        @Override
        public long originNodeReference()
        {
            return allRelationships.originNodeReference();
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

            relationshipCache.add( fromNode, toNode, direction, connections );
            done();
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
        public void removeTracer()
        {
            allRelationships.removeTracer();
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

    static class RelationshipCache
    {
        private static final int DEFAULT_CAPACITY = 100000;

        private final MutableMap<Key,List<Relationship>> map = Maps.mutable.withInitialCapacity( 8 );
        private final int capacity;

        RelationshipCache()
        {
            this( DEFAULT_CAPACITY );
        }

        RelationshipCache( int capacity )
        {
            this.capacity = capacity;
        }

        public void add( long start, long end, Direction direction, List<Relationship> relationships )
        {
            if ( map.size() < capacity )
            {
                map.put( key( start, end, direction ), relationships );
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

    static Relationship relationship( RelationshipTraversalCursor allRelationships )
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
