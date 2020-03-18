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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.RelationshipSelection;

/**
 * Utilities for dealing with RelationshipSelectionCursor and corresponding iterators.
 */
public final class RelationshipSelections
{

    private RelationshipSelections()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    /**
     * Returns an outgoing selection cursor given the provided node cursor and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param cursorTracer underlying page cursor tracer
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor outgoingCursor( CursorFactory cursors,
                                                              NodeCursor node,
                                                              int[] types,
                                                              PageCursorTracer cursorTracer )
    {
        return relationshipsCursor( cursors.allocateRelationshipTraversalCursor( cursorTracer ), node, types, Direction.OUTGOING );
    }

    /**
     * Returns an outgoing selection cursor given the provided cursors and relationship types.
     * @param traversalCursor A traversal a cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor outgoingCursor( RelationshipTraversalCursor traversalCursor,
            NodeCursor node,
            int[] types )
    {
        return relationshipsCursor( traversalCursor, node, types, Direction.OUTGOING );
    }

    /**
     * Returns an incoming selection cursor given the provided cursors and relationship types.
     * @param traversalCursor A traversal a cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor incomingCursor( RelationshipTraversalCursor traversalCursor,
            NodeCursor node,
            int[] types )
    {
        return relationshipsCursor( traversalCursor, node, types, Direction.INCOMING );
    }

    /**
     * Returns an incoming selection cursor given the provided node cursor and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param cursorTracer underlying page cursor tracer
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor incomingCursor( CursorFactory cursors,
                                                              NodeCursor node,
                                                              int[] types,
                                                              PageCursorTracer cursorTracer )
    {
        return relationshipsCursor( cursors.allocateRelationshipTraversalCursor( cursorTracer ), node, types, Direction.INCOMING );
    }

    /**
     * Returns a multi-directed selection cursor given the provided node cursor and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param cursorTracer underlying page cursor tracer
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor allCursor( CursorFactory cursors,
                                                         NodeCursor node,
                                                         int[] types,
                                                         PageCursorTracer cursorTracer )
    {
        return relationshipsCursor( cursors.allocateRelationshipTraversalCursor( cursorTracer ), node, types, Direction.BOTH );
    }

    /**
     * Returns a multi-directed selection cursor given the provided cursors and relationship types.
     * @param traversalCursor A traversal a cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipTraversalCursor allCursor( RelationshipTraversalCursor traversalCursor,
                                                         NodeCursor node,
                                                         int[] types )
    {
        node.relationships( traversalCursor, RelationshipSelection.selection( types, Direction.BOTH ) );
        return traversalCursor;
    }

    public static RelationshipTraversalCursor relationshipsCursor( RelationshipTraversalCursor traversalCursor, NodeCursor node, int[] types,
            Direction outgoing )
    {
        try
        {
            node.relationships( traversalCursor, RelationshipSelection.selection( types, outgoing ) );
            return traversalCursor;
        }
        catch ( Throwable t )
        {
            traversalCursor.close();
            throw t;
        }
    }

    /**
     * Returns an outgoing resource iterator given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param factory factory for creating instance of generic type T
     * @param cursorTracer underlying page cursor tracer
     * @return An iterator that allows traversing the relationship chain.
     */
    public static <T> ResourceIterator<T> outgoingIterator( CursorFactory cursors,
                                                            NodeCursor node,
                                                            int[] types,
                                                            RelationshipFactory<T> factory,
                                                            PageCursorTracer cursorTracer )
    {
        return new RelationshipEntityIterator<>( outgoingCursor( cursors, node, types, cursorTracer ), factory );
    }

    /**
     * Returns an incoming resource iterator given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param factory factory for creating instance of generic type T
     * @param cursorTracer underlying page cursor tracer
     * @return An iterator that allows traversing the relationship chain.
     */
    public static <T> ResourceIterator<T> incomingIterator( CursorFactory cursors,
                                                            NodeCursor node,
                                                            int[] types,
                                                            RelationshipFactory<T> factory,
                                                            PageCursorTracer cursorTracer )
    {
        return new RelationshipEntityIterator<>( incomingCursor( cursors, node, types, cursorTracer ), factory );
    }

    /**
     * Returns a multi-directed resource iterator given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param factory factory for creating instance of generic type T
     * @param cursorTracer underlying page cursor tracer
     * @return An iterator that allows traversing the relationship chain.
     */
    public static <T> ResourceIterator<T> allIterator( CursorFactory cursors,
                                                       NodeCursor node,
                                                       int[] types,
                                                       RelationshipFactory<T> factory,
                                                       PageCursorTracer cursorTracer )
    {
        return new RelationshipEntityIterator<>( allCursor( cursors, node, types, cursorTracer ), factory );
    }

    private static class RelationshipEntityIterator<T> extends PrefetchingResourceIterator<T>
    {
        private final RelationshipTraversalCursor relationshipTraversalCursor;
        private final RelationshipFactory<T> factory;

        RelationshipEntityIterator( RelationshipTraversalCursor relationshipTraversalCursor, RelationshipFactory<T> factory )
        {
            this.relationshipTraversalCursor = relationshipTraversalCursor;
            this.factory = factory;
        }

        @Override
        public void close()
        {
            relationshipTraversalCursor.close();
        }

        @Override
        protected T fetchNextOrNull()
        {
            if ( relationshipTraversalCursor.next() )
            {
                return factory.relationship( relationshipTraversalCursor.relationshipReference(), relationshipTraversalCursor.sourceNodeReference(),
                        relationshipTraversalCursor.type(), relationshipTraversalCursor.targetNodeReference() );
            }
            close();
            return null;
        }
    }
}
