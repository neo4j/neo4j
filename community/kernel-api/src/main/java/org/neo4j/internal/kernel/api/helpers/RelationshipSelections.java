/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.IOUtils;

/**
 * Utilities for dealing with RelationshipSelectionCursor and corresponding iterators.
 */
public final class RelationshipSelections
{
    static final long UNINITIALIZED = -2L;
    static final long NO_ID = -1L;

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
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor outgoingCursor( CursorFactory cursors,
                                                              NodeCursor node,
                                                              int[] types )
    {
        if ( node.isDense() )
        {
            return outgoingDenseCursor(
                    cursors.allocateRelationshipGroupCursor(),
                    cursors.allocateRelationshipTraversalCursor(),
                    node,
                    types );
        }
        else
        {
            return outgoingSparseCursor( cursors.allocateRelationshipTraversalCursor(), node, types );
        }
    }

    /**
     * Returns an outgoing selection cursor given the provided cursors and relationship types.
     * @param groupCursor A group cursor that will be used when traversing
     * @param traversalCursor A traversal a cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor outgoingCursor( RelationshipGroupCursor groupCursor,
                                                              RelationshipTraversalCursor traversalCursor,
                                                              NodeCursor node,
                                                              int[] types )
    {
        if ( node.isDense() )
        {
            return outgoingDenseCursor( groupCursor, traversalCursor, node, types );
        }
        else
        {
            return outgoingSparseCursor( traversalCursor, node, types );
        }
    }

    /**
     * Returns an outgoing selection cursor for a sparse node given the provided cursors and relationship types.
     * @param traversal A cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor outgoingSparseCursor( RelationshipTraversalCursor traversal,
                                                                    NodeCursor node,
                                                                    int[] types )
    {
        assert !node.isDense();
        RelationshipSparseSelectionCursor selectionCursor = new RelationshipSparseSelectionCursor();
        try
        {
            setupOutgoingSparse( selectionCursor, traversal, node, types );
        }
        catch ( Throwable t )
        {
            traversal.close();
            throw t;
        }
        return selectionCursor;
    }

    /**
     * Returns an outgoing selection cursor for a dense node given the provided cursors and relationship types.
     * @param group A cursor that will be used when traversing
     * @param traversal A cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor outgoingDenseCursor( RelationshipGroupCursor group,
                                                                   RelationshipTraversalCursor traversal,
                                                                   NodeCursor node,
                                                                   int[] types )
    {
        assert node.isDense();
        RelationshipDenseSelectionCursor selectionCursor = new RelationshipDenseSelectionCursor();

        try
        {
            setupOutgoingDense( selectionCursor, group, traversal, node, types );
        }
        catch ( Throwable t )
        {
            IOUtils.<Cursor>closeAllUnchecked( group, traversal );
            throw t;
        }
        return selectionCursor;
    }

    /**
     * Returns an incoming selection cursor given the provided node cursor and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor incomingCursor( CursorFactory cursors,
                                                              NodeCursor node,
                                                              int[] types )
    {
        if ( node.isDense() )
        {
            return incomingDenseCursor( cursors.allocateRelationshipGroupCursor(),
                    cursors.allocateRelationshipTraversalCursor(), node, types );
        }
        else
        {
            return incomingSparseCursor( cursors.allocateRelationshipTraversalCursor(), node, types );
        }
    }

    /**
     * Returns an incoming selection cursor given the provided cursors and relationship types.
     * @param groupCursor A group cursor that will be used when traversing
     * @param traversalCursor A traversal a cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor incomingCursor( RelationshipGroupCursor groupCursor,
                                                              RelationshipTraversalCursor traversalCursor,
                                                              NodeCursor node,
                                                              int[] types )
    {
        if ( node.isDense() )
        {
            return incomingDenseCursor( groupCursor, traversalCursor, node, types );
        }
        else
        {
            return incomingSparseCursor( traversalCursor, node, types );
        }
    }

    /**
     * Returns an incoming selection cursor for a sparse node given the provided cursors and relationship types.
     * @param traversal A cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor incomingSparseCursor( RelationshipTraversalCursor traversal,
                                                                    NodeCursor node,
                                                                    int[] types )
    {
        assert !node.isDense();
        RelationshipSparseSelectionCursor selectionCursor = new RelationshipSparseSelectionCursor();

        try
        {
            setupIncomingSparse( selectionCursor, traversal, node, types );
        }
        catch ( Throwable t )
        {
            traversal.close();
            throw t;
        }
        return selectionCursor;
    }

    /**
     * Returns an incoming selection cursor for a dense node given the provided cursors and relationship types.
     * @param group A cursor that will be used when traversing
     * @param traversal A cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor incomingDenseCursor( RelationshipGroupCursor group,
                                                                   RelationshipTraversalCursor traversal,
                                                                   NodeCursor node,
                                                                   int[] types )
    {
        assert node.isDense();
        RelationshipDenseSelectionCursor selectionCursor = new RelationshipDenseSelectionCursor();
        try
        {
            setupIncomingDense( selectionCursor, group, traversal, node, types );
        }
        catch ( Throwable t )
        {
            IOUtils.<Cursor>closeAllUnchecked( group, traversal );
            throw t;
        }
        return selectionCursor;
    }

    /**
     * Returns a multi-directed selection cursor given the provided node cursor and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor allCursor( CursorFactory cursors,
                                                         NodeCursor node,
                                                         int[] types )
    {
        if ( node.isDense() )
        {
            return allDenseCursor( cursors.allocateRelationshipGroupCursor(),
                    cursors.allocateRelationshipTraversalCursor(), node, types );
        }
        else
        {
            return allSparseCursor( cursors.allocateRelationshipTraversalCursor(), node, types );
        }
    }

    /**
     * Returns a multi-directed selection cursor given the provided cursors and relationship types.
     * @param groupCursor A group cursor that will be used when traversing
     * @param traversalCursor A traversal a cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor allCursor( RelationshipGroupCursor groupCursor,
                                                         RelationshipTraversalCursor traversalCursor,
                                                         NodeCursor node,
                                                         int[] types )
    {
        if ( node.isDense() )
        {
            return allDenseCursor( groupCursor, traversalCursor, node, types );
        }
        else
        {
            return allSparseCursor( traversalCursor, node, types );
        }
    }

    /**
     * Returns a multi-directed selection cursor for a sparse node given the provided cursors and relationship types.
     * @param traversal A cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor allSparseCursor( RelationshipTraversalCursor traversal,
                                                               NodeCursor node,
                                                               int[] types )
    {
        assert !node.isDense();
        RelationshipSparseSelectionCursor selectionCursor = new RelationshipSparseSelectionCursor();
        try
        {
            setupAllSparse( selectionCursor, traversal, node, types );
        }
        catch ( Throwable t )
        {
            traversal.close();
            throw t;
        }
        return selectionCursor;
    }

    /**
     * Returns a multi-directed selection cursor for a dense node given the provided cursors and relationship types.
     * @param group A cursor that will be used when traversing
     * @param traversal A cursor that will be used when traversing
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor allDenseCursor( RelationshipGroupCursor group,
                                                              RelationshipTraversalCursor traversal,
                                                              NodeCursor node,
                                                              int[] types )
    {
        assert node.isDense();
        RelationshipDenseSelectionCursor selectionCursor = new RelationshipDenseSelectionCursor();
        try
        {
            setupAllDense( selectionCursor, group, traversal, node, types );
        }
        catch ( Throwable t )
        {
            IOUtils.<Cursor>closeAllUnchecked( group, traversal );
            throw t;
        }
        return selectionCursor;
    }

    /**
     * Returns an outgoing resource iterator given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param factory factory for creating instance of generic type T
     * @return An iterator that allows traversing the relationship chain.
     */
    public static <T> ResourceIterator<T> outgoingIterator( CursorFactory cursors,
                                                            NodeCursor node,
                                                            int[] types,
                                                            RelationshipFactory<T> factory )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionIterator<T> selectionIterator =
                    new RelationshipDenseSelectionIterator<>( factory );
            RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
            RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor();
            try
            {
                setupOutgoingDense( selectionIterator, group, traversal, node, types );
            }
            catch ( Throwable t )
            {
                IOUtils.<Cursor>closeAllUnchecked( group, traversal );
                throw t;
            }
            return selectionIterator;
        }
        else
        {
            RelationshipSparseSelectionIterator<T> selectionIterator =
                    new RelationshipSparseSelectionIterator<>( factory );
            RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor();
            try
            {
                setupOutgoingSparse( selectionIterator, traversal, node, types );
            }
            catch ( Throwable t )
            {
                traversal.close();
                throw t;
            }
            return selectionIterator;
        }
    }

    /**
     * Returns an incoming resource iterator given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param factory factory for creating instance of generic type T
     * @return An iterator that allows traversing the relationship chain.
     */
    public static <T> ResourceIterator<T> incomingIterator( CursorFactory cursors,
                                                            NodeCursor node,
                                                            int[] types,
                                                            RelationshipFactory<T> factory )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionIterator<T> selectionIterator =
                    new RelationshipDenseSelectionIterator<>( factory );
            RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
            RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor();
            try
            {
                setupIncomingDense( selectionIterator, group, traversal, node, types );
            }
            catch ( Throwable t )
            {
                IOUtils.<Cursor>closeAllUnchecked( group, traversal );
                throw t;
            }
            return selectionIterator;
        }
        else
        {
            RelationshipSparseSelectionIterator<T> selectionIterator =
                    new RelationshipSparseSelectionIterator<>( factory );
            RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor();
            try
            {
                setupIncomingSparse( selectionIterator, traversal, node, types );
            }
            catch ( Throwable t )
            {
                traversal.close();
                throw t;
            }
            return selectionIterator;
        }
    }

    /**
     * Returns a multi-directed resource iterator given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @param factory factory for creating instance of generic type T
     * @return An iterator that allows traversing the relationship chain.
     */
    public static <T> ResourceIterator<T> allIterator( CursorFactory cursors,
                                                       NodeCursor node,
                                                       int[] types,
                                                       RelationshipFactory<T> factory )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionIterator<T> selectionIterator =
                    new RelationshipDenseSelectionIterator<>( factory );
            RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
            RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor();
            try
            {
                setupAllDense( selectionIterator, group, traversal, node, types );
            }
            catch ( Throwable t )
            {
                IOUtils.<Cursor>closeAllUnchecked( group, traversal );
                throw t;
            }
            return selectionIterator;
        }
        else
        {
            RelationshipSparseSelectionIterator<T> selectionIterator =
                    new RelationshipSparseSelectionIterator<>( factory );
            RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor();
            try
            {
                setupAllSparse( selectionIterator, traversal, node, types );
            }
            catch ( Throwable t )
            {
                traversal.close();
                throw t;
            }
            return selectionIterator;
        }
    }

    private static void setupOutgoingDense( RelationshipDenseSelection denseSelection,
                                            RelationshipGroupCursor groupCursor,
                                            RelationshipTraversalCursor traversalCursor,
                                            NodeCursor node,
                                            int[] types )
    {
        node.relationships( groupCursor );
        denseSelection.outgoing( groupCursor, traversalCursor, types );
    }

    private static void setupIncomingDense( RelationshipDenseSelection denseSelection,
                                            RelationshipGroupCursor groupCursor,
                                            RelationshipTraversalCursor traversalCursor,
                                            NodeCursor node,
                                            int[] types )
    {
        node.relationships( groupCursor );
        denseSelection.incoming( groupCursor, traversalCursor, types );
    }

    private static void setupAllDense( RelationshipDenseSelection denseSelection,
                                       RelationshipGroupCursor groupCursor,
                                       RelationshipTraversalCursor traversalCursor,
                                       NodeCursor node,
                                       int[] types )
    {
        node.relationships( groupCursor );
        denseSelection.all( groupCursor, traversalCursor, types );
    }

    private static void setupOutgoingSparse( RelationshipSparseSelection sparseSelection,
                                             RelationshipTraversalCursor traversalCursor,
                                             NodeCursor node,
                                             int[] types )
    {
        node.allRelationships( traversalCursor );
        sparseSelection.outgoing( traversalCursor, types );
    }

    private static void setupIncomingSparse( RelationshipSparseSelection sparseSelection,
                                             RelationshipTraversalCursor traversalCursor,
                                             NodeCursor node,
                                             int[] types )
    {
        node.allRelationships( traversalCursor );
        sparseSelection.incoming( traversalCursor, types );
    }

    private static void setupAllSparse( RelationshipSparseSelection sparseSelection,
                                        RelationshipTraversalCursor traversalCursor,
                                        NodeCursor node,
                                        int[] types )
    {
        node.allRelationships( traversalCursor );
        sparseSelection.all( traversalCursor, types );
    }
}
