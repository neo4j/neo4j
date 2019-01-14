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
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

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
    public static RelationshipSelectionCursor outgoingCursor( CursorFactory cursors, NodeCursor node, int[] types )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionCursor selectionCursor = new RelationshipDenseSelectionCursor();
            setupOutgoingDense( selectionCursor, cursors, node, types );
            return selectionCursor;
        }
        else
        {
            RelationshipSparseSelectionCursor selectionCursor = new RelationshipSparseSelectionCursor();
            setupOutgoingSparse( selectionCursor, cursors, node, types );
            return selectionCursor;
        }
    }

    /**
     * Returns an incoming selection cursor given the provided node cursor and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor incomingCursor( CursorFactory cursors, NodeCursor node, int[] types )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionCursor selectionCursor = new RelationshipDenseSelectionCursor();
            setupIncomingDense( selectionCursor, cursors, node, types );
            return selectionCursor;
        }
        else
        {
            RelationshipSparseSelectionCursor selectionCursor = new RelationshipSparseSelectionCursor();
            setupIncomingSparse( selectionCursor, cursors, node, types );
            return selectionCursor;
        }
    }

    /**
     * Returns a multi-directed selection cursor given the provided node cursor and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor allCursor( CursorFactory cursors, NodeCursor node, int[] types )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionCursor selectionCursor = new RelationshipDenseSelectionCursor();
            setupAllDense( selectionCursor, cursors, node, types );
            return selectionCursor;
        }
        else
        {
            RelationshipSparseSelectionCursor selectionCursor = new RelationshipSparseSelectionCursor();
            setupAllSparse( selectionCursor, cursors, node, types );
            return selectionCursor;
        }
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
    public static <T> ResourceIterator<T> outgoingIterator( CursorFactory cursors, NodeCursor node, int[] types,
            RelationshipFactory<T> factory )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionIterator<T> selectionIterator =
                    new RelationshipDenseSelectionIterator<>( factory );
            setupOutgoingDense( selectionIterator, cursors, node, types );
            return selectionIterator;
        }
        else
        {
            RelationshipSparseSelectionIterator<T> selectionIterator =
                    new RelationshipSparseSelectionIterator<>( factory );
            setupOutgoingSparse( selectionIterator, cursors, node, types );
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
    public static <T> ResourceIterator<T> incomingIterator( CursorFactory cursors, NodeCursor node, int[] types,
            RelationshipFactory<T> factory )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionIterator<T> selectionIterator =
                    new RelationshipDenseSelectionIterator<>( factory );
            setupIncomingDense( selectionIterator, cursors, node, types );
            return selectionIterator;
        }
        else
        {
            RelationshipSparseSelectionIterator<T> selectionIterator =
                    new RelationshipSparseSelectionIterator<>( factory );
            setupIncomingSparse( selectionIterator, cursors, node, types );
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
    public static <T> ResourceIterator<T> allIterator( CursorFactory cursors, NodeCursor node, int[] types,
            RelationshipFactory<T> factory )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionIterator<T> selectionIterator =
                    new RelationshipDenseSelectionIterator<>( factory );
            setupAllDense( selectionIterator, cursors, node, types );
            return selectionIterator;
        }
        else
        {
            RelationshipSparseSelectionIterator<T> selectionIterator =
                    new RelationshipSparseSelectionIterator<>( factory );
            setupAllSparse( selectionIterator, cursors, node, types );
            return selectionIterator;
        }
    }

    private static void setupOutgoingDense( RelationshipDenseSelection denseSelection, CursorFactory cursors,
            NodeCursor node, int[] types )
    {

        RelationshipGroupCursor groupCursor = cursors.allocateRelationshipGroupCursor();
        RelationshipTraversalCursor traversalCursor = cursors.allocateRelationshipTraversalCursor();
        try
        {
            node.relationships( groupCursor );
            denseSelection.outgoing( groupCursor, traversalCursor, types );
        }
        catch ( Throwable t )
        {
            groupCursor.close();
            traversalCursor.close();
            throw t;
        }
    }

    private static void setupIncomingDense( RelationshipDenseSelection denseSelection, CursorFactory cursors,
            NodeCursor node, int[] types )
    {

        RelationshipGroupCursor groupCursor = cursors.allocateRelationshipGroupCursor();
        RelationshipTraversalCursor traversalCursor = cursors.allocateRelationshipTraversalCursor();
        try
        {
            node.relationships( groupCursor );
            denseSelection.incoming( groupCursor, traversalCursor, types );
        }
        catch ( Throwable t )
        {
            groupCursor.close();
            traversalCursor.close();
            throw t;
        }
    }

    private static void setupAllDense( RelationshipDenseSelection denseSelection, CursorFactory cursors,
            NodeCursor node, int[] types )
    {

        RelationshipGroupCursor groupCursor = cursors.allocateRelationshipGroupCursor();
        RelationshipTraversalCursor traversalCursor = cursors.allocateRelationshipTraversalCursor();
        try
        {
            node.relationships( groupCursor );
            denseSelection.all( groupCursor, traversalCursor, types );
        }
        catch ( Throwable t )
        {
            groupCursor.close();
            traversalCursor.close();
            throw t;
        }
    }

    private static void setupOutgoingSparse( RelationshipSparseSelection sparseSelection,
            CursorFactory cursors, NodeCursor node, int[] types )
    {
        RelationshipTraversalCursor traversalCursor = cursors.allocateRelationshipTraversalCursor();
        try
        {
            node.allRelationships( traversalCursor );
            sparseSelection.outgoing( traversalCursor, types );
        }
        catch ( Throwable t )
        {
            traversalCursor.close();
            throw t;
        }
    }

    private static void setupIncomingSparse( RelationshipSparseSelection sparseSelection,
            CursorFactory cursors, NodeCursor node, int[] types )
    {
        RelationshipTraversalCursor traversalCursor = cursors.allocateRelationshipTraversalCursor();
        try
        {
            node.allRelationships( traversalCursor );
            sparseSelection.incoming( traversalCursor, types );
        }
        catch ( Throwable t )
        {
            traversalCursor.close();
            throw t;
        }
    }

    private static void setupAllSparse( RelationshipSparseSelection sparseSelection,
            CursorFactory cursors, NodeCursor node, int[] types )
    {
        RelationshipTraversalCursor traversalCursor = cursors.allocateRelationshipTraversalCursor();
        try
        {
            node.allRelationships( traversalCursor );
            sparseSelection.all( traversalCursor, types );
        }
        catch ( Throwable t )
        {
            traversalCursor.close();
            throw t;
        }
    }
}
