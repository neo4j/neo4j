/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

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
     * Returns a selection cursor given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param direction The direction of the the relationship.
     * @param types The types of the relationship
     * @return A cursor that allows traversing the relationship chain.
     */
    public static RelationshipSelectionCursor selectionCursor( CursorFactory cursors, NodeCursor node,
            Direction direction, int[] types )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionCursor selectionCursor = new RelationshipDenseSelectionCursor();
            setupDense( selectionCursor, cursors, node, direction, types );
            return selectionCursor;
        }
        else
        {
            RelationshipSparseSelectionCursor selectionCursor = new RelationshipSparseSelectionCursor();

            setupSparse( selectionCursor, cursors, node, direction, types );
            return selectionCursor;
        }
    }

    /**
     * Returns a resource iterator given the provided node cursor, direction and relationship types.
     *
     * @param cursors A cursor factor used for allocating the needed cursors
     * @param node A node cursor positioned at the current node.
     * @param direction The direction of the the relationship.
     * @param types The types of the relationship
     * @param factory factory for creating instance of generic type T
     * @return An iterator that allows traversing the relationship chain.
     */
    public static <T> ResourceIterator<T> selectionIterator( CursorFactory cursors, NodeCursor node,
            Direction direction, int[] types, RelationshipFactory<T> factory )
    {
        if ( node.isDense() )
        {
            RelationshipDenseSelectionIterator<T> selectionIterator =
                    new RelationshipDenseSelectionIterator<>( factory );
            setupDense( selectionIterator, cursors, node, direction, types );
            return selectionIterator;
        }
        else
        {
            RelationshipSparseSelectionIterator<T> selectionIterator =
                    new RelationshipSparseSelectionIterator<>( factory );

            setupSparse( selectionIterator, cursors, node, direction, types );
            return selectionIterator;
        }
    }

    private static void setupDense( RelationshipDenseSelection denseSelection, CursorFactory cursors, NodeCursor node,
            Direction direction, int[] types )
    {

        RelationshipGroupCursor groupCursor = cursors.allocateRelationshipGroupCursor();
        RelationshipTraversalCursor traversalCursor = cursors.allocateRelationshipTraversalCursor();
        try
        {
            node.relationships( groupCursor );
            switch ( direction )
            {
            case OUTGOING:
                denseSelection.outgoing( groupCursor, traversalCursor, types );
                break;
            case INCOMING:
                denseSelection.incoming( groupCursor, traversalCursor, types );
                break;
            case BOTH:
                denseSelection.all( groupCursor, traversalCursor, types );
                break;
            default:
                throw new IllegalStateException( "Unknown direction: " + direction );
            }
        }
        catch ( Throwable t )
        {
            groupCursor.close();
            traversalCursor.close();
            throw t;
        }
    }

    private static void setupSparse( RelationshipSparseSelection sparseSelection,
            CursorFactory cursors, NodeCursor node, Direction direction, int[] types )
    {
        RelationshipTraversalCursor traversalCursor = cursors.allocateRelationshipTraversalCursor();
        try
        {
            node.allRelationships( traversalCursor );
            switch ( direction )
            {
            case OUTGOING:
                sparseSelection.outgoing( traversalCursor, types );
                break;
            case INCOMING:
                sparseSelection.incoming( traversalCursor, types );
                break;
            case BOTH:
                sparseSelection.all( traversalCursor, types );
                break;
            default:
                throw new IllegalStateException( "Unknown direction: " + direction );
            }
        }
        catch ( Throwable t )
        {
            traversalCursor.close();
            throw t;
        }
    }
}
