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
package org.neo4j.cypher.internal.codegen;

import org.neo4j.exceptions.EntityNotFoundException;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Utilities for working with cursors from within generated code
 */
public final class CompiledCursorUtils
{
    /**
     * Do not instantiate this class
     */
    private CompiledCursorUtils()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Fetches a given property from a node
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return The value of the given property
     * @throws EntityNotFoundException If the node was deleted in transaction.
     */
    public static Value nodeGetProperty( Read read, NodeCursor nodeCursor, long node, PropertyCursor propertyCursor,
            int prop ) throws EntityNotFoundException
    {
        if ( prop == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return Values.NO_VALUE;
        }
        read.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            if ( read.nodeDeletedInTransaction( node ) )
            {
                throw new EntityNotFoundException( String.format("Node with id %d has been deleted in this transaction", node ) );
            }
            else
            {
                return Values.NO_VALUE;
            }
        }
        nodeCursor.properties( propertyCursor );
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == prop )
            {
                return propertyCursor.propertyValue();
            }
        }

        return Values.NO_VALUE;
    }

    /**
     * Checks if given node has a given label.
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param label The id of the label
     * @return {@code true} if the node has the label, otherwise {@code false}
     */
    public static boolean nodeHasLabel( Read read, NodeCursor nodeCursor, long node, int label )
    {
        read.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            return false;
        }

        return nodeCursor.hasLabel( label );
    }

    public static RelationshipSelectionCursor nodeGetRelationships( Read read, CursorFactory cursors, NodeCursor node,
            long nodeId, Direction direction, int[] types )
    {
        read.singleNode( nodeId, node );
        if ( !node.next() )
        {
            return RelationshipSelectionCursor.EMPTY;
        }
        switch ( direction )
        {
        case OUTGOING:
            return RelationshipSelections.outgoingCursor( cursors, node, types );
        case INCOMING:
            return RelationshipSelections.incomingCursor( cursors, node, types );
        case BOTH:
            return RelationshipSelections.allCursor( cursors, node, types );
        default:
            throw new IllegalStateException( "Unknown direction " + direction );
        }
    }

    /**
     * Fetches a given property from a relationship
     *
     * @param read The current Read instance
     * @param relationshipCursor The relationship cursor to use
     * @param relationship The id of the relationship
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return The value of the given property
     * @throws EntityNotFoundException If the node cannot be find.
     */
    public static Value relationshipGetProperty( Read read, RelationshipScanCursor relationshipCursor, long relationship, PropertyCursor propertyCursor,
            int prop ) throws EntityNotFoundException
    {
        if ( prop == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return Values.NO_VALUE;
        }
        read.singleRelationship( relationship, relationshipCursor );
        if ( !relationshipCursor.next() )
        {
            if ( read.relationshipDeletedInTransaction( relationship ) )
            {
                throw new EntityNotFoundException(
                        String.format( "Relationship with id %d has been deleted in this transaction", relationship ) );
            }
            else
            {
                return Values.NO_VALUE;
            }
        }
        relationshipCursor.properties( propertyCursor );
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == prop )
            {
                return propertyCursor.propertyValue();
            }
        }

        return Values.NO_VALUE;
    }

    public static RelationshipSelectionCursor nodeGetRelationships( Read read, CursorFactory cursors, NodeCursor node,
            long nodeId,
            Direction direction )
    {
        return nodeGetRelationships( read, cursors, node, nodeId, direction, null );
    }
}

