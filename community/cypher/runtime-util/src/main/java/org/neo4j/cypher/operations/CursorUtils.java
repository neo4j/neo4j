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
package org.neo4j.cypher.operations;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.EntityNotFoundException;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.newapi.Cursors;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Utilities for working with cursors from within generated code
 */
@SuppressWarnings( {"Duplicates"} )
public final class CursorUtils
{
    /**
     * Do not instantiate this class
     */
    private CursorUtils()
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
    public static Value nodeGetProperty(
            Read read,
            NodeCursor nodeCursor,
            long node,
            PropertyCursor propertyCursor,
            int prop
    ) throws EntityNotFoundException
    {
        return nodeGetProperty( read, nodeCursor, node, propertyCursor, prop, true );
    }

    /**
     * Fetches a given property from a node
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @param throwOnDeleted if <code>true</code> and exception will be thrown if node has been deleted
     * @return The value of the given property
     * @throws EntityNotFoundException If the node was deleted in transaction.
     */
    public static Value nodeGetProperty(
            Read read,
            NodeCursor nodeCursor,
            long node,
            PropertyCursor propertyCursor,
            int prop,
            boolean throwOnDeleted
    ) throws EntityNotFoundException
    {
        if ( prop == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return NO_VALUE;
        }
        read.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            if ( throwOnDeleted && read.nodeDeletedInTransaction( node ) )
            {
                throw new EntityNotFoundException( String.format("Node with id %d has been deleted in this transaction", node ) );
            }
            else
            {
                return NO_VALUE;
            }
        }
        return nodeGetProperty( nodeCursor, propertyCursor, prop );
    }

    /**
     * Fetches a given property from a node, where the node has already been loaded.
     *
     * @param nodeCursor The node cursor which currently points to the node to get the property from.
     * @param propertyCursor The property cursor to use to read the property.
     * @param prop The property key id
     * @return The value of the property, otherwise {@link Values#NO_VALUE} if not found.
     */
    public static Value nodeGetProperty(
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            int prop )
    {
        nodeCursor.properties( propertyCursor, PropertySelection.selection( prop ) );
        return propertyCursor.next() ? propertyCursor.propertyValue() : NO_VALUE;
    }

    /**
     * Checks if a given node has the given property
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return <code>true</code> if node has property otherwise <code>false</code>
     */
    public static boolean nodeHasProperty(
            Read read,
            NodeCursor nodeCursor,
            long node,
            PropertyCursor propertyCursor,
            int prop
    ) throws EntityNotFoundException
    {
        if ( prop == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return false;
        }
        read.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
           return false;
        }
        return nodeHasProperty( nodeCursor, propertyCursor, prop );
    }

    /**
     * Checks if a given node has the given property, where the node has already been loaded.
     *
     * @param nodeCursor The node cursor which currently points to the node to check property existence for.
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return {@code true} if node has property otherwise {@code false}.
     */
    public static boolean nodeHasProperty(
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor,
            int prop
    )
    {
        nodeCursor.properties( propertyCursor, PropertySelection.onlyKeysSelection( prop ) );
        return propertyCursor.next();
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

    /**
     * Returns true if any of the specified labels are set on the node with id `node`.
     */
    public static boolean nodeHasAnyLabel( Read read, NodeCursor nodeCursor, long node, int[] labels )
    {
        read.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            return false;
        }

        return nodeHasAnyLabel( nodeCursor, labels );
    }

    /**
     * Returns true if any of the specified labels are set on the node that `cursor` is pointing at.
     */
    public static boolean nodeHasAnyLabel( NodeCursor cursor, int[] labels )
    {
        var nodeLabels = cursor.labels();

        for ( int label : labels )
        {
            if ( nodeLabels.contains( label ) )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if given relationship has a given type.
     *
     * @param read The current Read instance
     * @param relationshipCursor The relationship cursor to use
     * @param relationship The id of the relationship
     * @param type The id of the type
     * @return {@code true} if the relationship has the type, otherwise {@code false}
     */
    public static boolean relationshipHasType( Read read, RelationshipScanCursor relationshipCursor, long relationship, int type )
    {
        read.singleRelationship( relationship, relationshipCursor );
        if ( !relationshipCursor.next() )
        {
            return false;
        }

        return relationshipCursor.type() == type;
    }

    public static RelationshipTraversalCursor nodeGetRelationships( Read read, CursorFactory cursors, NodeCursor node,
            long nodeId, Direction direction, int[] types, CursorContext cursorContext )
    {
        read.singleNode( nodeId, node );
        if ( !node.next() )
        {
            return Cursors.emptyTraversalCursor( read );
        }
        switch ( direction )
        {
        case OUTGOING:
            return RelationshipSelections.outgoingCursor( cursors, node, types, cursorContext );
        case INCOMING:
            return RelationshipSelections.incomingCursor( cursors, node, types, cursorContext );
        case BOTH:
            return RelationshipSelections.allCursor( cursors, node, types, cursorContext );
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
    public static Value relationshipGetProperty(
            Read read,
            RelationshipScanCursor relationshipCursor,
            long relationship,
            PropertyCursor propertyCursor,
            int prop
    ) throws EntityNotFoundException
    {
        return relationshipGetProperty( read, relationshipCursor, relationship, propertyCursor, prop, true );
    }

    /**
     * Fetches a given property from a relationship
     *
     * @param read The current Read instance
     * @param relationshipCursor The relationship cursor to use
     * @param relationship The id of the relationship
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @param throwOnDeleted if <code>true</code> and exception will be thrown if node has been deleted
     * @return The value of the given property
     * @throws EntityNotFoundException If the node cannot be find.
     */
    public static Value relationshipGetProperty(
            Read read,
            RelationshipScanCursor relationshipCursor,
            long relationship,
            PropertyCursor propertyCursor,
            int prop,
            boolean throwOnDeleted
    ) throws EntityNotFoundException
    {
        if ( prop == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return NO_VALUE;
        }
        read.singleRelationship( relationship, relationshipCursor );
        if ( !relationshipCursor.next() )
        {
            if ( throwOnDeleted && read.relationshipDeletedInTransaction( relationship ) )
            {
                throw new EntityNotFoundException(
                        String.format( "Relationship with id %d has been deleted in this transaction", relationship ) );
            }
            else
            {
                return NO_VALUE;
            }
        }
        return relationshipGetProperty( relationshipCursor, propertyCursor, prop );
    }

    /**
     * Fetches a given property from a relationship, where the relationship has already been loaded.
     *
     * @param relationshipCursor relationship cursor which currently points to the relationship to get the property from.
     * @param propertyCursor the property cursor to use to read the property.
     * @param prop property key id
     * @return the value of the property, otherwise {@link Values#NO_VALUE} if not found.
     */
    public static Value relationshipGetProperty( RelationshipDataAccessor relationshipCursor, PropertyCursor propertyCursor, int prop )
    {
        relationshipCursor.properties( propertyCursor, PropertySelection.selection( prop ) );
        return propertyCursor.next() ? propertyCursor.propertyValue() : NO_VALUE;
    }

    /**
     * Checks if a given relationship has the given property
     *
     * @param read The current Read instance
     * @param relationshipCursor The relationship cursor to use
     * @param relationship The id of the relationship
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return <code>true</code> if relationship has property otherwise <code>false</code>
     */
    public static boolean relationshipHasProperty(
            Read read,
            RelationshipScanCursor relationshipCursor,
            long relationship,
            PropertyCursor propertyCursor,
            int prop
    ) throws EntityNotFoundException
    {
        if ( prop == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return false;
        }
        read.singleRelationship( relationship, relationshipCursor );
        if ( !relationshipCursor.next() )
        {
            return false;
        }
        return relationshipHasProperty( relationshipCursor, propertyCursor, prop );
    }

    /**
     * Checks if a given relationship has the given property, where the relationship has already been loaded.
     *
     * @param relationshipCursor The relationship cursor which currently points to the relationship to check property existence for.
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return {@code true} if relationship has property otherwise {@code false}.
     */
    public static boolean relationshipHasProperty(
            RelationshipDataAccessor relationshipCursor,
            PropertyCursor propertyCursor,
            int prop
    )
    {
        relationshipCursor.properties( propertyCursor, PropertySelection.onlyKeysSelection( prop ) );
        return propertyCursor.next();
    }

    public static RelationshipTraversalCursor nodeGetRelationships( Read read, CursorFactory cursors, NodeCursor node,
            long nodeId, Direction direction, CursorContext cursorContext )
    {
        return nodeGetRelationships( read, cursors, node, nodeId, direction, null, cursorContext );
    }

    public static AnyValue propertyGet( String key,
            AnyValue container,
            Read read,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor )
    {
        assert container != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( container instanceof VirtualNodeValue )
        {
            return nodeGetProperty(
                    read,
                    nodeCursor,
                    ((VirtualNodeValue) container).id(),
                    propertyCursor,
                    dbAccess.propertyKey( key ) );
        }
        else if ( container instanceof VirtualRelationshipValue )
        {
            return relationshipGetProperty(
                    read,
                    relationshipScanCursor,
                    ((VirtualRelationshipValue) container).id(),
                    propertyCursor,
                    dbAccess.propertyKey( key ) );
        }
        else if ( container instanceof MapValue )
        {
            return ((MapValue) container).get( key );
        }
        else if ( container instanceof TemporalValue<?,?> )
        {
            return ((TemporalValue) container).get( key );
        }
        else if ( container instanceof DurationValue )
        {
            return ((DurationValue) container).get( key );
        }
        else if ( container instanceof PointValue )
        {
            return ((PointValue) container).get( key );
        }
        else
        {
            throw new CypherTypeException( format( "Type mismatch: expected a map but was %s", container ),
                    null );
        }
    }
}
