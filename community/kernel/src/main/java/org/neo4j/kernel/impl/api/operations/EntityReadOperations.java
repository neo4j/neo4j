/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.operations;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.api.store.StoreStatement;

public interface EntityReadOperations
{
    // Currently, of course, most relevant operations here are still in the old core API implementation.
    boolean nodeExists( KernelStatement state, long nodeId );

    boolean relationshipExists( KernelStatement statement, long relId );

    /**
     * @param labelId the label id of the label that returned nodes are guaranteed to have
     * @return ids of all nodes that have the given label
     */
    PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId );

    /**
     * Returns an iterable with the matched nodes.
     *
     * @throws IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexSeek( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterable with the matched nodes.
     *
     * @throws IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( KernelStatement state, IndexDescriptor index, Number lower, boolean includeLower, Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterable with the matched nodes.
     *
     * @throws IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( KernelStatement state, IndexDescriptor index, String lower, boolean includeLower, String upper, boolean includeUpper )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterable with the matched nodes.
     *
     * @throws IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( KernelStatement state, IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterable with the matched nodes.
     *
     * @throws IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexScan( KernelStatement state, IndexDescriptor index )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterable with the matched node.
     *
     * @throws IndexNotFoundKernelException if no such index found.
     * @throws IndexBrokenKernelException   if we found an index that was corrupt or otherwise in a failed state.
     */
    long nodeGetFromUniqueIndexSeek( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException;

    /**
     * Checks if a node is labeled with a certain label or not. Returns
     * {@code true} if the node is labeled with the label, otherwise {@code false.}
     * Label ids are retrieved from {@link KeyWriteOperations#labelGetOrCreateForName(org.neo4j.kernel.api.Statement,
     * String)} or
     * {@link KeyReadOperations#labelGetForName(org.neo4j.kernel.api.Statement, String)}.
     */
    boolean nodeHasLabel( KernelStatement state, NodeItem node, int labelId );

    /**
     * Returns all labels set on node with id {@code nodeId}.
     * If the node has no labels an empty iterator will be returned.
     */
    PrimitiveIntIterator nodeGetLabels( KernelStatement state, NodeItem node );

    public PrimitiveIntIterator nodeGetLabels( TxStateHolder txStateHolder,
            StoreStatement storeStatement,
            NodeItem node );

    boolean nodeHasProperty( KernelStatement statement, NodeItem node, int propertyKeyId );

    public boolean nodeHasProperty( TxStateHolder txStateHolder, StoreStatement storeStatement,
            NodeItem node, int propertyKeyId );

    Object nodeGetProperty( KernelStatement state, NodeItem node, int propertyKeyId );

    boolean relationshipHasProperty( KernelStatement state, RelationshipItem relationship, int propertyKeyId );

    boolean relationshipHasProperty( TxStateHolder txStateHolder, StoreStatement storeStatement, RelationshipItem relationship, int propertyKeyId );

    Object relationshipGetProperty( KernelStatement state, RelationshipItem relationship, int propertyKeyId );

    boolean graphHasProperty( KernelStatement state, int propertyKeyId );

    Object graphGetProperty( KernelStatement state, int propertyKeyId );

    /**
     * Return all property keys associated with a node.
     */
    PrimitiveIntIterator nodeGetPropertyKeys( KernelStatement state, NodeItem node );

    /**
     * Return all property keys associated with a relationship.
     */
    PrimitiveIntIterator relationshipGetPropertyKeys( KernelStatement state, RelationshipItem relationship );

    /**
     * Return all property keys associated with a relationship.
     */
    PrimitiveIntIterator graphGetPropertyKeys( KernelStatement state );

    RelationshipIterator nodeGetRelationships( KernelStatement statement, NodeItem node, Direction direction,
            int[] relTypes );

    RelationshipIterator nodeGetRelationships( KernelStatement statement, NodeItem node, Direction direction );

    int nodeGetDegree( KernelStatement statement, NodeItem node, Direction direction, int relType ) throws
            EntityNotFoundException;

    int nodeGetDegree( KernelStatement statement, NodeItem node, Direction direction ) throws EntityNotFoundException;

    PrimitiveIntIterator nodeGetRelationshipTypes( KernelStatement statement, NodeItem node ) throws
            EntityNotFoundException;

    PrimitiveLongIterator nodesGetAll( KernelStatement state );

    PrimitiveLongIterator relationshipsGetAll( KernelStatement state );

    <EXCEPTION extends Exception> void relationshipVisit( KernelStatement statement, long relId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EntityNotFoundException, EXCEPTION;

    Cursor<NodeItem> nodeCursor( KernelStatement statement, long nodeId );

    Cursor<NodeItem> nodeCursor( TxStateHolder txStateHolder, StoreStatement statement, long nodeId );

    Cursor<RelationshipItem> relationshipCursor( KernelStatement statement, long relId );

    Cursor<RelationshipItem> relationshipCursor( TxStateHolder txStateHolder, StoreStatement statement, long relId );

    Cursor<NodeItem> nodeCursorGetAll( KernelStatement statement );

    Cursor<RelationshipItem> relationshipCursorGetAll( KernelStatement statement );

    Cursor<NodeItem> nodeCursorGetForLabel( KernelStatement statement, int labelId );

    Cursor<NodeItem> nodeCursorGetFromIndexSeek( KernelStatement statement,
            IndexDescriptor index,
            Object value ) throws IndexNotFoundKernelException;
    
    Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByNumber( KernelStatement statement, IndexDescriptor index, Number lower, boolean includeLower, Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException;

    Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByString( KernelStatement statement, IndexDescriptor index, String lower, boolean includeLower, String upper, boolean includeUpper )
            throws IndexNotFoundKernelException;

    Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByPrefix( KernelStatement statement, IndexDescriptor index, String prefix ) throws IndexNotFoundKernelException;

    Cursor<NodeItem> nodeCursorGetFromIndexScan( KernelStatement statement,
            IndexDescriptor index ) throws IndexNotFoundKernelException;

    Cursor<NodeItem> nodeCursorGetFromIndexSeekByPrefix( KernelStatement statement,
            IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException;

    Cursor<NodeItem> nodeCursorGetFromUniqueIndexSeek( KernelStatement statement,
            IndexDescriptor index,
            Object value ) throws IndexBrokenKernelException, IndexNotFoundKernelException;

}
