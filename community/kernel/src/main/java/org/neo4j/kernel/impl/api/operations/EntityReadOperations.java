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
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.cursor.NodeCursor;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
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
    PrimitiveLongIterator nodesGetFromIndexSeekByPrefix( KernelStatement state, IndexDescriptor index, String prefix )
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
    boolean nodeHasLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException;

    /**
     * Returns all labels set on node with id {@code nodeId}.
     * If the node has no labels an empty iterator will be returned.
     */
    PrimitiveIntIterator nodeGetLabels( KernelStatement state, long nodeId ) throws EntityNotFoundException;

    public PrimitiveIntIterator nodeGetLabels( TxStateHolder txStateHolder, StoreStatement storeStatement, long nodeId )
            throws EntityNotFoundException;

    boolean nodeHasProperty( KernelStatement statement, long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    public boolean nodeHasProperty( TxStateHolder txStateHolder, StoreStatement storeStatement,
            long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    Object nodeGetProperty( KernelStatement state, long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    boolean relationshipHasProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException;

    Object relationshipGetProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException;

    boolean graphHasProperty( KernelStatement state, int propertyKeyId );

    Object graphGetProperty( KernelStatement state, int propertyKeyId );

    /**
     * Return all property keys associated with a node.
     */
    PrimitiveIntIterator nodeGetPropertyKeys( KernelStatement state, long nodeId ) throws EntityNotFoundException;

    /**
     * Return all property keys associated with a relationship.
     */
    PrimitiveIntIterator relationshipGetPropertyKeys( KernelStatement state, long relationshipId ) throws
            EntityNotFoundException;

    /**
     * Return all property keys associated with a relationship.
     */
    PrimitiveIntIterator graphGetPropertyKeys( KernelStatement state );

    RelationshipIterator nodeGetRelationships( KernelStatement statement, long nodeId, Direction direction,
            int[] relTypes ) throws EntityNotFoundException;

    RelationshipIterator nodeGetRelationships( KernelStatement statement,
            long nodeId,
            Direction direction ) throws EntityNotFoundException;

    int nodeGetDegree( KernelStatement statement,
            long nodeId,
            Direction direction,
            int relType ) throws EntityNotFoundException;

    int nodeGetDegree( KernelStatement statement, long nodeId, Direction direction ) throws EntityNotFoundException;

    PrimitiveIntIterator nodeGetRelationshipTypes( KernelStatement statement,
            long nodeId ) throws EntityNotFoundException;

    PrimitiveLongIterator nodesGetAll( KernelStatement state );

    PrimitiveLongIterator relationshipsGetAll( KernelStatement state );

    <EXCEPTION extends Exception> void relationshipVisit( KernelStatement statement, long relId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EntityNotFoundException, EXCEPTION;

    NodeCursor nodeCursor( KernelStatement statement, long nodeId );

    RelationshipCursor relationshipCursor( KernelStatement statement, long relId );

    NodeCursor nodeCursorGetAll( KernelStatement statement );

    RelationshipCursor relationshipCursorGetAll( KernelStatement statement );

    NodeCursor nodeCursorGetForLabel( KernelStatement statement, int labelId );

    NodeCursor nodeCursorGetFromIndexSeek( KernelStatement statement, IndexDescriptor index, Object value ) throws IndexNotFoundKernelException;

    NodeCursor nodeCursorGetFromIndexScan( KernelStatement statement, IndexDescriptor index ) throws IndexNotFoundKernelException;

    NodeCursor nodeCursorGetFromIndexSeekByPrefix( KernelStatement statement, IndexDescriptor index, String prefix ) throws IndexNotFoundKernelException;

    NodeCursor nodeCursorGetFromUniqueIndexSeek( KernelStatement statement, IndexDescriptor index, Object value ) throws IndexBrokenKernelException, IndexNotFoundKernelException;

}
