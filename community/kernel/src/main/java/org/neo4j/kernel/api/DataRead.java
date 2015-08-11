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
package org.neo4j.kernel.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

interface DataRead
{
    /**
     * @param labelId the label id of the label that returned nodes are guaranteed to have
     * @return ids of all nodes that have the given label
     */
    PrimitiveLongIterator nodesGetForLabel( int labelId );

    /**
     * Returns an iterator with the matched nodes.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexSeek( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterator with the matched nodes.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterator with the matched nodes.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( IndexDescriptor index, Number lower, boolean includeLower, Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterator with the matched nodes.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( IndexDescriptor index, String lower, boolean includeLower, String upper, boolean includeUpper )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterator with the matched nodes.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexScan( IndexDescriptor index )
            throws IndexNotFoundKernelException;

    /**
     * @return an iterator over all nodes in the database.
     */
    PrimitiveLongIterator nodesGetAll();

    /**
     * @return an iterator over all relationships in the database.
     */
    PrimitiveLongIterator relationshipsGetAll();

    RelationshipIterator nodeGetRelationships( long nodeId,
            Direction direction,
            int... relTypes ) throws EntityNotFoundException;

    RelationshipIterator nodeGetRelationships( long nodeId, Direction direction ) throws EntityNotFoundException;

    /**
     * Returns node id of unique node found in the given unique index for value or
     * {@link StatementConstants#NO_SUCH_NODE} if the index does not contain a
     * matching node.
     * <p/>
     * If a node is found, a READ lock for the index entry will be held. If no node
     * is found (if {@link StatementConstants#NO_SUCH_NODE} was returned), a WRITE
     * lock for the index entry will be held. This is to facilitate unique creation
     * of nodes, to build get-or-create semantics on top of this method.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    long nodeGetFromUniqueIndexSeek( IndexDescriptor index, Object value ) throws IndexNotFoundKernelException,
            IndexBrokenKernelException;

    boolean nodeExists( long nodeId );

    boolean relationshipExists( long relId );

    /**
     * Checks if a node is labeled with a certain label or not. Returns
     * {@code true} if the node is labeled with the label, otherwise {@code false.}
     */
    boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException;

    int nodeGetDegree( long nodeId, Direction direction, int relType ) throws EntityNotFoundException;

    int nodeGetDegree( long nodeId, Direction direction ) throws EntityNotFoundException;

    boolean nodeIsDense(long nodeId) throws EntityNotFoundException;

    /**
     * Returns all labels set on node with id {@code nodeId}.
     * If the node has no labels an empty {@link Iterable} will be returned.
     */
    PrimitiveIntIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException;

    PrimitiveIntIterator nodeGetPropertyKeys( long nodeId ) throws EntityNotFoundException;

    PrimitiveIntIterator relationshipGetPropertyKeys( long relationshipId ) throws EntityNotFoundException;

    PrimitiveIntIterator graphGetPropertyKeys();

    PrimitiveIntIterator nodeGetRelationshipTypes( long nodeId ) throws EntityNotFoundException;

    boolean nodeHasProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    Object nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    boolean relationshipHasProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException;

    Object relationshipGetProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException;

    boolean graphHasProperty( int propertyKeyId );

    Object graphGetProperty( int propertyKeyId );

    <EXCEPTION extends Exception> void relationshipVisit( long relId, RelationshipVisitor<EXCEPTION> visitor )
            throws EntityNotFoundException, EXCEPTION;
}
