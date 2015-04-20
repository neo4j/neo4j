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

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.register.Register;

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
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException
     *          if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexLookup( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException;

    /**
     * @return an iterator over all nodes in the database.
     */
    PrimitiveLongIterator nodesGetAll();

    /**
     * @return an iterator over all relationships in the database.
     */
    PrimitiveLongIterator relationshipsGetAll();

    PrimitiveLongIterator nodeGetRelationships( long nodeId, Direction direction, int... relTypes ) throws EntityNotFoundException;

    PrimitiveLongIterator nodeGetRelationships( long nodeId, Direction direction ) throws EntityNotFoundException;

    /**
     * Returns node id of unique node found in the given unique index for value or
     * {@link StatementConstants#NO_SUCH_NODE} if the index does not contain a
     * matching node.
     *
     * If a node is found, a READ lock for the index entry will be held. If no node
     * is found (if {@link StatementConstants#NO_SUCH_NODE} was returned), a WRITE
     * lock for the index entry will be held. This is to facilitate unique creation
     * of nodes, to build get-or-create semantics on top of this method.
     *
     * @throws org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException if no such index found.
     */
    long nodeGetUniqueFromIndexLookup( IndexDescriptor index, Object value ) throws IndexNotFoundKernelException,
            IndexBrokenKernelException;

    boolean nodeExists(long nodeId);

    boolean relationshipExists( long relId );

    /**
     * Checks if a node is labeled with a certain label or not. Returns
     * {@code true} if the node is labeled with the label, otherwise {@code false.}
     */
    boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException;

    int nodeGetDegree( long nodeId, Direction direction, int relType ) throws EntityNotFoundException;

    int nodeGetDegree( long nodeId, Direction direction ) throws EntityNotFoundException;

    /**
     * Returns all labels set on node with id {@code nodeId}.
     * If the node has no labels an empty {@link Iterable} will be returned.
     */
    PrimitiveIntIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException;

    PrimitiveLongIterator nodeGetAllPropertiesKeys( long nodeId ) throws EntityNotFoundException;

    PrimitiveLongIterator relationshipGetAllPropertiesKeys( long relationshipId ) throws EntityNotFoundException;

    PrimitiveIntIterator nodeGetRelationshipTypes( long nodeId ) throws EntityNotFoundException;

    Property nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    Property relationshipGetProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException;

    Property graphGetProperty( int propertyKeyId );

    Iterator<DefinedProperty> nodeGetAllProperties( long nodeId ) throws EntityNotFoundException;

    Iterator<DefinedProperty> relationshipGetAllProperties( long relationshipId )
            throws EntityNotFoundException;

    Iterator<DefinedProperty> graphGetAllProperties();

    <EXCEPTION extends Exception> void relationshipVisit( long relId, RelationshipVisitor<EXCEPTION> visitor )
            throws EntityNotFoundException, EXCEPTION;

    /**
     * Construct a traversal cursor which will expand from one node according to its input registers,
     * putting one row in its output registers each time the {@link org.neo4j.cursor.Cursor#next()}
     * method is called.
     *
     * The traverser will use its input cursor to request more arguments. This is repeated until the input cursor is
     * exhausted or the {@link org.neo4j.cursor.Cursor#close() close} method is called on the traversal cursor.
     *
     * Output is guaranteed to be ordered by input rows - all output rows for each input will be grouped together in
     * the order the inputs arrive. Other than this, no guarantees are given.
     *
     * Calling {@link org.neo4j.cursor.Cursor#reset()} will delegate to reset the input cursor and set the
     * traversal cursor up to start over with the next item returned from the input cursor.
     *
     * Calling {@link org.neo4j.cursor.Cursor#close()} will release any associated resources and delegate
     * the close call to the input cursor.
     *
     * @param direction signals the direction that the current row relationship goes from your start node to the
     *                  neighbor node. We use this instead of just having start/end node registers, as the core use case
     *                  is returning neighbor nodes, so the signature is optimized for that.
     *
     * @param startNodeId will always be the input node id used to find the current row. This is provided as a mechanism
     *                    to help when you expand from multiple starting points and want to segregate the outputs.
     */
    Cursor expand( Cursor inputCursor,
                     /* Inputs  */ NeoRegister.Node.In nodeId, Register.Object.In<int[]> expandTypes,
                                   Register.Object.In<Direction> expandDirection,
                     /* Outputs */ NeoRegister.Relationship.Out relId, NeoRegister.RelType.Out relType,
                                   Register.Object.Out<Direction> direction,
                                   NeoRegister.Node.Out startNodeId, NeoRegister.Node.Out neighborNodeId );

    Cursor nodeGetRelationships( long nodeId, Direction direction,
                                 RelationshipVisitor<? extends RuntimeException> visitor )
            throws EntityNotFoundException;

    Cursor nodeGetRelationships( long nodeId, Direction direction, int[] types,
                                 RelationshipVisitor<? extends RuntimeException> visitor )
            throws EntityNotFoundException;
}
