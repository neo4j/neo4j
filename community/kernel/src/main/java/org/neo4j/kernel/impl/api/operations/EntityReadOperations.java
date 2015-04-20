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
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.register.Register;

public interface EntityReadOperations
{
    // Currently, of course, most relevant operations here are still in the old core API implementation.
    boolean nodeExists(KernelStatement state, long nodeId);

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
    PrimitiveLongIterator nodesGetFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException;

    /**
     * Returns an iterable with the matched node.
     *
     * @throws IndexNotFoundKernelException if no such index found.
     * @throws IndexBrokenKernelException   if we found an index that was corrupt or otherwise in a failed state.
     */
    long nodeGetUniqueFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value )
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

    Property nodeGetProperty( KernelStatement state, long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    Property relationshipGetProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException;

    Property graphGetProperty( KernelStatement state, int propertyKeyId );

    // TODO: decide if this should be replaced by nodeGetAllProperties()

    /**
     * Return all property keys associated with a node.
     */
    PrimitiveLongIterator nodeGetPropertyKeys( KernelStatement state, long nodeId ) throws EntityNotFoundException;

    Iterator<DefinedProperty> nodeGetAllProperties( KernelStatement state, long nodeId ) throws EntityNotFoundException;

    // TODO: decide if this should be replaced by relationshipGetAllProperties()

    /**
     * Return all property keys associated with a relationship.
     */
    PrimitiveLongIterator relationshipGetPropertyKeys( KernelStatement state, long relationshipId ) throws
            EntityNotFoundException;

    Iterator<DefinedProperty> relationshipGetAllProperties( KernelStatement state,
                                                            long relationshipId ) throws EntityNotFoundException;

    // TODO: decide if this should be replaced by relationshipGetAllProperties()

    /**
     * Return all property keys associated with a relationship.
     */
    PrimitiveLongIterator graphGetPropertyKeys( KernelStatement state );

    Iterator<DefinedProperty> graphGetAllProperties( KernelStatement state );

    PrimitiveLongIterator nodeGetRelationships( KernelStatement statement, long nodeId, Direction direction,
                                                int[] relTypes ) throws EntityNotFoundException;

    PrimitiveLongIterator nodeGetRelationships( KernelStatement statement, long nodeId, Direction direction ) throws EntityNotFoundException;

    int nodeGetDegree( KernelStatement statement, long nodeId, Direction direction, int relType ) throws EntityNotFoundException;

    int nodeGetDegree( KernelStatement statement, long nodeId, Direction direction ) throws EntityNotFoundException;

    PrimitiveIntIterator nodeGetRelationshipTypes( KernelStatement statement, long nodeId ) throws EntityNotFoundException;

    PrimitiveLongIterator nodesGetAll( KernelStatement state );

    PrimitiveLongIterator relationshipsGetAll( KernelStatement state );

    <EXCEPTION extends Exception> void relationshipVisit( KernelStatement statement, long relId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EntityNotFoundException, EXCEPTION;

    Cursor expand( KernelStatement statement, Cursor inputCursor,
                     /* Inputs  */ NeoRegister.Node.In nodeId, Register.Object.In<int[]> types,
                     Register.Object.In<Direction> expandDirection,
                     /* Outputs */ NeoRegister.Relationship.Out relId, NeoRegister.RelType.Out relType,
                     Register.Object.Out<Direction> direction,
                     NeoRegister.Node.Out startNodeId, NeoRegister.Node.Out neighborNodeId );


    Cursor nodeGetRelationships( KernelStatement statement, long nodeId, Direction direction,
                                 RelationshipVisitor<? extends RuntimeException> visitor )
            throws EntityNotFoundException;

    Cursor nodeGetRelationships( KernelStatement statement, long nodeId, Direction direction, int[] types,
                                 RelationshipVisitor<? extends RuntimeException> visitor )
            throws EntityNotFoundException;
}
