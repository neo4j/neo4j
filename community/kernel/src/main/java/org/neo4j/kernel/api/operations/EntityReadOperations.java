/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.operations;

import java.util.Iterator;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public interface EntityReadOperations
{
    // Currently, of course, most relevant operations here are still in the old core API implementation.

    /**
     *
     * @param labelId the label id of the label that returned nodes are guaranteed to have
     * @return ids of all nodes that have the given label
     */
    PrimitiveLongIterator nodesGetForLabel( StatementState state, long labelId );

    /**
     * Returns an iterable with the matched nodes.
     *
     * @throws IndexNotFoundKernelException if no such index found.
     */
    PrimitiveLongIterator nodesGetFromIndexLookup( StatementState state, IndexDescriptor index, Object value ) throws IndexNotFoundKernelException;

    /**
     * Checks if a node is labeled with a certain label or not. Returns
     * {@code true} if the node is labeled with the label, otherwise {@code false.}
     * Label ids are retrieved from {@link KeyWriteOperations#labelGetOrCreateForName(StatementState, String)} or
     * {@link KeyReadOperations#labelGetForName(StatementState, String)}.
     */
    boolean nodeHasLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException;

    /**
     * Returns all labels set on node with id {@code nodeId}.
     * If the node has no labels an empty {@link Iterable} will be returned.
     */
    PrimitiveLongIterator nodeGetLabels( StatementState state, long nodeId ) throws EntityNotFoundException;

    Property nodeGetProperty( StatementState state, long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;

    Property relationshipGetProperty( StatementState state, long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;
    
    Property graphGetProperty( StatementState state, long propertyKeyId )
            throws PropertyKeyIdNotFoundException;

    /** Returns true if node has the property given it's property key id for the node with the given node id */
    boolean nodeHasProperty( StatementState state, long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;

    boolean relationshipHasProperty( StatementState state, long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;

    boolean graphHasProperty( StatementState state, long propertyKeyId )
            throws PropertyKeyIdNotFoundException;
    
    // TODO: decide if this should be replaced by nodeGetAllProperties()
    /** Return all property keys associated with a node. */
    PrimitiveLongIterator nodeGetPropertyKeys( StatementState state, long nodeId ) throws EntityNotFoundException;

    Iterator<Property> nodeGetAllProperties( StatementState state, long nodeId ) throws EntityNotFoundException;

    // TODO: decide if this should be replaced by relationshipGetAllProperties()
    /** Return all property keys associated with a relationship. */
    PrimitiveLongIterator relationshipGetPropertyKeys( StatementState state, long relationshipId ) throws EntityNotFoundException;

    Iterator<Property> relationshipGetAllProperties( StatementState state, long relationshipId ) throws EntityNotFoundException;

    // TODO: decide if this should be replaced by relationshipGetAllProperties()
    /** Return all property keys associated with a relationship. */
    PrimitiveLongIterator graphGetPropertyKeys( StatementState state );

    Iterator<Property> graphGetAllProperties( StatementState state );
}
