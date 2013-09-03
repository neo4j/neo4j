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
package org.neo4j.kernel.api;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.SafeProperty;
import org.neo4j.kernel.impl.api.constraints.ConstraintValidationKernelException;

interface DataWrite
{
    long nodeCreate();

    void nodeDelete( long nodeId );

    long relationshipCreate( long relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException;

    void relationshipDelete( long relationshipId );

    /**
     * Labels a node with the label corresponding to the given label id.
     * If the node already had that label nothing will happen. Label ids
     * are retrieved from {@link org.neo4j.kernel.api.operations.KeyWriteOperations#labelGetOrCreateForName(StatementState,
     * String)} or {@link
     * org.neo4j.kernel.api.operations.KeyReadOperations#labelGetForName(StatementState, String)}.
     */
    boolean nodeAddLabel( long nodeId, long labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException;

    /**
     * Removes a label with the corresponding id from a node.
     * If the node doesn't have that label nothing will happen. Label ids
     * are retrieved from {@link org.neo4j.kernel.api.operations.KeyWriteOperations#labelGetOrCreateForName(StatementState,
     * String)} or {@link
     * org.neo4j.kernel.api.operations.KeyReadOperations#labelGetForName(StatementState, String)}.
     */
    boolean nodeRemoveLabel( long nodeId, long labelId ) throws EntityNotFoundException;

    Property nodeSetProperty( long nodeId, SafeProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException;

    Property relationshipSetProperty( long relationshipId, SafeProperty property ) throws EntityNotFoundException;

    Property graphSetProperty( SafeProperty property );

    /**
     * Remove a node's property given the node's id and the property key id and return the value to which
     * it was set or null if it was not set on the node
     */
    Property nodeRemoveProperty( long nodeId, long propertyKeyId ) throws EntityNotFoundException;

    Property relationshipRemoveProperty( long relationshipId, long propertyKeyId ) throws EntityNotFoundException;

    Property graphRemoveProperty( long propertyKeyId );
}
