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

import java.util.List;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.procedures.ProcedureSignature.ProcedureName;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;

interface DataWrite
{
    long nodeCreate();

    void nodeDelete( long nodeId ) throws EntityNotFoundException;

    long relationshipCreate( int relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException;

    void relationshipDelete( long relationshipId ) throws EntityNotFoundException;

    /**
     * Labels a node with the label corresponding to the given label id.
     * If the node already had that label nothing will happen. Label ids
     * are retrieved from {@link org.neo4j.kernel.impl.api.operations.KeyWriteOperations#labelGetOrCreateForName(org.neo4j.kernel.api.Statement,
     * String)} or {@link
     * org.neo4j.kernel.impl.api.operations.KeyReadOperations#labelGetForName(org.neo4j.kernel.api.Statement, String)}.
     */
    boolean nodeAddLabel( long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException;

    /**
     * Removes a label with the corresponding id from a node.
     * If the node doesn't have that label nothing will happen. Label ids
     * are retrieved from {@link org.neo4j.kernel.impl.api.operations.KeyWriteOperations#labelGetOrCreateForName(org.neo4j.kernel.api.Statement,
     * String)} or {@link
     * org.neo4j.kernel.impl.api.operations.KeyReadOperations#labelGetForName(org.neo4j.kernel.api.Statement, String)}.
     */
    boolean nodeRemoveLabel( long nodeId, int labelId ) throws EntityNotFoundException;

    Property nodeSetProperty( long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException;

    Property relationshipSetProperty( long relationshipId, DefinedProperty property ) throws EntityNotFoundException;

    Property graphSetProperty( DefinedProperty property );

    /**
     * Remove a node's property given the node's id and the property key id and return the value to which
     * it was set or null if it was not set on the node
     */
    Property nodeRemoveProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    Property relationshipRemoveProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException;

    Property graphRemoveProperty( int propertyKeyId );

    // Implementation notes;
    // This is here in the spirit of iterative development - I'm not actually sure this is the right place in the stack for this
    // primitive. Another alternative would be to have the kernel only handle storage of procedures, and introduce a new service
    // for actually compiling and working with them. However, it is arguably convenient to have them here. In any case, please feel
    // free to move this if it gets awkward in the future.
    //
    // TODO: We should ideally not take List<Object> here, but rather AnyType[], highlighting that only valid Neo4j types
    // can be used as arguments to these calls. That same principle applies generally - now that we have a strict type system definition
    // on the kernel level, properties and other operations that deal with the graph domain types should use the Neo4j type system.
    void procedureCall( ProcedureName signature, List<Object> args, Visitor<List<Object>,ProcedureException> visitor ) throws ProcedureException;

}
