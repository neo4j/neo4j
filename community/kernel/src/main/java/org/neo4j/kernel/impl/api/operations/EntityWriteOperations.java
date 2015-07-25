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

import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.KernelStatement;

public interface EntityWriteOperations
{
    // Currently, of course, most relevant operations here are still in the old core API implementation.

    long relationshipCreate( KernelStatement statement,
            int relationshipTypeId,
            long startNodeId,
            long endNodeId ) throws EntityNotFoundException;

    long nodeCreate( KernelStatement statement );

    void nodeDelete( KernelStatement state, NodeItem node );

    void relationshipDelete( KernelStatement state, RelationshipItem relationship );

    /**
     * Labels a node with the label corresponding to the given label id.
     * If the node already had that label nothing will happen. Label ids
     * are retrieved from {@link KeyWriteOperations#labelGetOrCreateForName(org.neo4j.kernel.api.Statement, String)}
     * or {@link
     * KeyReadOperations#labelGetForName(org.neo4j.kernel.api.Statement, String)}.
     */
    boolean nodeAddLabel( KernelStatement state, NodeItem node, int labelId )
            throws ConstraintValidationKernelException;

    /**
     * Removes a label with the corresponding id from a node.
     * If the node doesn't have that label nothing will happen. Label ids
     * are retrieved from {@link KeyWriteOperations#labelGetOrCreateForName(org.neo4j.kernel.api.Statement, String)}
     * or {@link
     * KeyReadOperations#labelGetForName(org.neo4j.kernel.api.Statement, String)}.
     */
    boolean nodeRemoveLabel( KernelStatement state, NodeItem node, int labelId );

    Property nodeSetProperty( KernelStatement state, NodeItem node, DefinedProperty property )
            throws ConstraintValidationKernelException;

    Property relationshipSetProperty( KernelStatement state, RelationshipItem relationship, DefinedProperty property );

    Property graphSetProperty( KernelStatement state, DefinedProperty property );

    /**
     * Remove a node's property given the node's id and the property key id and return the value to which
     * it was set or null if it was not set on the node
     */
    Property nodeRemoveProperty( KernelStatement state, NodeItem node, int propertyKeyId );

    Property relationshipRemoveProperty( KernelStatement state, RelationshipItem relationship, int propertyKeyId );

    Property graphRemoveProperty( KernelStatement state, int propertyKeyId );
}
