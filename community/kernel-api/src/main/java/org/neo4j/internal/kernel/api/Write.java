/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.kernel.api;

import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.values.storable.Value;

/**
 * Defines the write operations of the Kernel API.
 */
public interface Write
{
    /**
     * Create a node.
     * @return The internal id of the created node
     */
    long nodeCreate();

    /**
     * Delete a node.
     * @param node the internal id of the node to delete
     * @return returns true if it deleted a node or false if no node was found for this id
     */
    boolean nodeDelete( long node ) throws AutoIndexingKernelException;

    /**
     * Deletes the node and all relationships connecting the node
     * @param node the node to delete
     * @return the number of deleted relationships
     */
    int nodeDetachDelete( long node ) throws KernelException;

    /**
     * Create a relationship between two nodes.
     * @param sourceNode the source internal node id
     * @param relationshipType the type of the relationship to create
     * @param targetNode the target internal node id
     * @return the internal id of the created relationship
     */
    long relationshipCreate( long sourceNode, int relationshipType, long targetNode ) throws EntityNotFoundException;

    /**
     * Delete a relationship
     * @param relationship the internal id of the relationship to delete
     */
    boolean relationshipDelete( long relationship ) throws AutoIndexingKernelException;
    /**
     * Add a label to a node
     * @param node the internal node id
     * @param nodeLabel the internal id of the label to add
     * @return {@code true} if a label was added otherwise {@code false}
     * @throws ConstraintValidationException if adding the label to node breaks a constraint
     */
    boolean nodeAddLabel( long node, int nodeLabel ) throws KernelException;

    /**
     * Remove a label from a node
     * @param node the internal node id
     * @param nodeLabel the internal id of the label to remove
     * @return {@code true} if node was removed otherwise {@code false}
     */
    boolean nodeRemoveLabel( long node, int nodeLabel ) throws EntityNotFoundException;

    /**
     * Set a property on a node
     * @param node the internal node id
     * @param propertyKey the property key id
     * @param value the value to set
     * @return The replaced value, or Values.NO_VALUE if the node did not have the property before
     */
    Value nodeSetProperty( long node, int propertyKey, Value value )
            throws KernelException;

    /**
     * Remove a property from a node
     * @param node the internal node id
     * @param propertyKey the property key id
     * @return The removed value, or Values.NO_VALUE if the node did not have the property before
     */
    Value nodeRemoveProperty( long node, int propertyKey ) throws EntityNotFoundException, AutoIndexingKernelException;

    /**
     * Set a property on a relationship
     * @param relationship the internal relationship id
     * @param propertyKey the property key id
     * @param value the value to set
     * @return The replaced value, or Values.NO_VALUE if the relationship did not have the property before
     */
    Value relationshipSetProperty( long relationship, int propertyKey, Value value ) throws EntityNotFoundException, AutoIndexingKernelException;

    /**
     * Remove a property from a relationship
     * @param relationship the internal relationship id
     * @param propertyKey the property key id
     * @return The removed value, or Values.NO_VALUE if the relationship did not have the property before
     */
    Value relationshipRemoveProperty( long relationship, int propertyKey ) throws EntityNotFoundException, AutoIndexingKernelException;

    /**
     * Set a property on the graph
     * @param propertyKey the property key id
     * @param value the value to set
     * @return The replaced value, or Values.NO_VALUE if the graph did not have the property before
     */
    Value graphSetProperty( int propertyKey, Value value );

    /**
     * Remove a property from the graph
     * @param propertyKey the property key id
     * @return The removed value, or Values.NO_VALUE if the graph did not have the property before
     */
    Value graphRemoveProperty( int propertyKey );
}
