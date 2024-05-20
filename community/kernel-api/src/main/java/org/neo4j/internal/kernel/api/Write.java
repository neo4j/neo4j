/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api;

import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Defines the write operations of the Kernel API.
 */
public interface Write {
    /**
     * Create a node.
     *
     * @return The internal id of the created node
     */
    long nodeCreate();

    /**
     * Create a node, and assign it the given array of labels.
     * <p>
     * This method differs from a {@link #nodeCreate()} and {@link #nodeAddLabel(long, int)} sequence, in that we will
     * avoid taking the "unlabelled node lock" of the {@code nodeCreate}, and we will avoid taking the exclusive node
     * lock in the {@code nodeAddLabel} method.
     *
     * @param labels The labels to assign to the newly created node.
     * @return The internal id of the created node.
     */
    long nodeCreateWithLabels(int[] labels) throws ConstraintValidationException;

    /**
     * Delete a node.
     *
     * @param node the internal id of the node to delete
     * @return returns true if it deleted a node or false if no node was found for this id
     */
    boolean nodeDelete(long node);

    /**
     * Deletes the node and all relationships connecting the node
     *
     * @param node the node to delete
     * @return the number of deleted relationships
     */
    int nodeDetachDelete(long node) throws KernelException;

    /**
     * Create a relationship between two nodes.
     *
     * @param sourceNode the source internal node id
     * @param relationshipType the type of the relationship to create
     * @param targetNode the target internal node id
     * @return the internal id of the created relationship
     */
    long relationshipCreate(long sourceNode, int relationshipType, long targetNode) throws EntityNotFoundException;

    /**
     * Delete a relationship
     *
     * @param relationship the internal id of the relationship to delete
     */
    boolean relationshipDelete(long relationship);

    /**
     * Add a label to a node
     *
     * @param node the internal node id
     * @param nodeLabel the internal id of the label to add
     * @return {@code true} if a label was added otherwise {@code false}
     * @throws ConstraintValidationException if adding the label to node breaks a constraint
     */
    boolean nodeAddLabel(long node, int nodeLabel) throws KernelException;

    /**
     * Remove a label from a node
     *
     * @param node the internal node id
     * @param nodeLabel the internal id of the label to remove
     * @return {@code true} if node was removed otherwise {@code false}
     */
    boolean nodeRemoveLabel(long node, int nodeLabel) throws EntityNotFoundException;

    /**
     * Set a property on a node
     *
     * @param node        the internal node id
     * @param propertyKey the property key id
     * @param value       the value to set
     */
    void nodeSetProperty(long node, int propertyKey, Value value) throws KernelException;

    /**
     * Applies multiple label and property changes to a node in one call, checking constraints on the resulting data, not the intermediary state,
     * which would have been the case if multiple changes gets applied individually via e.g. {@link #nodeSetProperty(long, int, Value)}
     * and {@link #nodeAddLabel(long, int)}.
     *
     * @param node the internal node id.
     * @param addedLabels added labels, applied idempotently.
     * @param removedLabels removed labels, applied idempotently.
     * @param properties added/changed/removed properties. A value of {@link Values#NO_VALUE} means the property should be removed.
     */
    void nodeApplyChanges(long node, IntSet addedLabels, IntSet removedLabels, IntObjectMap<Value> properties)
            throws EntityNotFoundException, ConstraintValidationException;

    /**
     * Applies multiple property changes to a relationship in one call, checking constraints on the resulting data, not the intermediary state,
     * which would have been the case if multiple properties would have been applied individually via e.g. {@link #relationshipSetProperty(long, int, Value)}.
     *
     * @param relationship the internal relationship id.
     * @param properties added/changed/removed properties. A value of {@link Values#NO_VALUE} means the property should be removed.
     */
    void relationshipApplyChanges(long relationship, IntObjectMap<Value> properties)
            throws EntityNotFoundException, ConstraintValidationException;

    /**
     * Remove a property from a node
     *
     * @param node the internal node id
     * @param propertyKey the property key id
     * @return The removed value, or Values.NO_VALUE if the node did not have the property before
     */
    Value nodeRemoveProperty(long node, int propertyKey) throws EntityNotFoundException;

    /**
     * Set a property on a relationship
     *
     * @param relationship the internal relationship id
     * @param propertyKey  the property key id
     * @param value        the value to set
     */
    void relationshipSetProperty(long relationship, int propertyKey, Value value)
            throws EntityNotFoundException, ConstraintValidationException;

    /**
     * Remove a property from a relationship
     *
     * @param relationship the internal relationship id
     * @param propertyKey the property key id
     * @return The removed value, or Values.NO_VALUE if the relationship did not have the property before
     */
    Value relationshipRemoveProperty(long relationship, int propertyKey) throws EntityNotFoundException;
}
