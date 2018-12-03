/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.storageengine.api;

import java.util.Iterator;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;

/**
 * Abstraction for accessing data from a {@link StorageEngine}.
 * A reader is expected to be scoped to one transaction or query or similar.
 */
public interface StorageReader extends AutoCloseable
{
    /**
     * Closes this reader and all associated resources.
     */
    @Override
    void close();

    /**
     * @param labelId label to list indexes for.
     * @return {@link StorageIndexReference} associated with the given {@code labelId}.
     */
    Iterator<StorageIndexReference> indexesGetForLabel( int labelId );

    /**
     * @param name name of index to find
     * @return {@link StorageIndexReference} associated with the given {@code name}.
     */
    StorageIndexReference indexGetForName( String name );

    /**
     * @return all {@link StorageIndexReference} in storage.
     */
    Iterator<StorageIndexReference> indexesGetAll();

    /**
     * Returns all indexes (including unique) related to a property.
     */
    Iterator<StorageIndexReference> indexesGetRelatedToProperty( int propertyId );

    /**
     * Looks for a stored index by given {@code descriptor}
     *
     * @param descriptor a description of the index.
     * @return {@link StorageIndexReference} for matching index, or {@code null} if not found.
     */
    StorageIndexReference indexGetForSchema( SchemaDescriptor descriptor );

    /**
     * @param index {@link StorageIndexReference} to get related uniqueness constraint for.
     * @return schema rule id of uniqueness constraint that owns the given {@code index}, or {@code null}
     * if the given index isn't related to a uniqueness constraint.
     */
    Long indexGetOwningUniquenessConstraintId( StorageIndexReference index );

    /**
     * @param descriptor describing the label and property key (or keys) defining the requested constraint.
     * @return node property constraints associated with the label and one or more property keys token ids.
     */
    Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor );

    boolean constraintExists( ConstraintDescriptor descriptor );

    /**
     * @param labelId label token id.
     * @return node property constraints associated with the label token id.
     */
    Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId );

    /**
     * @param typeId relationship type token id .
     * @return relationship property constraints associated with the relationship type token id.
     */
    Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId );

    /**
     * @return all stored property constraints.
     */
    Iterator<ConstraintDescriptor> constraintsGetAll();

    /**
     * Returns number of stored nodes labeled with the label represented by {@code labelId}.
     *
     * @param labelId label id to match.
     * @return number of stored nodes with this label.
     */
    long countsForNode( int labelId );

    /**
     * Returns number of stored relationships of a certain {@code typeId} whose start/end nodes are labeled
     * with the {@code startLabelId} and {@code endLabelId} respectively.
     *
     * @param startLabelId label id of start nodes to match.
     * @param typeId relationship type id to match.
     * @param endLabelId label id of end nodes to match.
     * @return number of stored relationships matching these criteria.
     */
    long countsForRelationship( int startLabelId, int typeId, int endLabelId );

    /**
     * Returns size of index, i.e. number of entities in that index.
     *
     * @param descriptor {@link SchemaDescriptor} to return size for.
     * @return number of entities in the given index.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    long indexSize( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Returns percentage of values in the given {@code index} are unique. A value of {@code 1.0} means that
     * all values in the index are unique, e.g. that there are no duplicate values. A value of, say {@code 0.9}
     * means that 10% of the values are duplicates.
     *
     * @param descriptor {@link SchemaDescriptor} to get uniqueness percentage for.
     * @return percentage of values being unique in this index, max {@code 1.0} for all unique.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    double indexUniqueValuesPercentage( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

    long nodesGetCount();

    long relationshipsGetCount();

    int labelCount();

    int propertyKeyCount();

    int relationshipTypeCount();

    DoubleLongRegister indexUpdatesAndSize( SchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException;

    DoubleLongRegister indexSample( SchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException;

    boolean nodeExists( long id );

    boolean relationshipExists( long id );

    <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StorageReader, T> factory );

    /**
     * Batched all node scan
     * @return a new AllNodeScan maintaining the state of the batched all-node scan
     */
    AllNodeScan allNodeScan();

    /**
     * Batched all relationship scan
     * @return a new AllRelationship maintaining the state of the batched all-relationship scan
     */
    AllRelationshipsScan allRelationshipScan();

    /**
     * @return a new {@link StorageNodeCursor} capable of reading node data from the underlying storage.
     */
    StorageNodeCursor allocateNodeCursor();

    /**
     * @return a new {@link StoragePropertyCursor} capable of reading property data from the underlying storage.
     */
    StoragePropertyCursor allocatePropertyCursor();

    /**
     * @return a new {@link StorageRelationshipGroupCursor} capable of reading relationship group data from the underlying storage.
     */
    StorageRelationshipGroupCursor allocateRelationshipGroupCursor();

    /**
     * @return a new {@link StorageRelationshipTraversalCursor} capable of traversing relationships from the underlying storage.
     */
    StorageRelationshipTraversalCursor allocateRelationshipTraversalCursor();

    /**
     * @return a new {@link StorageRelationshipScanCursor} capable of reading relationship data from the underlying storage.
     */
    StorageRelationshipScanCursor allocateRelationshipScanCursor();
}
