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
package org.neo4j.storageengine.api;

import java.util.Iterator;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.PopulationProgress;

public interface StorageSchemaReader
{
    /**
     * Looks for a stored index by given {@code descriptor}
     *
     * @param descriptor a description of the index.
     * @return {@link CapableIndexDescriptor} for matching index, or {@code null} if not found.
     */
    CapableIndexDescriptor indexGetForSchema( SchemaDescriptor descriptor );

    /**
     * @param labelId label to list indexes for.
     * @return {@link IndexDescriptor} associated with the given {@code labelId}.
     */
    Iterator<CapableIndexDescriptor> indexesGetForLabel( int labelId );

    /**
     * @param relationshipType relationship type to list indexes for.
     * @return {@link IndexDescriptor} associated with the given {@code relationshipType}.
     */
    Iterator<CapableIndexDescriptor> indexesGetForRelationshipType( int relationshipType );

    /**
     * @return all {@link CapableIndexDescriptor} in storage.
     */
    Iterator<CapableIndexDescriptor> indexesGetAll();

    /**
     * Returns state of a stored index.
     *
     * @param descriptor {@link IndexDescriptor} to get state for.
     * @return {@link InternalIndexState} for index.
     * @throws IndexNotFoundKernelException if index not found.
     */
    InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * @param descriptor {@link SchemaDescriptor} to get population progress for.
     * @return progress of index population, which is the initial state of an index when it's created.
     * @throws IndexNotFoundKernelException if index not found.
     */
    PopulationProgress indexGetPopulationProgress( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Returns any failure that happened during population or operation of an index. Such failures
     * are persisted and can be accessed even after restart.
     *
     * @param descriptor {@link SchemaDescriptor} to get failure for.
     * @return failure of an index, or {@code null} if index is working as it should.
     * @throws IndexNotFoundKernelException if index not found.
     */
    String indexGetFailure( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException;

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
}
