/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Iterator;

import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.PopulationProgress;

/**
 * The reduced core set of schema read methods
 */
public interface SchemaReadCore
{
    /**
     * Acquire a reference to the index mapping the given {@code SchemaDescriptor}.
     *
     * @param schema {@link SchemaDescriptor} for the index
     * @return the IndexReference, or {@link IndexReference#NO_INDEX} if such an index does not exist.
     */
    IndexReference index( SchemaDescriptor schema );

    /**
     * Returns all indexes associated with the given label
     *
     * @param labelId The id of the label which associated indexes you are looking for
     * @return The indexes associated with the given label
     */
    Iterator<IndexReference> indexesGetForLabel( int labelId );

    /**
     * Returns all indexes associated with the given relationship type
     *
     * @param relationshipType The id of the relationship type which associated indexes you are looking for
     * @return The indexes associated with the given relationship type.
     */
    Iterator<IndexReference> indexesGetForRelationshipType( int relationshipType );

    /**
     * Returns all indexes used in the database
     *
     * @return all indexes used in the database
     */
    Iterator<IndexReference> indexesGetAll();

    /**
     * Retrieves the state of an index
     *
     * @param index the index which state to retrieve
     * @return The state of the provided index
     * @throws IndexNotFoundKernelException if the index was not found in the database
     */
    InternalIndexState indexGetState( IndexReference index ) throws IndexNotFoundKernelException;

    /**
     * Retrives the population progress of the index
     *
     * @param index The index whose progress to retrieve
     * @return The population progress of the given index
     * @throws IndexNotFoundKernelException if the index was not found in the database
     */
    PopulationProgress indexGetPopulationProgress( IndexReference index ) throws IndexNotFoundKernelException;

    /**
     * Returns the failure description of a failed index.
     *
     * @param index the failed index
     * @return The failure message from the index
     * @throws IndexNotFoundKernelException if the index was not found in the database
     */
    String indexGetFailure( IndexReference index ) throws IndexNotFoundKernelException;

    /**
     * Finds all constraints for the given label
     *
     * @param labelId The id of the label
     * @return All constraints for the given label
     */
    Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId );

    /**
     * Get all constraints applicable to relationship type.
     *
     * @param typeId the id of the relationship type
     * @return An iterator of constraints associated with the given type.
     */
    Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId );

    /**
     * Find all constraints in the database
     *
     * @return An iterator of all the constraints in the database.
     */
    Iterator<ConstraintDescriptor> constraintsGetAll();
}
