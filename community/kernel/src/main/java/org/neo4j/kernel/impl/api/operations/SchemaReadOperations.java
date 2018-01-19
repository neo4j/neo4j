/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.storageengine.api.schema.PopulationProgress;

public interface SchemaReadOperations
{
    /**
     * Returns the descriptor for the given labelId and propertyKey.
     */
    SchemaIndexDescriptor indexGetForSchema( KernelStatement state, LabelSchemaDescriptor descriptor );

    /**
     * Get all indexes for a label.
     */
    Iterator<SchemaIndexDescriptor> indexesGetForLabel( KernelStatement state, int labelId );

    /**
     * Returns all indexes.
     */
    Iterator<SchemaIndexDescriptor> indexesGetAll( KernelStatement state );

    /**
     * Retrieve the state of an index.
     */
    InternalIndexState indexGetState( KernelStatement state, SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Retrieve the index provider descriptor for an index.
     */
    IndexProvider.Descriptor indexGetProviderDescriptor( KernelStatement state, SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Retrieve the population progress of an index.
     */
    PopulationProgress indexGetPopulationProgress( KernelStatement state, SchemaIndexDescriptor descriptor ) throws
            IndexNotFoundKernelException;

    /**
     * Get the index size.
     **/
    long indexSize( KernelStatement state, SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Calculate the index unique values percentage.
     **/
    double indexUniqueValuesPercentage( KernelStatement state, SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Returns the failure description of a failed index.
     */
    String indexGetFailure( Statement state, SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Get all constraints applicable to label and propertyKeys.
     */
    Iterator<ConstraintDescriptor> constraintsGetForSchema( KernelStatement state, SchemaDescriptor descriptor );

    /**
     * Returns true if a constraint exists that matches the given {@link ConstraintDescriptor}.
     */
    boolean constraintExists( KernelStatement state, ConstraintDescriptor descriptor );

    /**
     * Get all constraints applicable to label.
     */
    Iterator<ConstraintDescriptor> constraintsGetForLabel( KernelStatement state, int labelId );

    /**
     * Get all constraints applicable to relationship type.
     */
    Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( KernelStatement state, int typeId );

    /**
     * Get all constraints.
     */
    Iterator<ConstraintDescriptor> constraintsGetAll( KernelStatement state );

    /**
     * Get the owning constraint for a constraint index. Returns null if the index does not have an owning constraint.
     */
    Long indexGetOwningUniquenessConstraintId( KernelStatement state, SchemaIndexDescriptor index );

    /**
     * Get the index id (the id or the schema rule record) for a committed index
     * - throws exception for indexes that aren't committed.
     */
    long indexGetCommittedId( KernelStatement state, SchemaIndexDescriptor index ) throws SchemaRuleNotFoundException;
}
