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

import java.util.Iterator;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.store.SchemaStorage;

public interface SchemaReadOperations
{
    /**
     * Returns the descriptor for the given labelId and propertyKey.
     */
    IndexDescriptor indexesGetForLabelAndPropertyKey( KernelStatement state, int labelId, int propertyKey );

    /**
     * Get all indexes for a label.
     */
    Iterator<IndexDescriptor> indexesGetForLabel( KernelStatement state, int labelId );

    /**
     * Returns all indexes.
     */
    Iterator<IndexDescriptor> indexesGetAll( KernelStatement state );

    /**
     * Get all constraint indexes for a label.
     */
    Iterator<IndexDescriptor> uniqueIndexesGetForLabel( KernelStatement state, int labelId );

    /**
     * Returns all constraint indexes.
     */
    Iterator<IndexDescriptor> uniqueIndexesGetAll( KernelStatement state );

    /**
     * Retrieve the state of an index.
     */
    InternalIndexState indexGetState( KernelStatement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Calculate the index unique values percentage.
     **/
    double indexUniqueValuesPercentage( KernelStatement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Returns the failure description of a failed index.
     */
    String indexGetFailure( Statement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Get all constraints applicable to label and propertyKey. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( KernelStatement state, int labelId, int propertyKeyId );

    /**
     * Get all constraints applicable to label. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> constraintsGetForLabel( KernelStatement state, int labelId );

    /**
     * Get all constraints. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> constraintsGetAll( KernelStatement state );

    /**
     * Get the owning constraint for a constraint index. Returns null if the index does not have an owning constraint.
     */
    Long indexGetOwningUniquenessConstraintId( KernelStatement state, IndexDescriptor index ) throws SchemaRuleNotFoundException;

    /**
     * Get the index id (the id or the schema rule record) for a committed index
     * - throws exception for indexes that aren't committed.
     */
    long indexGetCommittedId( KernelStatement state, IndexDescriptor index, SchemaStorage.IndexRuleKind constraint ) throws SchemaRuleNotFoundException;
}
