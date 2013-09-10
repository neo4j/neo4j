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
package org.neo4j.kernel.api.operations;

import java.util.Iterator;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public interface SchemaReadOperations
{
    /**
     * Returns the descriptor for the given labelId and propertyKey.
     */
    IndexDescriptor indexesGetForLabelAndPropertyKey( Statement state, long labelId, long propertyKey )
            throws SchemaRuleNotFoundException;

    /**
     * Get all indexes for a label.
     */
    Iterator<IndexDescriptor> indexesGetForLabel( Statement state, long labelId );

    /**
     * Returns all indexes.
     */
    Iterator<IndexDescriptor> indexesGetAll( Statement state );

    /**
     * Get all constraint indexes for a label.
     */
    Iterator<IndexDescriptor> uniqueIndexesGetForLabel( Statement state, long labelId );

    /**
     * Returns all constraint indexes.
     */
    Iterator<IndexDescriptor> uniqueIndexesGetAll( Statement state );

    /**
     * Retrieve the state of an index.
     */
    InternalIndexState indexGetState( Statement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException;
	
    /**
     * Returns the failure description of a failed index.
     */
    String indexGetFailure( Statement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Get all constraints applicable to label and propertyKey. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( Statement state, long labelId, long propertyKeyId );

    /**
     * Get all constraints applicable to label. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> constraintsGetForLabel( Statement state, long labelId );

    /**
     * Get all constraints. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> constraintsGetAll( Statement state );

    /**
     * Get the owning constraint for a constraint index. Returns null if the index does not have an owning constraint.
     */
    Long indexGetOwningUniquenessConstraintId( Statement state, IndexDescriptor index ) throws SchemaRuleNotFoundException;

    /**
     * Get the index id (the id or the schema rule record) for a committed index
     * - throws exception for indexes that aren't committed.
     */
    long indexGetCommittedId( Statement state, IndexDescriptor index ) throws SchemaRuleNotFoundException;
}
