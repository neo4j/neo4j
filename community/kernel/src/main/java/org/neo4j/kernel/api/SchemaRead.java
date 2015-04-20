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

import java.util.Iterator;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;

interface SchemaRead
{
    /** Returns the index rule for the given labelId and propertyKey. */
    IndexDescriptor indexesGetForLabelAndPropertyKey( int labelId, int propertyKey )
            throws SchemaRuleNotFoundException;

    /** Get all indexes for a label. */
    Iterator<IndexDescriptor> indexesGetForLabel( int labelId );

    /** Returns all indexes. */
    Iterator<IndexDescriptor> indexesGetAll();

    /** Returns the constraint index for the given labelId and propertyKey. */
    IndexDescriptor uniqueIndexGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
        throws SchemaRuleNotFoundException;

    /** Get all constraint indexes for a label. */
    Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId );

    /** Returns all constraint indexes. */
    Iterator<IndexDescriptor> uniqueIndexesGetAll();

    /** Retrieve the state of an index. */
    InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Calculate the index unique values percentage (range: {@code 0.0} exclusive to {@code 1.0} inclusive). */
    double indexUniqueValuesSelectivity( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Returns the failure description of a failed index. */
    String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /**
     * Get all constraints applicable to label and propertyKey. There are only {@link
     * org.neo4j.kernel.api.constraints.UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( int labelId, int propertyKeyId );

    /**
     * Get all constraints applicable to label. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> constraintsGetForLabel( int labelId );

    /**
     * Get all constraints. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> constraintsGetAll();

    /**
     * Get the owning constraint for a constraint index. Returns null if the index does not have an owning constraint.
     */
    Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException;
}
