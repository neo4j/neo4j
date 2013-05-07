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

import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public interface SchemaReadOperations
{
    /**
     * Returns the index rule for the given labelId and propertyKey.
     */
    IndexDescriptor getIndex( long labelId, long propertyKey ) throws SchemaRuleNotFoundException;

    /**
     * Get all indexes for a label.
     */
    Iterator<IndexDescriptor> getIndexes( long labelId );

    /**
     * Returns all indexes.
     */
    Iterator<IndexDescriptor> getIndexes();

    /**
     * Get all constraint indexes for a label.
     */
    Iterator<IndexDescriptor> getConstraintIndexes( long labelId );

    /**
     * Returns all constraint indexes.
     */
    Iterator<IndexDescriptor> getConstraintIndexes();

    /**
     * Retrieve the state of an index.
     */
    InternalIndexState getIndexState( IndexDescriptor indexRule ) throws IndexNotFoundKernelException;

    /**
     * Get all constraints applicable to label and propertyKey. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> getConstraints( long labelId, long propertyKeyId );

    /**
     * Get all constraints applicable to label. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> getConstraints( long labelId );

    /**
     * Get all constraints. There are only {@link UniquenessConstraint}
     * for the time being.
     */
    Iterator<UniquenessConstraint> getConstraints();

    /**
     * Get the owning constraint for a constraint index.
     */
    Long getOwningConstraint( IndexDescriptor index ) throws SchemaRuleNotFoundException;

    /**
     * Get the index id (the id or the schema rule record) for a committed index
     * - throws exception for indexes that aren't committed.
     */
    long getCommittedIndexId( IndexDescriptor index ) throws SchemaRuleNotFoundException;
}
