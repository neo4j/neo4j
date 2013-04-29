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
    IndexDescriptor getIndexRule( long labelId, long propertyKey ) throws SchemaRuleNotFoundException;

    /**
     * Get all indexes for a label.
     */
    Iterator<IndexDescriptor> getIndexRules( long labelId );

    /**
     * Returns all index rules.
     */
    Iterator<IndexDescriptor> getIndexRules();

    /**
     * Retrieve the state of an index.
     */
    InternalIndexState getIndexState( IndexDescriptor indexRule ) throws IndexNotFoundKernelException;

    Iterator<UniquenessConstraint> getConstraints( long labelId, long propertyKeyId );
}
