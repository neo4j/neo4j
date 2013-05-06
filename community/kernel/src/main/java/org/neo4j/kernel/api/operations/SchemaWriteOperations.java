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

import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public interface SchemaWriteOperations
{
    /**
     * Adds a {@link IndexDescriptor} to the database which applies globally on both
     * existing as well as new data.
     */
    IndexDescriptor addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException;

    /**
     * Drops a {@link IndexDescriptor} from the database
     */
    void dropIndexRule( IndexDescriptor indexRule ) throws ConstraintViolationKernelException;

    UniquenessConstraint addUniquenessConstraint( long labelId, long propertyKeyId );

    void dropConstraint( UniquenessConstraint constraint );
}
