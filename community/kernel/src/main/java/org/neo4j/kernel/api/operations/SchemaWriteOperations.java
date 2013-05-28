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

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public interface SchemaWriteOperations
{
    /**
     * Creates an index, indexing properties with the given {@code propertyKeyId} for nodes with the given
     * {@code labelId}.
     */
    IndexDescriptor indexCreate( long labelId, long propertyKeyId ) throws SchemaKernelException;

    /**
     * Creates an index for use with a uniqueness constraint. The index indexes properties with the given
     * {@code propertyKeyId} for nodes with the given {@code labelId}, and assumes that the database provides it with
     * unique property values. If unique property values are not provided by the database, the index will notify
     * through an exception and enter a "bad state". (This notification facility is used during the verification phase
     * of uniqueness constraint creation).
     *
     * This method is not used from the outside. It is used internally when
     * {@link #uniquenessConstraintCreate(long, long) creating a uniqueness constraint}, invoked through a separate
     * transaction (the separate transaction is why it has to be exposed in this API).
     */
    IndexDescriptor uniqueIndexCreate( long labelId, long propertyKey ) throws SchemaKernelException;

    /** Drops a {@link IndexDescriptor} from the database */
    void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException;

    void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException;

    UniquenessConstraint uniquenessConstraintCreate( long labelId, long propertyKeyId )
            throws SchemaKernelException;

    void constraintDrop( UniquenessConstraint constraint );
}
