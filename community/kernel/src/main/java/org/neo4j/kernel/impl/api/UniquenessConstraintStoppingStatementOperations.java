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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.KernelStatement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.AddIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

class UniquenessConstraintStoppingStatementOperations implements SchemaWriteOperations
{
    private final SchemaWriteOperations schemaWriteDelegate;

    public UniquenessConstraintStoppingStatementOperations( SchemaWriteOperations schemaWriteDelegate )
    {
        this.schemaWriteDelegate = schemaWriteDelegate;
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( KernelStatement state, long labelId, long propertyKeyId )
    {
        throw unsupportedOperation();
    }

    @Override
    public void constraintDrop( KernelStatement state, UniquenessConstraint constraint )
    {
        throw unsupportedOperation();
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        throw unsupportedOperation();
    }

    private RuntimeException unsupportedOperation()
    {
        return new UnsupportedSchemaModificationException();
    }

    // === TODO Below is unnecessary delegate methods

    @Override
    public IndexDescriptor indexCreate( KernelStatement state, long labelId, long propertyKeyId )
            throws AddIndexFailureException, AlreadyIndexedException, AlreadyConstrainedException
    {
        return schemaWriteDelegate.indexCreate( state, labelId, propertyKeyId );
    }

    @Override
    public void indexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        schemaWriteDelegate.indexDrop( state, descriptor );
    }
}
