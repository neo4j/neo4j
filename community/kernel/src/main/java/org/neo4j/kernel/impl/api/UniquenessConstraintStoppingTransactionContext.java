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

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public class UniquenessConstraintStoppingTransactionContext extends DelegatingTransactionContext
{
    public UniquenessConstraintStoppingTransactionContext( TransactionContext delegate )
    {
        super( delegate );
    }

    @Override
    public StatementContext newStatementContext()
    {
        StatementContext delegate = super.newStatementContext();
        UniquenessConstraintStoppingStatementContext schemaOperations =
                new UniquenessConstraintStoppingStatementContext( delegate );
        return new CompositeStatementContext( delegate, schemaOperations );
    }

    private class UniquenessConstraintStoppingStatementContext extends DelegatingSchemaOperations
    {
        private UniquenessConstraintStoppingStatementContext( SchemaOperations schemaOperations )
        {
            super( schemaOperations );
        }

        @Override
        public UniquenessConstraint uniquenessConstraintCreate( long labelId, long propertyKeyId )
                throws SchemaKernelException
        {
            throw unsupportedOperation();
        }

        @Override
        public void constraintDrop( UniquenessConstraint constraint )
        {
            throw unsupportedOperation();
        }

        @Override
        public IndexDescriptor uniqueIndexCreate( long labelId, long propertyKey ) throws SchemaKernelException
        {
            throw unsupportedOperation();
        }

        @Override
        public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
        {
            throw unsupportedOperation();
        }

        private RuntimeException unsupportedOperation()
        {
            return new UnsupportedSchemaModificationException();
        }
    }
}
