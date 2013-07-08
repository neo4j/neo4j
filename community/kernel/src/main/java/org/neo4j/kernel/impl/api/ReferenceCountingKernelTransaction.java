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

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.operations.LifecycleOperations;
import org.neo4j.kernel.api.operations.StatementState;

public class ReferenceCountingKernelTransaction extends DelegatingKernelTransaction
{
    private StatementStateOwner statementContextOwner;

    public ReferenceCountingKernelTransaction( KernelTransaction delegate,
            LifecycleOperations refCountingOperations )
    {
        super( delegate );
        statementContextOwner = new StatementStateOwner( refCountingOperations )
        {
            @Override
            protected StatementState createStatementState()
            {
                return ReferenceCountingKernelTransaction.this.createOwnedStatementState();
            }
        };
    }
    
    @Override
    public StatementOperationParts newStatementOperations()
    {
        StatementOperationParts parts = delegate.newStatementOperations();
        ReferenceCountingStatementOperations ops = new ReferenceCountingStatementOperations();
        return parts.override( null, null, null, null, null, null, null, ops );
    }
    
    @Override
    public StatementState newStatementState()
    {
        return statementContextOwner.getStatementState();
    }
    
    private StatementState createOwnedStatementState()
    {
        return delegate.newStatementState();
    }
    
    @Override
    public void commit() throws TransactionFailureException
    {
        statementContextOwner.closeAllStatements();
        delegate.commit();
    }

    @Override
    public void rollback() throws TransactionFailureException
    {
        statementContextOwner.closeAllStatements();
        delegate.rollback();
    }
}
