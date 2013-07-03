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
import org.neo4j.kernel.api.LifecycleOperations;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.operations.StatementState;

public class ReferenceCountingTransactionContext extends DelegatingTransactionContext
{
    private StatementContextOwner statementContextOwner;

    public ReferenceCountingTransactionContext( KernelTransaction delegate,
            LifecycleOperations refCountingOperations )
    {
        super( delegate );
        statementContextOwner = new StatementContextOwner( refCountingOperations )
        {
            @Override
            protected StatementState createStatementState()
            {
                return ReferenceCountingTransactionContext.this.createOwnedStatementState();
            }
        };
    }
    
    @Override
    public StatementOperationParts newStatementOperations()
    {
        StatementOperationParts parts = delegate.newStatementOperations();
        ReferenceCountingStatementOperations ops = new ReferenceCountingStatementOperations(
                parts.lifecycleOperations() );
        parts.replace( null, null, null, null, null, null, null, ops );
        return parts;
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
