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

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.neo4j.kernel.api.StatementContextParts;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.transaction.LockManager;

public class LockingTransactionContext extends DelegatingTransactionContext
{
    private final LockHolder lockHolder;

    public LockingTransactionContext( TransactionContext delegate, LockManager lockManager,
            TransactionManager transactionManager, NodeManager nodeManager )
    {
        super( delegate );
        try
        {
            // TODO Not happy about the NodeManager dependency. It's needed a.t.m. for making
            // equality comparison between GraphProperties instances. It should change.
            this.lockHolder = new LockHolder( lockManager, transactionManager.getTransaction(), nodeManager );
        }
        catch ( SystemException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( "Unable to get transaction", e );
        }
    }

    @Override
    public StatementContextParts newStatementContext()
    {
        // The actual transaction context
        StatementContextParts parts = delegate.newStatementContext();
        
        // + Locking
        LockingStatementContext lockingContext = new LockingStatementContext(
                parts.entityWriteOperations(),
                parts.schemaReadOperations(),
                parts.schemaWriteOperations(),
                parts.schemaStateOperations(),
                lockHolder );
        
        parts.replace(
                null, null, null, lockingContext, lockingContext, lockingContext, lockingContext, null );
        return parts;
    }

    @Override
    public void commit() throws TransactionFailureException
    {
        try
        {
            delegate.commit();
        }
        finally
        {
            lockHolder.releaseLocks();
        }
    }

    @Override
    public void rollback() throws TransactionFailureException
    {
        try
        {
            delegate.rollback();
        }
        finally
        {
            lockHolder.releaseLocks();
        }
    }
}
