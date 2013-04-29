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

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.LockManager;

public class LockingTransactionContext extends DelegatingTransactionContext
{
    private final LockHolder lockHolder;

    public LockingTransactionContext( TransactionContext actual, LockManager lockManager,
            TransactionManager transactionManager )
    {
        super( actual );
        try
        {
            this.lockHolder = new LockHolder( lockManager, transactionManager.getTransaction() );
        }
        catch ( SystemException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( "Unable to get transaction", e );
        }
    }

    @Override
    public StatementContext newStatementContext()
    {
        // The actual transaction context
        StatementContext result = super.newStatementContext();
        // + Locking
        result = new LockingStatementContext( result, lockHolder );
        
        // done
        return result;
    }

    @Override
    public void commit() throws TransactionFailureException
    {
        // TODO: this checking should be removed at some point in the future.
        // Currently the TxManager will release all locks if the transaction fails to commit.
        // That should not be the case when the transaction code has been refactored (moved away from JTA).
        boolean unlock = true;
        try
        {
            super.commit();
        }
        catch ( TransactionFailureException e )
        {
            unlock = false;
            throw e;
        }
        finally
        {
            if ( unlock )
            {
                lockHolder.releaseLocks();
            }
        }
    }

    @Override
    public void rollback()
    {
        try
        {
            super.rollback();
        }
        finally
        {
            lockHolder.releaseLocks();
        }
    }
}
