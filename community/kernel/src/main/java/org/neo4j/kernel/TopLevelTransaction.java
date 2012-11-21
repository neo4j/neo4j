/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.WritableTransactionState.LockElement;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;

public class TopLevelTransaction implements Transaction
{
    static class TransactionOutcome
    {
        private boolean success = false;
        private boolean failure = false;

        public void failed()
        {
            failure = true;
        }

        public void success()
        {
            success = true;
        }

        public boolean canCommit()
        {
            return success && !failure;
        }

        public boolean triedToSucceed()
        {
            return success;
        }
    }
    
    private final AbstractTransactionManager transactionManager;
    private final TransactionOutcome transactionOutcome = new TransactionOutcome();
    private final LockManager lockManager;
    private final TransactionState state;

    public TopLevelTransaction( AbstractTransactionManager transactionManager, LockManager lockManager,
            TransactionState state )
    {
        this.transactionManager = transactionManager;
        this.lockManager = lockManager;
        this.state = state;
    }

    public void failure()
    {
        transactionOutcome.failed();
        try
        {
            transactionManager.getTransaction().setRollbackOnly();
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                "Failed to mark transaction as rollback only.", e );
        }
    }

    public void success()
    {
        transactionOutcome.success();
    }
    
    protected boolean isMarkedAsSuccessful()
    {
        try
        {
            return transactionOutcome.canCommit() && transactionManager.getTransaction().getStatus() !=
                    Status.STATUS_MARKED_ROLLBACK;
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void finish()
    {
        try
        {
            javax.transaction.Transaction transaction = transactionManager.getTransaction();
            if ( transaction != null )
            {
                if ( transactionOutcome.canCommit()  )
                {
                    // TODO Why call transaction commit, since it just delegates back to TxManager.commit()?
                    transaction.commit();
                }
                else
                {
                    transaction.rollback();
                }
            }
        }
        catch ( RollbackException e )
        {
            throw new TransactionFailureException( "Unable to commit transaction", e );
        }
        catch ( Exception e )
        {
            if ( transactionOutcome.triedToSucceed() )
            {
                throw new TransactionFailureException(
                    "Unable to commit transaction", e );
            }
            else
            {
                throw new TransactionFailureException(
                    "Unable to rollback transaction", e );
            }
        }
    }
    
    @Override
    public Lock acquireWriteLock( PropertyContainer entity )
    {
        lockManager.getWriteLock( entity );
        LockElement lockElement = state.addLockToTransaction( lockManager, entity, LockType.WRITE );
        return new LockImpl( lockManager, lockElement );
    }

    @Override
    public Lock acquireReadLock( PropertyContainer entity )
    {
        lockManager.getReadLock( entity );
        LockElement lockElement = state.addLockToTransaction( lockManager, entity, LockType.READ );
        return new LockImpl( lockManager, lockElement );
    }
}
