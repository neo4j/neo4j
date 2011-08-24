/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;

public class TopLevelTransaction implements Transaction
{
    private static class TransactionOutcome {
        private boolean success = false;
        private boolean failure = false;
    
        public void failed() {
            failure = true;
        }
        
        public void success() {
            success = true;
        }
        
        public boolean canCommit() {
            return success && !failure;
        }

        public boolean triedToSucceed()
        {
            return success;
        }
        
    }
    
    private final TransactionManager transactionManager;
    private final TransactionOutcome transactionOutcome = new TransactionOutcome();

    public TopLevelTransaction( TransactionManager transactionManager )
    {
        this.transactionManager = transactionManager;
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

    protected TransactionManager getTransactionManager()
    {
        return this.transactionManager;
    }
    
    public void finish()
    {
        try
        {
            if ( transactionOutcome.canCommit()  )
            {
                if ( transactionManager.getTransaction() != null )
                {
                    transactionManager.getTransaction().commit();
                }
            }
            else
            {
                if ( transactionManager.getTransaction() != null )
                {
                    transactionManager.getTransaction().rollback();
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
}
