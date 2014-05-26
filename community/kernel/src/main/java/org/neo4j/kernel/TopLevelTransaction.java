/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
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

        public boolean successCalled()
        {
            return success;
        }

        public boolean failureCalled()
        {
            return failure;
        }
    }

    private final static PropertyContainerLocker locker = new PropertyContainerLocker();
    private final ThreadToStatementContextBridge stmtProvider;
    private final TransactionOutcome transactionOutcome = new TransactionOutcome();
    private final KernelAPI kernel;
    private final KernelTransaction transaction;

    public TopLevelTransaction( KernelAPI kernel, KernelTransaction transaction,
                                ThreadToStatementContextBridge stmtProvider )
    {
        this.kernel = kernel;
        this.transaction = transaction;
        this.stmtProvider = stmtProvider;
    }

    @Override
    public void failure()
    {
        transactionOutcome.failed();
    }

    @Override
    public void success()
    {
    	transactionOutcome.success();
    }

    @Override
    public final void finish()
    {
        close();
    }

    @Override
    public void close()
    {
        try
        {
            if ( transaction != null )
            {
                if ( transactionOutcome.canCommit()  )
                {
                    kernel.commit( transaction );
                }
                else
                {
                    kernel.rollback( transaction );
                }
            }
        }
        catch ( Exception e )
        {
            if ( transactionOutcome.successCalled() )
            {
                throw new TransactionFailureException( "Unable to commit transaction", e );
            }
            else
            {
                throw new TransactionFailureException( "Unable to rollback transaction", e );
            }
        }
        finally
        {
            stmtProvider.unbindTransactionFromCurrentThread( this );
        }
    }

    @Override
    public Lock acquireWriteLock( PropertyContainer entity )
    {
        return locker.exclusiveLock( stmtProvider, entity );
    }

    @Override
    public Lock acquireReadLock( PropertyContainer entity )
    {
        return locker.sharedLock( stmtProvider, entity );
    }

    @Deprecated
    public KernelTransaction getTransaction()
    {
        return transaction;
    }

	TransactionOutcome getTransactionOutcome()
	{
		return transactionOutcome;
	}
}
