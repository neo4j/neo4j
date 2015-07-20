/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.CloseListener;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status.Classification;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class TopLevelTransaction implements Transaction
{
    private final static PropertyContainerLocker locker = new PropertyContainerLocker();
    private final ThreadToStatementContextBridge stmtProvider;
    private boolean successCalled;
    private boolean failureCalled;
    private final KernelTransaction transaction;

    public TopLevelTransaction( KernelTransaction transaction,
                                final ThreadToStatementContextBridge stmtProvider )
    {
        this.transaction = transaction;
        this.stmtProvider = stmtProvider;
        this.transaction.registerCloseListener( new CloseListener()
        {
            @Override
            public void notify( boolean success )
            {
                stmtProvider.unbindTransactionFromCurrentThread();
            }
        } );
    }

    @Override
    public void failure()
    {
        failureCalled = true;
        transaction.failure();
    }

    @Override
    public void success()
    {
        successCalled = true;
        transaction.success();
    }

    @Override
    public final void finish()
    {
        close();
    }

    @Override
    public final void terminate()
    {
        this.transaction.markForTermination();
    }

    @Override
    public void close()
    {
        try
        {
            if ( transaction.isOpen() )
            {
                transaction.close();
            }
        }
        catch ( TransientFailureException e )
        {
            // We let deadlock exceptions pass through unchanged since they aren't really transaction failures
            // in the same sense as unexpected failures are. A deadlock exception signals that the transaction
            // can be retried and might be successful the next time.
            throw e;
        }
        catch ( Exception e )
        {
            String userMessage = successCalled
                    ? "Transaction was marked as successful, but unable to commit transaction so rolled back."
                    : "Unable to rollback transaction";
            if ( e instanceof KernelException &&
                    ((KernelException)e).status().code().classification() == Classification.TransientError )
            {
                throw new TransientTransactionFailureException( userMessage, e );
            }
            throw new TransactionFailureException( userMessage, e );
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
        return locker.sharedLock(stmtProvider, entity);
    }

    @Deprecated
    public KernelTransaction getTransaction()
    {
        return transaction;
    }

    boolean failureCalled()
    {
        return failureCalled;
    }
}
