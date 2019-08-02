/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;

/**
 * This is meant to serve as the bridge that tie transactions to threads.
 * APIs will use this to get the appropriate {@link Statement} when it performs operations.
 */
public class ThreadToStatementContextBridge
{
    private final ThreadLocal<KernelTransaction> threadLocalTransaction = new ThreadLocal<>();

    public boolean hasTransaction()
    {
        KernelTransaction kernelTransaction = threadLocalTransaction.get();
        if ( kernelTransaction != null )
        {
            assertInUnterminatedTransaction( kernelTransaction );
            return true;
        }
        return false;
    }

    public void bindTransactionToCurrentThread( KernelTransaction transaction )
    {
        if ( hasTransaction() )
        {
            throw new IllegalStateException( Thread.currentThread() + " already has a transaction bound" );
        }
        threadLocalTransaction.set( transaction );
    }

    public KernelTransaction getAndUnbindAnyTransaction()
    {
        KernelTransaction kernelTransaction = threadLocalTransaction.get();
        if ( kernelTransaction != null )
        {
            threadLocalTransaction.remove();
        }
        return kernelTransaction;
    }

    public void unbindTransactionFromCurrentThread()
    {
        threadLocalTransaction.remove();
    }

    public Statement get( DatabaseId databaseId )
    {
        return getKernelTransactionBoundToThisThread( true, databaseId ).acquireStatement();
    }

    public void assertInUnterminatedTransaction()
    {
        assertInUnterminatedTransaction( threadLocalTransaction.get() );
    }

    public KernelTransaction getKernelTransactionBoundToThisThread( boolean strict, DatabaseId databaseId )
    {
        KernelTransaction transaction = threadLocalTransaction.get();
        validateTransactionDatabaseIs( transaction, databaseId );
        if ( strict )
        {
            assertInUnterminatedTransaction( transaction );
        }
        return transaction;
    }

    private static void assertInUnterminatedTransaction( KernelTransaction transaction )
    {
        if ( transaction == null )
        {
            throw new NotInTransactionException();
        }
        if ( transaction.getAvailabilityGuard().isShutdown() )
        {
            throw new DatabaseShutdownException();
        }
        if ( transaction.isTerminated() )
        {
            throw new TransactionTerminatedException( transaction.getReasonIfTerminated().orElse( Status.Transaction.Terminated ) );
        }
    }

    private void validateTransactionDatabaseIs( KernelTransaction contextTransaction, DatabaseId requestedDatabaseId )
    {
        if ( contextTransaction == null )
        {
            return;
        }
        DatabaseId boundTransactionDbId = contextTransaction.getDatabaseId();
        if ( !boundTransactionDbId.equals( requestedDatabaseId ) )
        {
            throw new TransactionFailureException(
                    format( "Fail to get transaction for database '%s', since '%s' database transaction already bound to this thread.",
                            requestedDatabaseId.name(), boundTransactionDbId.name() ) );
        }
    }
}
