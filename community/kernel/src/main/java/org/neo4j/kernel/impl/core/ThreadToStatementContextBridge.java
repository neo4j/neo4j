/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.function.Supplier;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.AvailabilityGuard;

/**
 * This is meant to serve as the bridge that tie transactions to threads.
 * APIs will use this to get the appropriate {@link Statement} when it performs operations.
 */
public class ThreadToStatementContextBridge implements Supplier<Statement>
{
    private final ThreadLocal<KernelTransaction> threadToTransactionMap = new ThreadLocal<>();
    private final AvailabilityGuard availabilityGuard;

    public ThreadToStatementContextBridge( AvailabilityGuard availabilityGuard )
    {
        this.availabilityGuard = availabilityGuard;
    }

    public boolean hasTransaction()
    {
        KernelTransaction kernelTransaction = threadToTransactionMap.get();
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
        threadToTransactionMap.set( transaction );
    }

    public void unbindTransactionFromCurrentThread()
    {
        threadToTransactionMap.remove();
    }

    @Override
    public Statement get()
    {
        return getKernelTransactionBoundToThisThread( true ).acquireStatement();
    }

    public void assertInUnterminatedTransaction()
    {
        assertInUnterminatedTransaction( threadToTransactionMap.get() );
    }

    public KernelTransaction getKernelTransactionBoundToThisThread( boolean strict )
    {
        KernelTransaction transaction = threadToTransactionMap.get();
        if ( strict )
        {
            assertInUnterminatedTransaction( transaction );
        }
        return transaction;
    }

    private void assertInUnterminatedTransaction( KernelTransaction transaction )
    {
        if ( availabilityGuard.isShutdown() )
        {
            throw new DatabaseShutdownException();
        }
        if ( transaction == null )
        {
            throw new BridgeNotInTransactionException();
        }
        if ( transaction.isTerminated() )
        {
            throw new TransactionTerminatedException( transaction.getReasonIfTerminated().orElse( Status.Transaction.Terminated ) );
        }
    }

    private static class BridgeNotInTransactionException extends NotInTransactionException implements Status.HasStatus
    {
        @Override
        public Status status()
        {
            return Status.Request.TransactionRequired;
        }
    }
}
