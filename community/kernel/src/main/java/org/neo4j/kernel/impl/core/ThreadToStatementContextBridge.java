/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This is meant to serve as the bridge that makes the Beans API tie transactions to threads. The Beans API
 * will use this to get the appropriate {@link Statement} when it performs operations.
 */
public class ThreadToStatementContextBridge extends LifecycleAdapter implements Supplier<Statement>
{
    private final ThreadLocal<TopLevelTransaction> threadToTransactionMap = new ThreadLocal<>();
    private boolean isShutdown;

    public boolean hasTransaction()
    {
        checkIfShutdown();
        return threadToTransactionMap.get() != null;
    }

    public void bindTransactionToCurrentThread( TopLevelTransaction transaction )
    {
        if ( threadToTransactionMap.get() != null )
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
        checkIfShutdown();
        return getKernelTransactionBoundToThisThread( true ).acquireStatement();
    }

    private void assertInUnterminatedTransaction( TopLevelTransaction transaction )
    {
        if ( transaction == null )
        {
            throw new NotInTransactionException();
        }
        Status terminationReason = transaction.getTransaction().getReasonIfTerminated();
        if ( terminationReason != null )
        {
            throw new TransactionTerminatedException( terminationReason );
        }
    }

    public void assertInUnterminatedTransaction()
    {
        checkIfShutdown();
        assertInUnterminatedTransaction( threadToTransactionMap.get() );
    }

    @Override
    public void shutdown() throws Throwable
    {
        isShutdown = true;
    }

    private void checkIfShutdown()
    {
        if ( isShutdown )
        {
            throw new DatabaseShutdownException();
        }
    }

    public TopLevelTransaction getTopLevelTransactionBoundToThisThread( boolean strict )
    {
        TopLevelTransaction transaction = threadToTransactionMap.get();
        if ( strict )
        {
            assertInUnterminatedTransaction( transaction );
        }
        return transaction;
    }

    public KernelTransaction getKernelTransactionBoundToThisThread( boolean strict )
    {
        TopLevelTransaction tx = getTopLevelTransactionBoundToThisThread( strict );
        return tx != null ? tx.getTransaction() : null;
    }
}
