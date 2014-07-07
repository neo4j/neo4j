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
package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This is meant to serve as the bridge that makes the Beans API tie transactions to threads. The Beans API
 * will use this to get the appropriate {@link Statement} when it performs operations.
 */
public class ThreadToStatementContextBridge extends LifecycleAdapter implements Provider<Statement>
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
    public Statement instance()
    {
        checkIfShutdown();
        return getKernelTransactionBoundToThisThread( true ).acquireStatement();
    }

    private void assertInTransaction( TopLevelTransaction transaction )
    {
        if ( transaction == null )
        {
            throw new NotInTransactionException();
        }
    }

    private void assertNotInterrupted( TopLevelTransaction transaction )
    {
        if ( transaction.getTransaction().shouldBeTerminated() )
        {
            throw new TransactionTerminatedException();
        }
    }

    public void assertInUninterruptedTransaction()
    {
        checkIfShutdown();
        TopLevelTransaction transaction = threadToTransactionMap.get();
        assertInTransaction( transaction );
        assertNotInterrupted( transaction );
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
            assertInTransaction( transaction );
        }
        return transaction;
    }

    public KernelTransaction getKernelTransactionBoundToThisThread( boolean strict )
    {
        TopLevelTransaction tx = getTopLevelTransactionBoundToThisThread( strict );
        return tx != null ? tx.getTransaction() : null;
    }

    public TransactionRecordState getTransactionRecordStateBoundToThisThread( boolean strict )
    {
        KernelTransaction tx = getKernelTransactionBoundToThisThread( strict );
        return tx != null ? tx.getTransactionRecordState() : null;
    }
}
