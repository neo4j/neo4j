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
package org.neo4j.bolt.dbapi.impl;

import java.util.Optional;

import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;

public class PeriodicBoltKernelTransaction extends BoltQueryExecutorImpl implements BoltTransaction
{
    private final ThreadToStatementContextBridge txBridge;
    private final DatabaseId databaseId;
    private volatile KernelTransaction transaction;

    public PeriodicBoltKernelTransaction( QueryExecutionEngine queryExecutionEngine, ThreadToStatementContextBridge txBridge,
            TransactionalContextFactory transactionalContextFactory, KernelTransaction kernelTransaction, InternalTransaction internalTransaction )
    {
        super( queryExecutionEngine, transactionalContextFactory, internalTransaction );
        this.txBridge = txBridge;
        this.databaseId = kernelTransaction.getDatabaseId();
    }

    @Override
    public void bindToCurrentThread()
    {
        if ( transaction != null )
        {
            txBridge.bindTransactionToCurrentThread( transaction );
        }
    }

    @Override
    public void unbindFromCurrentThread()
    {
        transaction = txBridge.getKernelTransactionBoundToThisThread( true , databaseId );
        txBridge.unbindTransactionFromCurrentThread();
    }

    @Override
    public void commit() throws TransactionFailureException
    {
        var transaction = txBridge.getKernelTransactionBoundToThisThread( true, databaseId );
        if ( transaction != null )
        {
            transaction.commit();
        }
    }

    @Override
    public void rollback() throws TransactionFailureException
    {
        var transaction = txBridge.getKernelTransactionBoundToThisThread( true, databaseId );
        if ( transaction != null )
        {
            transaction.rollback();
        }
    }

    @Override
    public void markForTermination( Status reason )
    {
        var transaction = txBridge.getKernelTransactionBoundToThisThread( true, databaseId );
        if ( transaction != null )
        {
            transaction.markForTermination( reason );
        }
    }

    @Override
    public void markForTermination()
    {
        var transaction = txBridge.getKernelTransactionBoundToThisThread( true, databaseId );
        if ( transaction != null )
        {
            transaction.markForTermination( Status.Transaction.Terminated );
        }
    }

    @Override
    public Optional<Status> getReasonIfTerminated()
    {
        var transaction = txBridge.getKernelTransactionBoundToThisThread( true, databaseId );
        return Optional.ofNullable( transaction ).flatMap( Transaction::getReasonIfTerminated );
    }
}
