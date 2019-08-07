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
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;

public class BoltKernelTransaction extends BoltQueryExecutorImpl implements BoltTransaction
{
    private final ThreadToStatementContextBridge txBridge;
    private final KernelTransaction kernelTransaction;
    private final InternalTransaction topLevelInternalTransaction;

    public BoltKernelTransaction( QueryExecutionEngine queryExecutionEngine, ThreadToStatementContextBridge txBridge,
            TransactionalContextFactory transactionalContextFactory, KernelTransaction kernelTransaction, InternalTransaction topLevelInternalTransaction,
            Supplier<InternalTransaction> placeboTransactionFactory )
    {
        super( queryExecutionEngine, transactionalContextFactory, placeboTransactionFactory );
        this.txBridge = txBridge;
        this.kernelTransaction = kernelTransaction;
        this.topLevelInternalTransaction = topLevelInternalTransaction;
    }

    @Override
    public void bindToCurrentThread()
    {
        txBridge.bindTransactionToCurrentThread( kernelTransaction );
    }

    @Override
    public void unbindFromCurrentThread()
    {
        txBridge.unbindTransactionFromCurrentThread();
    }

    @Override
    public void commit() throws TransactionFailureException
    {
        // because of the placebo context changes and existence this needs to be temporary like this.
        if ( kernelTransaction.isOpen() )
        {
            kernelTransaction.commit();
        }
    }

    @Override
    public void rollback() throws TransactionFailureException
    {
        if ( kernelTransaction.isOpen() )
        {
            kernelTransaction.rollback();
        }
    }

    @Override
    public void markForTermination( Status reason )
    {
        kernelTransaction.markForTermination( reason );
    }

    @Override
    public void markForTermination()
    {
        kernelTransaction.markForTermination( Status.Transaction.Terminated );
    }

    @Override
    public Optional<Status> getReasonIfTerminated()
    {
        return topLevelInternalTransaction.terminationReason();
    }
}
