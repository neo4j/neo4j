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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.TransactionHook;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This is the Neo4j Kernel, an implementation of the Kernel API which is an internal component used by Cypher and the
 * Core API (the API under org.neo4j.graphdb).
 *
 * <h1>Structure</h1>
 *
 * The Kernel lets you start transactions. The transactions allow you to create "statements", which, in turn, operate
 * against the database. Statements and transactions are separate concepts due to isolation requirements. A single
 * cypher query will normally use one statement, and there can be multiple statements executed in one transaction.
 *
 * Please refer to the {@link KernelTransaction} javadoc for details.
 *
 * The architecture of the kernel is based around a layered design, where one layer performs some task, and potentially
 * delegates down to a lower layer. For instance, writing to the database will pass through
 * {@link LockingStatementOperations}, which will grab locks and delegate to {@link StateHandlingStatementOperations}
 * which will store the change in the transaction state, to be applied later if the transaction is committed.
 *
 * A read will, similarly, pass through {@link LockingStatementOperations}. It then reaches
 * {@link StateHandlingStatementOperations}, which includes any changes that exist in the current transaction, and then
 * finally {@link org.neo4j.kernel.impl.api.store.StoreReadLayer} will read the current committed state from
 * the stores or caches.
 *
 * <h1>Refactoring</h1>
 *
 * There are several sources of pain around the current state, which we hope to refactor away down the line.
 *
 * One pain is transaction state, which is maintained in the {@link org.neo4j.kernel.impl.api.state.TxState} class.
 * This class is huge and complicated, as it has been used as a gathering point for consolidating all transaction state
 * management in one place. This is now done, and the TxState class should now be refactored to be easier to understand.
 *
 * Please expand and update this as you learn things or find errors in the text above.
 */
public class Kernel extends LifecycleAdapter implements KernelAPI
{
    private final KernelTransactions transactions;
    private final TransactionHooks hooks;
    private final KernelHealth health;
    private final TransactionMonitor transactionMonitor;

    public Kernel( KernelTransactions transactionFactory,
                   TransactionHooks hooks, KernelHealth health, TransactionMonitor transactionMonitor )
    {
        this.transactions = transactionFactory;
        this.hooks = hooks;
        this.health = health;
        this.transactionMonitor = transactionMonitor;
    }

    @Override
    public KernelTransaction newTransaction() throws TransactionFailureException
    {
        health.assertHealthy( TransactionFailureException.class );
        KernelTransaction transaction = transactions.newInstance();
        transactionMonitor.transactionStarted();
        return transaction;
    }

    @Override
    public void registerTransactionHook( TransactionHook hook )
    {
        hooks.register( hook );
    }

    @Override
    public void unregisterTransactionHook( TransactionHook hook )
    {
        hooks.unregister( hook );
    }

    @Override
    public void stop() throws Throwable
    {
        transactions.disposeAll();
    }
}
