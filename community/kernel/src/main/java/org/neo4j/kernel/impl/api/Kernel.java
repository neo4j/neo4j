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
package org.neo4j.kernel.impl.api;

import org.neo4j.helpers.Factory;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.TransactionHook;
import org.neo4j.kernel.api.TxState;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.heuristics.StatisticsData;
import org.neo4j.kernel.impl.api.statistics.StatisticsService;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This is the beginnings of an implementation of the Kernel API, which is meant to be an internal API for
 * consumption by both the Core API, Cypher, and any other components that want to interface with the
 * underlying database.
 *
 * This is currently in an intermediate phase, with many features still unavailable unless the Core API is also
 * present. We are in the process of moving Core API features into the kernel.
 *
 * <h1>Structure</h1>
 *
 * The Kernel itself has a simple API - it lets you start transactions. The transactions, in turn, allow you to
 * create statements, which, in turn, operate against the database. The reason for the separation between statements
 * and transactions is database isolation. Please refer to the {@link KernelTransaction} javadoc for details.
 *
 * The architecture of the kernel is based around a layered design, where one layer performs some task, and potentially
 * delegates down to a lower layer. For instance, writing to the database will pass through
 * {@link LockingStatementOperations}, which will grab locks and delegate to {@link StateHandlingStatementOperations}
 * which will store the change in the transaction state, to be applied later if the transaction is committed.
 *
 * A read will, similarly, pass through {@link LockingStatementOperations}, which should (but does not currently) grab
 * read locks. It then reaches {@link StateHandlingStatementOperations}, which includes any changes that exist in the
 * current transaction, and then finally {@link org.neo4j.kernel.impl.api.store.DiskLayer} will read the current committed state from
 * the stores or caches.
 *
 * <h1>A story of JTA</h1>
 *
 * We have, for a long time, supported full X.Open two-phase commits, which is handled by our TxManager implementation
 * of the JTA interfaces. However, two phase commit is slow and complex. Ideally we don't want every day use of neo4j
 * to require JTA anymore, but rather have it be a bonus feature that can be enabled when the user wants two-phase
 * commit support. As such, we are working to keep the Kernel compatible with a JTA system built on top of it, but
 * at the same time it should remain independent and runnable without a transaction manager.
 *
 * The heart of this work is in the relationship between {@link KernelTransaction} and
 * {@link org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState}. The latter should become a wrapper around
 * KernelTransactions, exposing them as JTA-capable transactions. The Write transaction should be hidden from the outside,
 * an implementation detail living inside the kernel.
 *
 * <h1>Refactoring</h1>
 *
 * There are several sources of pain around the current state, which we hope to refactor away down the line.
 *
 * One pain is transaction state, where lots of legacy code still rules supreme. Please refer to {@link TxState}
 * for details about the work in this area.
 *
 * Cache invalidation is similarly problematic, where cache invalidation really should be done when changes are applied
 * to the store, through the logical log. However, this is mostly not the case, cache invalidation is done as we work
 * through the Core API. Only in HA mode is cache invalidation done through log application, and then only through
 * evicting whole entities from the cache whenever they change, leading to large performance hits on writes. This area
 * is still open for investigation, but an approach where the logical log simply tells a store write API to apply some
 * change, and the implementation of that API is responsible for keeping caches in sync.
 *
 * Please expand and update this as you learn things or find errors in the text above.
 */
public class Kernel extends LifecycleAdapter implements KernelAPI
{
    private final StatisticsService statisticsService;
    private final Factory<KernelTransaction> transactionFactory;
    private final TransactionHooks hooks;
    private final KernelHealth health;

    public Kernel( StatisticsService statisticsService, Factory<KernelTransaction> transactionFactory,
            TransactionHooks hooks, KernelHealth health )
    {
        this.transactionFactory = transactionFactory;
        this.statisticsService = statisticsService;
        this.hooks = hooks;
        this.health = health;
    }

    @Override
    public KernelTransaction newTransaction() throws TransactionFailureException
    {
        health.assertHealthy( TransactionFailureException.class );
        return transactionFactory.newInstance();
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
    public StatisticsData heuristics()
    {
        return statisticsService.statistics();
    }
}
