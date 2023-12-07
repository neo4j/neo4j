/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.query;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;
import org.neo4j.values.ElementIdMapper;

public interface TransactionalContext {
    ExecutingQuery executingQuery();

    KernelTransaction kernelTransaction();

    InternalTransaction transaction();

    boolean isTopLevelTx();

    /**
     * This should be called once the query is finished, either successfully or not.
     * Should be called from the same thread the query was executing in.
     *
     * This does not close the underlying transaction.
     */
    void close();

    /**
     * Close this context and propagate the commit call to the underlying transaction.
     */
    void commit();

    /**
     * Close and rollback this context. This will propagate the rollback call to the underlying transaction.
     */
    void rollback();

    /**
     * This is used to terminate a currently running query. Can be called from any thread.
     * Will mark the current transaction for termination if it is still open.
     */
    void terminate();

    /**
     * Commit the underlying {@link KernelTransaction} and open a new {@link KernelTransaction}.
     * The new {@link KernelTransaction} will be integrated both in the same {@link InternalTransaction}
     * and in this context.
     *
     * @return id of the committed transaction
     */
    long commitAndRestartTx();

    /**
     * Open a new {@link InternalTransaction} with a new {@link KernelTransaction} and a new {@link Statement}.
     * Return a new {@link TransactionalContext} that is bound to the new transaction and statement.
     * The new transaction is called an inner transaction that is connected to the transaction of this context, which we will call the outer transaction.
     * The connection is as follows:
     * <ul>
     *   <li>An outer transaction cannot commit if it is connected to an open inner transaction.</li>
     *   <li>A termination or rollback of an outer transaction propagates to any open inner transactions.</li>
     *   <li>The outer transaction and all connected inner transactions are connected to the same {@link ExecutingQuery}.</li>
     * </ul>
     * <p/>
     * This context is still open and can continue to be used.
     *
     * @return the new context.
     */
    TransactionalContext contextWithNewTransaction();

    /**
     * Make sure this context is open. If it is currently closed, acquire the {@link Statement} from the already open transaction,
     * otherwise do nothing.
     *
     * @throws TransactionTerminatedException if the context is closed or if the transaction is marked for termination.
     * @return the same instance.
     */
    TransactionalContext getOrBeginNewIfClosed();

    boolean isOpen();

    GraphDatabaseQueryService graph();

    NamedDatabaseId databaseId();

    Statement statement();

    SecurityContext securityContext();

    StatisticProvider kernelStatisticProvider();

    KernelTransaction.Revertable restrictCurrentTransaction(SecurityContext context);

    ResourceTracker resourceTracker();

    ElementIdMapper elementIdMapper();

    QueryExecutionConfiguration queryExecutingConfiguration();

    boolean targetsComposite();
}
