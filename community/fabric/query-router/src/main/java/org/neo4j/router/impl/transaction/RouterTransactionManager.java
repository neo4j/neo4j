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
package org.neo4j.router.impl.transaction;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.router.transaction.RouterTransaction;
import org.neo4j.util.VisibleForTesting;

public class RouterTransactionManager extends LifecycleAdapter {

    private final Set<RouterTransactionImpl> transactions = ConcurrentHashMap.newKeySet();
    private final QueryRouterTransactionMonitor transactionMonitor;
    private final long awaitActiveTransactionDeadlineMillis;

    public RouterTransactionManager(QueryRouterTransactionMonitor transactionMonitor, Config config) {
        this.transactionMonitor = transactionMonitor;
        this.awaitActiveTransactionDeadlineMillis = config.get(GraphDatabaseSettings.shutdown_transaction_end_timeout)
                .toMillis();
    }

    public void registerTransaction(RouterTransactionImpl transaction) {
        transactions.add(transaction);
        transactionMonitor.startMonitoringTransaction(transaction);
    }

    public void unregisterTransaction(RouterTransactionImpl transaction) {
        transactions.remove(transaction);
        transactionMonitor.stopMonitoringTransaction(transaction);
    }

    public Optional<RouterTransaction> findTransactionContaining(InternalTransaction transaction) {
        return transactions.stream()
                .filter(routerTransaction -> routerTransaction.getInternalTransactions().stream()
                        .anyMatch(itx -> itx.kernelTransaction() == transaction.kernelTransaction()))
                .map(RouterTransaction.class::cast)
                .findAny();
    }

    @VisibleForTesting
    public Set<RouterTransaction> registeredTransactions() {
        return new HashSet<>(transactions);
    }

    @Override
    public void stop() {
        // Stop remote transactions after given timeout.
        // Any db specific transaction will be handled on a database level with own set of rules, checks etc
        transactions.forEach(rTx -> rTx.stopRemoteDbsAfterTimeout(awaitActiveTransactionDeadlineMillis));
    }
}
