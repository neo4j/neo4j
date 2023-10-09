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

import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOut;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOutClientConfiguration;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

public class QueryRouterTransactionMonitor
        extends TransactionMonitor<QueryRouterTransactionMonitor.QueryRouterMonitoredTransaction> {

    private final Map<RouterTransactionImpl, QueryRouterTransactionMonitor.QueryRouterMonitoredTransaction>
            transactions = new ConcurrentHashMap<>();
    private final SystemNanoClock clock;
    private final Config config;

    public QueryRouterTransactionMonitor(Config config, SystemNanoClock clock, LogService logService) {
        super(config, clock, logService);
        this.clock = clock;
        this.config = config;
    }

    public void startMonitoringTransaction(RouterTransactionImpl transaction) {
        long startTimeNanos = clock.nanos();
        TransactionTimeout timeout;
        var transactionInfo = transaction.transactionInfo();
        if (transactionInfo.txTimeout() != null) {
            timeout = new TransactionTimeout(transactionInfo.txTimeout(), TransactionTimedOutClientConfiguration);
        } else {
            timeout = new TransactionTimeout(config.get(transaction_timeout), TransactionTimedOut);
        }

        transactions.put(
                transaction,
                new QueryRouterTransactionMonitor.QueryRouterMonitoredTransaction(
                        transaction, startTimeNanos, timeout));
    }

    public void stopMonitoringTransaction(RouterTransactionImpl transaction) {
        transactions.remove(transaction);
    }

    @Override
    protected Set<QueryRouterMonitoredTransaction> getActiveTransactions() {
        return new HashSet<>(transactions.values());
    }

    static class QueryRouterMonitoredTransaction implements TransactionMonitor.MonitoredTransaction {
        private final RouterTransactionImpl routerTransaction;
        private final long startTimeNanos;
        private final TransactionTimeout timeout;

        QueryRouterMonitoredTransaction(
                RouterTransactionImpl routerTransaction, long startTimeNanos, TransactionTimeout timeout) {
            this.routerTransaction = routerTransaction;
            this.startTimeNanos = startTimeNanos;
            this.timeout = timeout;
        }

        @Override
        public long startTimeNanos() {
            return startTimeNanos;
        }

        @Override
        public TransactionTimeout timeout() {
            return timeout;
        }

        @Override
        public boolean isSchemaTransaction() {
            return routerTransaction.isSchemaTransaction();
        }

        @Override
        public Optional<TerminationMark> terminationMark() {
            return routerTransaction.getTerminationMark();
        }

        @Override
        public boolean markForTermination(Status reason) {
            return routerTransaction.markForTermination(reason);
        }

        @Override
        public String getIdentifyingDescription() {
            StringBuilder sb = new StringBuilder();
            sb.append("QueryRouterTransaction[");

            var rawAddress = routerTransaction.transactionInfo().clientInfo().clientAddress();
            var address = rawAddress == null ? "embedded" : rawAddress;
            sb.append("clientAddress=").append(address);
            var authSubject = routerTransaction.transactionInfo().loginContext().subject();
            if (authSubject != AuthSubject.ANONYMOUS && authSubject != AuthSubject.AUTH_DISABLED) {
                sb.append(",").append("username=").append(authSubject.executingUser());
            }

            sb.append("]");
            return sb.toString();
        }

        @Override
        public TransactionInitializationTrace transactionInitialisationTrace() {
            return routerTransaction.initializationTrace();
        }
    }
}
