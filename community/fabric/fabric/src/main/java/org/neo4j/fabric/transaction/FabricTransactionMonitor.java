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
package org.neo4j.fabric.transaction;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOut;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOutClientConfiguration;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.configuration.Config;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.transaction.monitor.TransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

public class FabricTransactionMonitor extends TransactionMonitor<FabricTransactionMonitor.FabricMonitoredTransaction> {
    private final Map<FabricTransactionImpl, FabricMonitoredTransaction> transactions = new ConcurrentHashMap<>();
    private final SystemNanoClock clock;
    private final FabricConfig fabricConfig;

    public FabricTransactionMonitor(
            Config config, SystemNanoClock clock, LogService logService, FabricConfig fabricConfig) {
        super(config, clock, logService);

        this.clock = clock;
        this.fabricConfig = fabricConfig;
    }

    public void startMonitoringTransaction(FabricTransactionImpl transaction, FabricTransactionInfo transactionInfo) {
        long startTimeNanos = clock.nanos();
        TransactionTimeout timeout;
        if (transactionInfo.getTxTimeout() != null) {
            timeout = new TransactionTimeout(transactionInfo.getTxTimeout(), TransactionTimedOutClientConfiguration);
        } else {
            timeout = new TransactionTimeout(fabricConfig.getTransactionTimeout(), TransactionTimedOut);
        }

        transactions.put(transaction, new FabricMonitoredTransaction(transaction, startTimeNanos, timeout));
    }

    public void stopMonitoringTransaction(FabricTransactionImpl transaction) {
        transactions.remove(transaction);
    }

    @Override
    protected Set<FabricMonitoredTransaction> getActiveTransactions() {
        return new HashSet<>(transactions.values());
    }

    static class FabricMonitoredTransaction implements MonitoredTransaction {
        private final FabricTransactionImpl fabricTransaction;
        private final long startTimeNanos;
        private final TransactionTimeout timeout;

        private FabricMonitoredTransaction(
                FabricTransactionImpl fabricTransaction, long startTimeNanos, TransactionTimeout timeout) {
            this.fabricTransaction = fabricTransaction;
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
            return fabricTransaction.isSchemaTransaction();
        }

        @Override
        public Optional<TerminationMark> terminationMark() {
            return fabricTransaction.getTerminationMark();
        }

        @Override
        public boolean markForTermination(Status reason) {
            return fabricTransaction.markForTermination(reason);
        }

        @Override
        public String getIdentifyingDescription() {
            StringBuilder sb = new StringBuilder();
            sb.append("QueryRouterTransaction[");

            var rawAddress = fabricTransaction
                    .getTransactionInfo()
                    .getClientConnectionInfo()
                    .clientAddress();
            var address = rawAddress == null ? "embedded" : rawAddress;
            sb.append("clientAddress=").append(address);
            var authSubject =
                    fabricTransaction.getTransactionInfo().getLoginContext().subject();
            if (authSubject != AuthSubject.ANONYMOUS && authSubject != AuthSubject.AUTH_DISABLED) {
                sb.append(",").append("username=").append(authSubject.executingUser());
            }

            sb.append("]");
            return sb.toString();
        }

        @Override
        public TransactionInitializationTrace transactionInitialisationTrace() {
            return fabricTransaction.getInitializationTrace();
        }
    }
}
