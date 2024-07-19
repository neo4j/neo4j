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
package org.neo4j.kernel.impl.api.transaction.monitor;

import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionVisibilityProvider;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.logging.internal.LogService;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.SystemNanoClock;

public class KernelTransactionMonitor extends TransactionMonitor<KernelTransactionMonitor.MonitoredKernelTransaction>
        implements TransactionVisibilityProvider {
    private final KernelTransactions kernelTransactions;
    private final TransactionIdStore transactionIdStore;
    private final AtomicLong oldestVisibilityBoundary = new AtomicLong(BASE_TX_ID);
    private final AtomicLong oldestVisibleClosedTransactionId = new AtomicLong(BASE_TX_ID);

    public KernelTransactionMonitor(
            KernelTransactions kernelTransactions,
            TransactionIdStore transactionIdStore,
            Config config,
            SystemNanoClock clock,
            LogService logService) {
        super(config, clock, logService);
        this.kernelTransactions = kernelTransactions;
        this.transactionIdStore = transactionIdStore;
        oldestVisibleClosedTransactionId.setRelease(
                transactionIdStore.getHighestEverClosedTransaction().id());
        oldestVisibilityBoundary.setRelease(
                transactionIdStore.getHighestEverClosedTransaction().id());
    }

    @Override
    protected void updateTransactionBoundaries() {
        // we return gap free transaction that is already closed, and if we do not have any readers it should be safe to
        // assume that no one will need
        // data before that point of history
        var oldestOpenTransactionId = transactionIdStore.getLastClosedTransactionId();
        var executingTransactions = kernelTransactions.executingTransactions();
        long oldestTxId = oldestOpenTransactionId;
        long oldestHorizon = oldestOpenTransactionId;

        for (var txHandle : executingTransactions) {
            if (txHandle.terminationMark().isEmpty()) {
                oldestTxId = Math.min(oldestTxId, txHandle.getLastClosedTxId());
                oldestHorizon = Math.min(oldestHorizon, txHandle.getTransactionHorizon());
            }
        }
        oldestVisibleClosedTransactionId.setRelease(oldestTxId);
        oldestVisibilityBoundary.setRelease(oldestHorizon);
    }

    @Override
    protected Set<MonitoredKernelTransaction> getActiveTransactions() {
        return kernelTransactions.activeTransactions().stream()
                .map(MonitoredKernelTransaction::new)
                .collect(Collectors.toSet());
    }

    @Override
    public long oldestVisibleClosedTransactionId() {
        return oldestVisibleClosedTransactionId.getAcquire();
    }

    @Override
    public long oldestObservableHorizon() {
        return oldestVisibilityBoundary.getAcquire();
    }

    @Override
    public long youngestObservableHorizon() {
        long youngestHorizon = Long.MIN_VALUE;
        for (var monitoredTx : getActiveTransactions()) {
            if (monitoredTx.terminationMark().isEmpty()) {
                youngestHorizon = Math.max(youngestHorizon, monitoredTx.kernelTransaction.getTransactionHorizon());
            }
        }
        return youngestHorizon;
    }

    static class MonitoredKernelTransaction implements MonitoredTransaction {
        private final KernelTransactionHandle kernelTransaction;

        private MonitoredKernelTransaction(KernelTransactionHandle kernelTransaction) {
            this.kernelTransaction = kernelTransaction;
        }

        @Override
        public long startTimeNanos() {
            return kernelTransaction.startTimeNanos();
        }

        @Override
        public TransactionTimeout timeout() {
            return kernelTransaction.timeout();
        }

        @Override
        public Optional<TerminationMark> terminationMark() {
            return kernelTransaction.terminationMark();
        }

        @Override
        public boolean isSchemaTransaction() {
            return kernelTransaction.isSchemaTransaction();
        }

        @Override
        public boolean markForTermination(Status reason) {
            return kernelTransaction.markForTermination(reason);
        }

        @Override
        public String getIdentifyingDescription() {
            // this is a legacy implementation, so let's use
            // 'toString' on KernelTransactionHandle which was used for years for this purpose
            return kernelTransaction.toString();
        }

        @Override
        public TransactionInitializationTrace transactionInitialisationTrace() {
            return kernelTransaction.transactionInitialisationTrace();
        }
    }
}
