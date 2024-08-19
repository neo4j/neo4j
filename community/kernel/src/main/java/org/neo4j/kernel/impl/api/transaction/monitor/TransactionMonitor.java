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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

/**
 * Transaction monitor that checks transactions with a configured timeout for expiration
 * and for being stuck in a terminating state (stale).
 * In case if transaction timed out it will be terminated.
 */
public abstract class TransactionMonitor<T extends TransactionMonitor.MonitoredTransaction> implements Runnable {
    private final Config config;
    private final SystemNanoClock clock;
    private final InternalLog log;

    public TransactionMonitor(Config config, SystemNanoClock clock, LogService logService) {
        this.config = config;
        this.clock = clock;
        this.log = logService.getInternalLog(TransactionMonitor.class);
    }

    @Override
    public void run() {
        checkActiveTransactions(getActiveTransactions(), clock.nanos());
        updateTransactionBoundaries();
    }

    protected void updateTransactionBoundaries() {}

    protected abstract Set<T> getActiveTransactions();

    private void checkExpiredTransaction(T transaction, long nowNanos) {
        long transactionTimeoutNanos = transaction.timeout().timeout().toNanos();
        if (transactionTimeoutNanos > 0) {
            if (isTransactionExpired(transaction, nowNanos, transactionTimeoutNanos)
                    && !transaction.isSchemaTransaction()) {
                if (transaction.markForTermination(transaction.timeout().status())) {
                    log.warn("Transaction %s timeout.", transaction.getIdentifyingDescription());
                }
            }
        }
    }

    private void checkStaleTerminatedTransaction(
            MonitoredTransaction transaction, long nowNanos, long terminationTimeoutNanos) {
        transaction.terminationMark().ifPresent(mark -> {
            if (mark.isMarkedAsStale()) {
                return;
            }

            final var nanosSinceTermination = nowNanos - mark.getTimestampNanos();
            if (nanosSinceTermination >= terminationTimeoutNanos) {
                log.warn(
                        "Transaction %s has been marked for termination for %d seconds; it may have been leaked. %s",
                        transaction.getIdentifyingDescription(),
                        TimeUnit.NANOSECONDS.toSeconds(nanosSinceTermination),
                        buildTraceOrHelpMessage(transaction.transactionInitialisationTrace()));
                mark.markAsStale();
            }
        });
    }

    private static String buildTraceOrHelpMessage(TransactionInitializationTrace initializationTrace) {
        final String trace = initializationTrace.getTrace();
        if (StringUtils.isEmpty(trace)) {
            return "For a transaction initialization trace, set '%s=ALL'."
                    .formatted(GraphDatabaseSettings.transaction_tracing_level.name());
        } else {
            return "Initialization trace:%n%s".formatted(trace);
        }
    }

    private void checkActiveTransactions(Set<T> activeTransactions, long nowNanos) {
        long terminationTimeoutNanos = config.get(GraphDatabaseInternalSettings.transaction_termination_timeout)
                .toNanos();

        for (T activeTransaction : activeTransactions) {
            checkExpiredTransaction(activeTransaction, nowNanos);

            if (terminationTimeoutNanos > 0) {
                checkStaleTerminatedTransaction(activeTransaction, nowNanos, terminationTimeoutNanos);
            }
        }
    }

    private static boolean isTransactionExpired(
            MonitoredTransaction activeTransaction, long nowNanos, long transactionTimeoutNanos) {
        return nowNanos - activeTransaction.startTimeNanos() > transactionTimeoutNanos;
    }

    public interface MonitoredTransaction {
        long startTimeNanos();

        TransactionTimeout timeout();

        boolean isSchemaTransaction();

        Optional<TerminationMark> terminationMark();

        /**
         * Mark the underlying transaction for termination.
         *
         * @param reason the reason for termination.
         * @return {@code true} if the underlying transaction was marked for termination, {@code false} otherwise
         * (when this handle represents an old transaction that has been closed).
         */
        boolean markForTermination(Status reason);

        /**
         * A meaningful description used in log messages related to this transaction.
         * <p>
         * In other words, this is meant to be a user-facing 'toString' containing
         * information that can help the reader of the log to identify the transaction
         * that timed out.
         */
        String getIdentifyingDescription();

        TransactionInitializationTrace transactionInitialisationTrace();
    }
}
