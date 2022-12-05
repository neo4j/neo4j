/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.transaction_termination_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_tracing_level;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.TransactionTracingLevel;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.trace.TraceProvider;
import org.neo4j.kernel.impl.api.transaction.trace.TraceProviderFactory;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

class KernelTransactionTimeoutMonitorTest {
    private static final long EXPECTED_USER_TRANSACTION_ID = 2;
    private KernelTransactions kernelTransactions;
    private FakeClock fakeClock;
    private AssertableLogProvider logProvider;
    private LogService logService;

    @BeforeEach
    void setUp() {
        kernelTransactions = mock(KernelTransactions.class);
        fakeClock = Clocks.fakeClock();
        logProvider = new AssertableLogProvider();
        logService = new SimpleLogService(logProvider, logProvider);
    }

    @Test
    void terminateExpiredTransactions() {
        Set<KernelTransactionHandle> transactions = new HashSet<>();
        KernelTransactionImplementation tx1 = prepareTxMock(3, 1, 3);
        KernelTransactionImplementation tx2 = prepareTxMock(4, 1, 8);
        KernelTransactionImplementationHandle handle1 = new KernelTransactionImplementationHandle(tx1, fakeClock);
        KernelTransactionImplementationHandle handle2 = new KernelTransactionImplementationHandle(tx2, fakeClock);
        transactions.add(handle1);
        transactions.add(handle2);

        when(kernelTransactions.activeTransactions()).thenReturn(transactions);

        KernelTransactionMonitor transactionMonitor = buildTransactionMonitor();

        fakeClock.forward(3, TimeUnit.MILLISECONDS);
        transactionMonitor.run();

        verify(tx1, never()).markForTermination(Status.Transaction.TransactionTimedOut);
        verify(tx2, never()).markForTermination(Status.Transaction.TransactionTimedOut);
        assertThat(logProvider).doesNotContainMessage("timeout");

        fakeClock.forward(2, TimeUnit.MILLISECONDS);
        transactionMonitor.run();

        verify(tx1).markForTermination(EXPECTED_USER_TRANSACTION_ID, Status.Transaction.TransactionTimedOut);
        verify(tx2, never()).markForTermination(Status.Transaction.TransactionTimedOut);
        assertThat(logProvider).containsMessages("timeout");

        logProvider.clear();
        fakeClock.forward(10, TimeUnit.MILLISECONDS);
        transactionMonitor.run();

        verify(tx2).markForTermination(EXPECTED_USER_TRANSACTION_ID, Status.Transaction.TransactionTimedOut);
        assertThat(logProvider).containsMessages("timeout");
    }

    @Test
    void skipTransactionWithoutTimeout() {
        Set<KernelTransactionHandle> transactions = new HashSet<>();
        KernelTransactionImplementation tx1 = prepareTxMock(7, 3, 0);
        KernelTransactionImplementation tx2 = prepareTxMock(8, 4, 0);
        KernelTransactionImplementationHandle handle1 = new KernelTransactionImplementationHandle(tx1, fakeClock);
        KernelTransactionImplementationHandle handle2 = new KernelTransactionImplementationHandle(tx2, fakeClock);
        transactions.add(handle1);
        transactions.add(handle2);

        when(kernelTransactions.activeTransactions()).thenReturn(transactions);

        KernelTransactionMonitor transactionMonitor = buildTransactionMonitor();

        fakeClock.forward(300, TimeUnit.MILLISECONDS);
        transactionMonitor.run();

        verify(tx1, never()).markForTermination(Status.Transaction.TransactionTimedOut);
        verify(tx2, never()).markForTermination(Status.Transaction.TransactionTimedOut);
        assertThat(logProvider).doesNotContainMessage("timeout");
    }

    @ParameterizedTest
    @EnumSource(
            value = TransactionTracingLevel.class,
            names = {"DISABLED", "ALL"})
    void logStaleTransactions(TransactionTracingLevel traceLevel) {
        final var terminationTimeout = Duration.ofSeconds(1);
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.transaction_tracing_level, traceLevel)
                .set(transaction_termination_timeout, terminationTimeout)
                .build();

        TraceProvider traceProvider = TraceProviderFactory.getTraceProvider(config);

        Set<KernelTransactionHandle> transactions = new HashSet<>();
        KernelTransactionImplementation tx1 = prepareTxMock(3, 1, 0);
        when(tx1.getTerminationMark())
                .thenReturn(Optional.of(
                        new TerminationMark(Status.Transaction.TransactionMarkedAsFailed, fakeClock.nanos())));
        var initializationTrace = traceProvider.getTraceInfo();
        when(tx1.getInitializationTrace()).thenReturn(initializationTrace);

        KernelTransactionImplementationHandle handle1 = new KernelTransactionImplementationHandle(tx1, fakeClock);
        transactions.add(handle1);

        when(kernelTransactions.activeTransactions()).thenReturn(transactions);

        KernelTransactionMonitor transactionMonitor = buildTransactionMonitor(config);

        // Should not log before time limit
        fakeClock.forward(terminationTimeout.toNanos() - 1, TimeUnit.NANOSECONDS);
        transactionMonitor.run();
        assertThat(tx1.getTerminationMark())
                .hasValueSatisfying(mark -> assertThat(mark.isMarkedAsStale()).isFalse());
        assertThat(logProvider)
                .as("should not log before time limit")
                .doesNotContainMessage("has been marked for termination for");

        // Should log at time limit...
        fakeClock.forward(1, TimeUnit.NANOSECONDS);
        transactionMonitor.run();
        assertThat(tx1.getTerminationMark())
                .hasValueSatisfying(mark -> assertThat(mark.isMarkedAsStale()).isTrue());

        // ...and the log message should contain what we expect:
        var expectedTraceMessage = traceLevel == TransactionTracingLevel.DISABLED
                ? "For a transaction initialization trace, set '%s=ALL'.".formatted(transaction_tracing_level.name())
                : "Initialization trace:\n" + initializationTrace.getTrace();
        var expectedLogMessage =
                "Transaction %s has been marked for termination for %d seconds; it may have been leaked. %s"
                        .formatted(handle1.toString(), terminationTimeout.toSeconds(), expectedTraceMessage);

        assertThat(logProvider)
                .as("should log expected message at time limit")
                .containsMessagesOnce(expectedLogMessage);

        // Should only log once
        fakeClock.forward(terminationTimeout);
        transactionMonitor.run();
        assertThat(logProvider).as("should only log once").containsMessagesOnce("has been marked for termination for");

        logProvider.clear();
    }

    @Test
    void doNotLogStaleTransactionsIfDisabled() {
        Config config = Config.newBuilder()
                .set(transaction_termination_timeout, Duration.ZERO)
                .build();

        Set<KernelTransactionHandle> transactions = new HashSet<>();
        KernelTransactionImplementation tx1 = prepareTxMock(3, 1, 0);
        when(tx1.getTerminationMark())
                .thenReturn(Optional.of(
                        new TerminationMark(Status.Transaction.TransactionMarkedAsFailed, fakeClock.nanos())));
        when(tx1.getInitializationTrace()).thenReturn(TransactionInitializationTrace.NONE);

        KernelTransactionImplementationHandle handle1 = new KernelTransactionImplementationHandle(tx1, fakeClock);
        transactions.add(handle1);

        when(kernelTransactions.activeTransactions()).thenReturn(transactions);

        KernelTransactionMonitor transactionMonitor = buildTransactionMonitor(config);

        // Should not log
        fakeClock.forward(1, TimeUnit.SECONDS);
        transactionMonitor.run();
        assertThat(tx1.getTerminationMark())
                .hasValueSatisfying(mark -> assertThat(mark.isMarkedAsStale()).isFalse());
        assertThat(logProvider)
                .as("should not log before time limit")
                .doesNotContainMessage("has been marked for termination for");

        logProvider.clear();
    }

    private KernelTransactionMonitor buildTransactionMonitor() {
        return buildTransactionMonitor(Config.defaults());
    }

    private KernelTransactionMonitor buildTransactionMonitor(Config config) {
        return new KernelTransactionMonitor(kernelTransactions, config, fakeClock, logService);
    }

    private static KernelTransactionImplementation prepareTxMock(long userTxId, long startMillis, long timeoutMillis) {
        KernelTransactionImplementation transaction = mock(KernelTransactionImplementation.class);
        when(transaction.startTime()).thenReturn(startMillis);
        when(transaction.getTransactionSequenceNumber()).thenReturn(userTxId);
        when(transaction.getTransactionSequenceNumber()).thenReturn(EXPECTED_USER_TRANSACTION_ID);
        when(transaction.timeout()).thenReturn(timeoutMillis);
        when(transaction.markForTermination(EXPECTED_USER_TRANSACTION_ID, Status.Transaction.TransactionTimedOut))
                .thenReturn(true);
        return transaction;
    }
}
