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
package org.neo4j.kernel.impl.api.transaciton.monitor;

import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOut;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.time.FakeClock;

class KernelTransactionMonitorTest {
    @Test
    void shouldNotTimeoutSchemaTransactions() {
        // given
        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        when(transactionIdStore.getLastClosedTransactionId()).thenReturn(10L);
        when(transactionIdStore.getHighestEverClosedTransaction())
                .thenReturn(new TransactionId(10, 10, LatestVersions.LATEST_KERNEL_VERSION, 10, 10, 10));

        KernelTransactions kernelTransactions = mock(KernelTransactions.class);
        FakeClock clock = new FakeClock(100, MINUTES);
        KernelTransactionMonitor monitor = new KernelTransactionMonitor(
                kernelTransactions, transactionIdStore, Config.defaults(), clock, NullLogService.getInstance());

        // a 2 minutes old schema transaction which has a timeout of 1 minute
        KernelTransactionHandle oldSchemaTransaction = mock(KernelTransactionHandle.class);
        when(oldSchemaTransaction.isSchemaTransaction()).thenReturn(true);
        when(oldSchemaTransaction.startTime()).thenReturn(clock.millis() - MINUTES.toMillis(2));
        when(oldSchemaTransaction.timeout())
                .thenReturn(new TransactionTimeout(Duration.ofMinutes(1), TransactionTimedOut));
        when(kernelTransactions.activeTransactions()).thenReturn(Iterators.asSet(oldSchemaTransaction));

        // when
        monitor.run();

        // then
        verify(oldSchemaTransaction, times(1)).isSchemaTransaction();
        verify(oldSchemaTransaction, never()).markForTermination(any());
    }

    @Test
    void readOldestVisibilityBoundaries() {
        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        when(transactionIdStore.getLastClosedTransactionId()).thenReturn(1L);
        when(transactionIdStore.getHighestEverClosedTransaction())
                .thenReturn(new TransactionId(1, 1, LatestVersions.LATEST_KERNEL_VERSION, 1, 1, 1));

        KernelTransactions kernelTransactions = mock(KernelTransactions.class);
        KernelTransactionMonitor transactionMonitor = new KernelTransactionMonitor(
                kernelTransactions,
                transactionIdStore,
                Config.defaults(),
                new FakeClock(100, MINUTES),
                NullLogService.getInstance());

        assertEquals(1, transactionMonitor.oldestVisibleClosedTransactionId());
        assertEquals(1, transactionMonitor.oldestObservableHorizon());

        // no transactions - default boundaries
        transactionMonitor.run();
        assertEquals(1, transactionMonitor.oldestVisibleClosedTransactionId());
        assertEquals(1, transactionMonitor.oldestObservableHorizon());

        KernelTransactionHandle txHandle = mock(KernelTransactionHandle.class);
        when(txHandle.isSchemaTransaction()).thenReturn(true);
        when(txHandle.startTime()).thenReturn(17L);
        when(txHandle.timeout()).thenReturn(new TransactionTimeout(Duration.ofMinutes(1), TransactionTimedOut));
        when(txHandle.getTransactionHorizon()).thenReturn(5L);
        when(txHandle.getLastClosedTxId()).thenReturn(15L);
        when(kernelTransactions.executingTransactions()).thenReturn(Iterators.asSet(txHandle));

        // one active transaction - new boundaries
        when(transactionIdStore.getLastClosedTransactionId()).thenReturn(20L);

        transactionMonitor.run();
        assertEquals(15, transactionMonitor.oldestVisibleClosedTransactionId());
        assertEquals(5, transactionMonitor.oldestObservableHorizon());

        when(kernelTransactions.executingTransactions()).thenReturn(emptySet());

        // no active transaction again - new boundary based on last closed tx
        transactionMonitor.run();
        assertEquals(20, transactionMonitor.oldestVisibleClosedTransactionId());
        assertEquals(20, transactionMonitor.oldestObservableHorizon());
    }

    @Test
    void defaultVisibilityBoundaryComesFromHighestEverClosed() {
        var transactionIdStore = mock(TransactionIdStore.class);
        when(transactionIdStore.getHighestEverClosedTransaction())
                .thenReturn(new TransactionId(42, 42, LatestVersions.LATEST_KERNEL_VERSION, 42, 42, 42));

        var kernelTransactions = mock(KernelTransactions.class);
        var transactionMonitor = new KernelTransactionMonitor(
                kernelTransactions,
                transactionIdStore,
                Config.defaults(),
                new FakeClock(100, MINUTES),
                NullLogService.getInstance());

        assertEquals(42, transactionMonitor.oldestVisibleClosedTransactionId());
        assertEquals(42, transactionMonitor.oldestObservableHorizon());
    }
}
