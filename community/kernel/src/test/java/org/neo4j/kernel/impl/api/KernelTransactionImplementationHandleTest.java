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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

class KernelTransactionImplementationHandleTest {
    private final SystemNanoClock clock = Clocks.nanoClock();

    @Test
    void returnsCorrectLastTransactionTimestampWhenStarted() {
        long lastCommittedTxTimestamp = 42;

        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.lastTransactionTimestampWhenStarted()).thenReturn(lastCommittedTxTimestamp);
        when(tx.isOpen()).thenReturn(true);

        KernelTransactionImplementationHandle handle = new KernelTransactionImplementationHandle(tx, clock);

        assertEquals(lastCommittedTxTimestamp, handle.lastTransactionTimestampWhenStarted());
    }

    @Test
    void returnsCorrectLastTransactionTimestampWhenStartedForClosedTx() {
        long lastCommittedTxTimestamp = 4242;

        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.lastTransactionTimestampWhenStarted()).thenReturn(lastCommittedTxTimestamp);
        when(tx.isOpen()).thenReturn(false);

        KernelTransactionImplementationHandle handle = new KernelTransactionImplementationHandle(tx, clock);

        assertEquals(lastCommittedTxTimestamp, handle.lastTransactionTimestampWhenStarted());
    }

    @Test
    void isOpenForUnchangedKernelTransactionImplementation() {
        long userTransactionId = 42;

        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.isOpen()).thenReturn(true);
        when(tx.getUserTransactionId()).thenReturn(userTransactionId);

        KernelTransactionImplementationHandle handle = new KernelTransactionImplementationHandle(tx, clock);

        assertTrue(handle.isOpen());
    }

    @Test
    void isOpenForReusedKernelTransactionImplementation() {
        long initialUserTransactionId = 42;
        long nextUserTransactionId = 4242;

        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.isOpen()).thenReturn(true);
        when(tx.getUserTransactionId()).thenReturn(initialUserTransactionId).thenReturn(nextUserTransactionId);

        KernelTransactionImplementationHandle handle = new KernelTransactionImplementationHandle(tx, clock);

        assertFalse(handle.isOpen());
    }

    @Test
    void markForTerminationCallsKernelTransactionImplementation() {
        long userTransactionId = 42;
        Status.Transaction terminationReason = Status.Transaction.Terminated;

        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.getUserTransactionId()).thenReturn(userTransactionId);

        KernelTransactionImplementationHandle handle = new KernelTransactionImplementationHandle(tx, clock);
        handle.markForTermination(terminationReason);

        verify(tx).markForTermination(userTransactionId, terminationReason);
    }

    @Test
    void markForTerminationReturnsTrueWhenSuccessful() {
        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.getUserTransactionId()).thenReturn(42L);
        when(tx.markForTermination(anyLong(), any())).thenReturn(true);

        KernelTransactionImplementationHandle handle = new KernelTransactionImplementationHandle(tx, clock);
        assertTrue(handle.markForTermination(Status.Transaction.Terminated));
    }

    @Test
    void markForTerminationReturnsFalseWhenNotSuccessful() {
        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.getUserTransactionId()).thenReturn(42L);
        when(tx.markForTermination(anyLong(), any())).thenReturn(false);

        KernelTransactionImplementationHandle handle = new KernelTransactionImplementationHandle(tx, clock);
        assertFalse(handle.markForTermination(Status.Transaction.Terminated));
    }

    @Test
    void transactionStatisticForReusedTransactionIsNotAvailable() {
        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.isOpen()).thenReturn(true);
        when(tx.getUserTransactionId()).thenReturn(2L).thenReturn(3L);

        KernelTransactionImplementationHandle handle = new KernelTransactionImplementationHandle(tx, clock);
        assertSame(TransactionExecutionStatistic.NOT_AVAILABLE, handle.transactionStatistic());
    }
}
