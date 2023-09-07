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
package org.neo4j.kernel.impl.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.OldestTransactionIdFactory.EMPTY_OLDEST_ID_FACTORY;

import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.OldestTransactionIdFactory;
import org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.context.TransactionVersionContext;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

class KernelTransactionImplementationHandleTest {
    private final SystemNanoClock clock = Clocks.nanoClock();

    @Test
    void isOpenForUnchangedKernelTransactionImplementation() {
        long userTransactionId = 42;

        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.isOpen()).thenReturn(true);
        when(tx.concurrentCursorContextLookup())
                .thenReturn(new CursorContextFactory(PageCacheTracer.NULL, new TestVersionContextSupplier())
                        .create("test"));
        when(tx.getTransactionSequenceNumber()).thenReturn(userTransactionId);

        KernelTransactionImplementationHandle handle =
                new KernelTransactionImplementationHandle(tx, clock, tx.concurrentCursorContextLookup());

        assertTrue(handle.isOpen());
    }

    @Test
    void isOpenForReusedKernelTransactionImplementation() {
        long initialUserTransactionId = 42;
        long nextUserTransactionId = 4242;

        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.concurrentCursorContextLookup())
                .thenReturn(new CursorContextFactory(PageCacheTracer.NULL, new TestVersionContextSupplier())
                        .create("test"));
        when(tx.isOpen()).thenReturn(true);
        when(tx.getTransactionSequenceNumber())
                .thenReturn(initialUserTransactionId)
                .thenReturn(nextUserTransactionId);

        KernelTransactionImplementationHandle handle =
                new KernelTransactionImplementationHandle(tx, clock, tx.concurrentCursorContextLookup());

        assertFalse(handle.isOpen());
    }

    @Test
    void markForTerminationCallsKernelTransactionImplementation() {
        long userTransactionId = 42;
        Status.Transaction terminationReason = Status.Transaction.Terminated;

        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.concurrentCursorContextLookup())
                .thenReturn(new CursorContextFactory(PageCacheTracer.NULL, new TestVersionContextSupplier())
                        .create("test"));
        when(tx.getTransactionSequenceNumber()).thenReturn(userTransactionId);

        KernelTransactionImplementationHandle handle =
                new KernelTransactionImplementationHandle(tx, clock, tx.concurrentCursorContextLookup());
        handle.markForTermination(terminationReason);

        verify(tx).markForTermination(userTransactionId, terminationReason);
    }

    @Test
    void markForTerminationReturnsTrueWhenSuccessful() {
        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.concurrentCursorContextLookup())
                .thenReturn(new CursorContextFactory(PageCacheTracer.NULL, new TestVersionContextSupplier())
                        .create("test"));
        when(tx.getTransactionSequenceNumber()).thenReturn(42L);
        when(tx.markForTermination(anyLong(), any())).thenReturn(true);

        KernelTransactionImplementationHandle handle =
                new KernelTransactionImplementationHandle(tx, clock, tx.concurrentCursorContextLookup());
        assertTrue(handle.markForTermination(Status.Transaction.Terminated));
    }

    @Test
    void markForTerminationReturnsFalseWhenNotSuccessful() {
        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.concurrentCursorContextLookup())
                .thenReturn(new CursorContextFactory(PageCacheTracer.NULL, new TestVersionContextSupplier())
                        .create("test"));
        when(tx.getTransactionSequenceNumber()).thenReturn(42L);
        when(tx.markForTermination(anyLong(), any())).thenReturn(false);

        KernelTransactionImplementationHandle handle =
                new KernelTransactionImplementationHandle(tx, clock, tx.concurrentCursorContextLookup());
        assertFalse(handle.markForTermination(Status.Transaction.Terminated));
    }

    @Test
    void transactionStatisticForReusedTransactionIsNotAvailable() {
        KernelTransactionImplementation tx = mock(KernelTransactionImplementation.class);
        when(tx.concurrentCursorContextLookup())
                .thenReturn(new CursorContextFactory(PageCacheTracer.NULL, new TestVersionContextSupplier())
                        .create("test"));
        when(tx.isOpen()).thenReturn(true);
        when(tx.getTransactionSequenceNumber()).thenReturn(2L).thenReturn(3L);

        KernelTransactionImplementationHandle handle =
                new KernelTransactionImplementationHandle(tx, clock, tx.concurrentCursorContextLookup());
        assertSame(TransactionExecutionStatistic.NOT_AVAILABLE, handle.transactionStatistic());
    }

    private static class TestVersionContextSupplier implements VersionContextSupplier {
        @Override
        public void init(
                TransactionIdSnapshotFactory transactionIdSnapshotFactory,
                OldestTransactionIdFactory oldestTransactionIdFactory) {}

        @Override
        public VersionContext createVersionContext() {
            var context = new TransactionVersionContext(
                    TransactionIdSnapshotFactory.EMPTY_SNAPSHOT_FACTORY, EMPTY_OLDEST_ID_FACTORY);
            context.initRead();
            return context;
        }
    }
}
