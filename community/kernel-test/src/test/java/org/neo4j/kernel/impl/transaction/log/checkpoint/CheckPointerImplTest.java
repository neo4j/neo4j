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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.AppendBatchInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl.ForceOperation;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Panic;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.LatestVersions;
import org.neo4j.time.Clocks;
import org.neo4j.util.concurrent.BinaryLatch;

class CheckPointerImplTest {
    private static final SimpleTriggerInfo INFO = new SimpleTriggerInfo("Test");
    private static final long TRANSACTION_APPEND_INDEX = 8;

    private final MetadataProvider metadataProvider = mock(MetadataProvider.class);
    private final CheckPointThreshold threshold = mock(CheckPointThreshold.class);
    private final ForceOperation forceOperation = mock(ForceOperation.class);
    private final LogPruning logPruning = mock(LogPruning.class);
    private final CheckpointAppender appender = mock(CheckpointAppender.class);
    private final Panic panic = mock(DatabaseHealth.class);
    private final DatabaseTracer tracer = mock(DatabaseTracer.class, RETURNS_MOCKS);

    private final long initialAppendIndex = 2L;
    private final long transactionId = 42L;
    private final LogPosition logPosition = new LogPosition(16L, 233L);
    private final Clock clock = Clocks.fakeClock();
    private final StoreId storeId = new StoreId(1, 1, "engine-1", "format-1", 1, 1);

    @Test
    void shouldNotFlushIfItIsNotNeeded() throws Throwable {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when(threshold.isCheckPointingNeeded(anyLong(), any(LogPosition.class), any(TriggerInfo.class)))
                .thenReturn(false);
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.checkPointIfNeeded(INFO);

        // Then
        assertEquals(-1, txId);
        verifyNoInteractions(forceOperation);
        verifyNoInteractions(tracer);
        verifyNoInteractions(appender);
    }

    @Test
    void shouldFlushIfItIsNeeded() throws Throwable {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when(threshold.isCheckPointingNeeded(anyLong(), any(LogPosition.class), eq(INFO)))
                .thenReturn(true, false);
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.checkPointIfNeeded(INFO);

        // Then
        assertEquals(transactionId, txId);
        verify(forceOperation).flushAndForce(any(), any());
        verify(panic, times(2)).assertNoPanic(IOException.class);
        verify(appender)
                .checkPoint(
                        any(LogCheckPointEvent.class),
                        any(TransactionId.class),
                        anyLong(),
                        any(KernelVersion.class),
                        eq(logPosition),
                        eq(logPosition),
                        any(Instant.class),
                        any(String.class));
        verify(threshold).initialize(initialAppendIndex, logPosition);
        verify(threshold).isCheckPointingNeeded(TRANSACTION_APPEND_INDEX, logPosition, INFO);
        verify(threshold).checkPointHappened(TRANSACTION_APPEND_INDEX, logPosition);
        verify(logPruning).pruneLogs(logPosition.getLogVersion());
        verify(tracer).beginCheckPoint();
        verifyNoMoreInteractions(forceOperation, panic, appender, threshold, tracer);
    }

    @Test
    void shouldForceCheckPointAlways() throws Throwable {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when(threshold.isCheckPointingNeeded(anyLong(), any(LogPosition.class), eq(INFO)))
                .thenReturn(false);
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.forceCheckPoint(INFO);

        // Then
        assertEquals(transactionId, txId);
        verify(forceOperation).flushAndForce(any(), any());
        verify(panic, times(2)).assertNoPanic(IOException.class);
        verify(appender)
                .checkPoint(
                        any(LogCheckPointEvent.class),
                        any(TransactionId.class),
                        anyLong(),
                        any(KernelVersion.class),
                        eq(logPosition),
                        eq(logPosition),
                        any(Instant.class),
                        any(String.class));
        verify(threshold).initialize(initialAppendIndex, logPosition);
        verify(threshold).checkPointHappened(TRANSACTION_APPEND_INDEX, logPosition);
        verify(threshold, never()).isCheckPointingNeeded(TRANSACTION_APPEND_INDEX, logPosition, INFO);
        verify(logPruning).pruneLogs(logPosition.getLogVersion());
        verifyNoMoreInteractions(forceOperation, panic, appender, threshold);
    }

    @Test
    void shouldCheckPointAlwaysWhenThereIsNoRunningCheckPoint() throws Throwable {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when(threshold.isCheckPointingNeeded(anyLong(), any(LogPosition.class), eq(INFO)))
                .thenReturn(false);
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.tryCheckPoint(INFO);

        // Then
        assertEquals(transactionId, txId);
        verify(forceOperation).flushAndForce(any(), any());
        verify(panic, times(2)).assertNoPanic(IOException.class);
        verify(appender)
                .checkPoint(
                        any(LogCheckPointEvent.class),
                        any(TransactionId.class),
                        anyLong(),
                        any(KernelVersion.class),
                        eq(logPosition),
                        eq(logPosition),
                        any(Instant.class),
                        any(String.class));
        verify(threshold).initialize(initialAppendIndex, logPosition);
        verify(threshold).checkPointHappened(TRANSACTION_APPEND_INDEX, logPosition);
        verify(threshold, never()).isCheckPointingNeeded(TRANSACTION_APPEND_INDEX, logPosition, INFO);
        verify(logPruning).pruneLogs(logPosition.getLogVersion());
        verifyNoMoreInteractions(forceOperation, panic, appender, threshold);
    }

    @Test
    void shouldCheckPointNoWaitAlwaysWhenThereIsNoRunningCheckPoint() throws Throwable {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when(threshold.isCheckPointingNeeded(anyLong(), any(LogPosition.class), eq(INFO)))
                .thenReturn(false);
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.tryCheckPointNoWait(INFO);

        // Then
        assertEquals(transactionId, txId);
        verify(forceOperation).flushAndForce(any(), any());
        verify(panic, times(2)).assertNoPanic(IOException.class);
        verify(appender)
                .checkPoint(
                        any(LogCheckPointEvent.class),
                        any(TransactionId.class),
                        anyLong(),
                        any(KernelVersion.class),
                        eq(logPosition),
                        eq(logPosition),
                        any(Instant.class),
                        any(String.class));
        verify(threshold).initialize(initialAppendIndex, logPosition);
        verify(threshold).checkPointHappened(TRANSACTION_APPEND_INDEX, logPosition);
        verify(threshold, never()).isCheckPointingNeeded(TRANSACTION_APPEND_INDEX, logPosition, INFO);
        verify(logPruning).pruneLogs(logPosition.getLogVersion());
        verifyNoMoreInteractions(forceOperation, panic, appender, threshold);
    }

    @Test
    void forceCheckPointShouldWaitTheCurrentCheckPointingToCompleteBeforeRunning() throws Throwable {
        // Given
        var lock = new CheckpointCountingLock();

        final CheckPointerImpl checkPointing = checkPointer(mutex(lock));
        mockTxIdStore();

        final CountDownLatch startSignal = new CountDownLatch(2);
        final CountDownLatch completed = new CountDownLatch(2);

        checkPointing.start();

        Thread checkPointerThread = new CheckPointerThread(checkPointing, startSignal, completed);

        Thread forceCheckPointThread = new Thread(() -> {
            try {
                startSignal.countDown();
                startSignal.await();
                checkPointing.forceCheckPoint(INFO);

                completed.countDown();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });

        // when
        checkPointerThread.start();
        forceCheckPointThread.start();

        completed.await();

        assertThat(lock.getUnlockCounter()).isEqualTo(2);
        assertThat(lock.getLockCounter()).isEqualTo(2);
    }

    private static StoreCopyCheckPointMutex mutex(Lock lock) {
        return new StoreCopyCheckPointMutex(new ReadWriteLock() {
            @Override
            public Lock writeLock() {
                return lock;
            }

            @Override
            public Lock readLock() {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Test
    void tryCheckPointShouldWaitTheCurrentCheckPointingToCompleteNoRunCheckPointButUseTheTxIdOfTheEarlierRun()
            throws Throwable {
        // Given
        Lock lock = mock(Lock.class);
        when(lock.tryLock(anyLong(), any(TimeUnit.class))).thenReturn(true);
        final CheckPointerImpl checkPointing = checkPointer(mutex(lock));
        mockTxIdStore();

        checkPointing.forceCheckPoint(INFO);

        verify(appender)
                .checkPoint(
                        any(LogCheckPointEvent.class),
                        any(TransactionId.class),
                        anyLong(),
                        any(KernelVersion.class),
                        eq(logPosition),
                        eq(logPosition),
                        any(Instant.class),
                        any(String.class));
        reset(appender);

        checkPointing.tryCheckPoint(INFO);

        verifyNoMoreInteractions(appender);
    }

    @Test
    void tryCheckPointNoWaitShouldReturnWhenCheckPointIsAlreadyRunning() throws Throwable {
        // Given
        Lock lock = mock(Lock.class);
        when(lock.tryLock()).thenReturn(false);
        CheckPointerImpl checkPointing = checkPointer(mutex(lock));
        mockTxIdStore();

        // When
        long id = checkPointing.tryCheckPointNoWait(INFO);

        // Then
        assertEquals(-1, id);
        verifyNoMoreInteractions(appender);
    }

    @Test
    void propagateCheckpointingReason() throws IOException {
        mockTxIdStore();
        CheckPointerImpl checkPointer = checkPointer();
        checkPointer.start();

        String triggerName = "Test checkpoint reason";
        checkPointer.forceCheckPoint(new SimpleTriggerInfo(triggerName));

        verify(appender)
                .checkPoint(
                        any(LogCheckPointEvent.class),
                        any(TransactionId.class),
                        anyLong(),
                        any(KernelVersion.class),
                        eq(logPosition),
                        eq(logPosition),
                        any(Instant.class),
                        contains(triggerName));
    }

    @Test
    void tryCheckPointMustWaitForOnGoingCheckPointsToCompleteAsLongAsTimeoutPredicateIsFalse() throws Exception {
        mockTxIdStore();
        CheckPointerImpl checkPointer = checkPointer();
        BinaryLatch arriveFlushAndForce = new BinaryLatch();
        BinaryLatch finishFlushAndForce = new BinaryLatch();

        doAnswer(invocation -> {
                    arriveFlushAndForce.release();
                    finishFlushAndForce.await();
                    return null;
                })
                .when(forceOperation)
                .flushAndForce(any(), any());

        Thread forceCheckPointThread = new Thread(() -> {
            try {
                checkPointer.forceCheckPoint(INFO);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
        forceCheckPointThread.start();

        arriveFlushAndForce.await(); // Wait for force-thread to arrive in flushAndForce().

        BooleanSupplier predicate = mock(BooleanSupplier.class);
        when(predicate.getAsBoolean()).thenReturn(false, false, true);
        assertThat(checkPointer.tryCheckPoint(INFO, predicate))
                .isEqualTo(-1L); // We decided to not wait for the on-going check point to finish.

        finishFlushAndForce.release(); // Let the flushAndForce complete.
        forceCheckPointThread.join();

        assertThat(checkPointer.tryCheckPoint(INFO, predicate)).isEqualTo(this.transactionId);
    }

    private CheckPointerImpl checkPointer(StoreCopyCheckPointMutex mutex) {
        var databaseTracers = mock(DatabaseTracers.class);
        when(databaseTracers.getDatabaseTracer()).thenReturn(tracer);
        when(databaseTracers.getPageCacheTracer()).thenReturn(PageCacheTracer.NULL);
        when(metadataProvider.getStoreId()).thenReturn(storeId);
        when(metadataProvider.getLastAppendIndex()).thenReturn(initialAppendIndex);
        return new CheckPointerImpl(
                metadataProvider,
                threshold,
                forceOperation,
                logPruning,
                appender,
                panic,
                NullLogProvider.getInstance(),
                databaseTracers,
                mutex,
                new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER),
                clock,
                IOController.DISABLED,
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER);
    }

    private CheckPointerImpl checkPointer() {
        return checkPointer(new StoreCopyCheckPointMutex());
    }

    private void mockTxIdStore() {
        var initialCommitted = new ClosedTransactionMetadata(
                new TransactionId(
                        initialAppendIndex, initialAppendIndex, LATEST_KERNEL_VERSION, 4, 5, UNKNOWN_CONSENSUS_INDEX),
                logPosition);
        var otherCommitted = new ClosedTransactionMetadata(
                new TransactionId(
                        transactionId, TRANSACTION_APPEND_INDEX, LATEST_KERNEL_VERSION, 6, 7, UNKNOWN_CONSENSUS_INDEX),
                logPosition);
        when(metadataProvider.getLastClosedTransaction()).thenReturn(initialCommitted, otherCommitted);
        when(metadataProvider.getLastClosedTransactionId())
                .thenReturn(initialAppendIndex, transactionId, transactionId);
        when(metadataProvider.lastBatch()).thenReturn(new AppendBatchInfo(TRANSACTION_APPEND_INDEX, logPosition));
    }

    private class CheckpointCountingLock extends ReentrantLock {
        private final AtomicLong unlockCounter = new AtomicLong();
        private final AtomicLong lockCounter = new AtomicLong();

        @Override
        public void unlock() {
            try {
                unlockCounter.incrementAndGet();
                verify(appender)
                        .checkPoint(
                                any(LogCheckPointEvent.class),
                                any(TransactionId.class),
                                anyLong(),
                                any(KernelVersion.class),
                                any(LogPosition.class),
                                any(LogPosition.class),
                                any(Instant.class),
                                any(String.class));
                reset(appender);
                super.unlock();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void lock() {
            lockCounter.incrementAndGet();
            super.lock();
        }

        public long getUnlockCounter() {
            return unlockCounter.get();
        }

        public long getLockCounter() {
            return lockCounter.get();
        }
    }

    private static class CheckPointerThread extends Thread {
        private final CheckPointerImpl checkPointing;
        private final CountDownLatch startSignal;
        private final CountDownLatch completed;

        CheckPointerThread(CheckPointerImpl checkPointing, CountDownLatch startSignal, CountDownLatch completed) {
            this.checkPointing = checkPointing;
            this.startSignal = startSignal;
            this.completed = completed;
        }

        @Override
        public void run() {
            try {
                startSignal.countDown();
                startSignal.await();
                checkPointing.forceCheckPoint(INFO);
                completed.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
