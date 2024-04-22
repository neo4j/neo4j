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

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Panic;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.OtherThreadExecutor;

class CheckPointSchedulerTest {
    private final CheckPointer checkPointer = mock(CheckPointer.class);
    private final OnDemandJobScheduler jobScheduler = spy(new OnDemandJobScheduler());
    private final Panic panic = mock(DatabaseHealth.class);

    private static ExecutorService executor;

    @BeforeAll
    static void setUpExecutor() {
        executor = Executors.newCachedThreadPool();
    }

    @AfterAll
    static void tearDownExecutor() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    void shouldScheduleTheCheckPointerJobOnStart() {
        // given
        CheckPointScheduler scheduler = new CheckPointScheduler(checkPointer, jobScheduler, 20L, panic, "test db");

        assertNull(jobScheduler.getJob());

        // when
        scheduler.start();

        // then
        assertNotNull(jobScheduler.getJob());
        verify(jobScheduler)
                .schedule(
                        eq(Group.CHECKPOINT),
                        any(JobMonitoringParams.class),
                        any(Runnable.class),
                        eq(20L),
                        eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldRescheduleTheJobAfterARun() throws Throwable {
        // given
        CheckPointScheduler scheduler = new CheckPointScheduler(checkPointer, jobScheduler, 20L, panic, "test db");

        assertNull(jobScheduler.getJob());

        scheduler.start();

        Object scheduledJob = jobScheduler.getJob();
        assertNotNull(scheduledJob);

        // when
        jobScheduler.runJob();

        // then
        verify(jobScheduler, times(2))
                .schedule(
                        eq(Group.CHECKPOINT),
                        any(JobMonitoringParams.class),
                        any(Runnable.class),
                        eq(20L),
                        eq(TimeUnit.MILLISECONDS));
        verify(checkPointer).checkPointIfNeeded(any(TriggerInfo.class));
        assertEquals(scheduledJob, jobScheduler.getJob());
    }

    @Test
    void shouldNotRescheduleAJobWhenStopped() {
        // given
        CheckPointScheduler scheduler = new CheckPointScheduler(checkPointer, jobScheduler, 20L, panic, "test db");

        assertNull(jobScheduler.getJob());

        scheduler.start();

        assertNotNull(jobScheduler.getJob());

        // when
        scheduler.stop();

        // then
        assertNull(jobScheduler.getJob());
    }

    @Test
    void stoppedJobCantBeInvoked() throws Throwable {
        CheckPointScheduler scheduler = new CheckPointScheduler(checkPointer, jobScheduler, 10L, panic, "test db");
        scheduler.start();
        jobScheduler.runJob();

        // verify checkpoint was triggered
        verify(checkPointer).checkPointIfNeeded(any(TriggerInfo.class));

        // simulate scheduled run that was triggered just before stop
        scheduler.stop();
        scheduler.start();
        jobScheduler.runJob();

        // checkpointer should not be invoked now because job stopped
        verifyNoMoreInteractions(checkPointer);
    }

    @Test
    void shouldWaitOnStopUntilTheRunningCheckpointIsDone() {
        // Timeout as fallback safety if test deadlocks
        assertTimeoutPreemptively(ofSeconds(60), this::testWaitOnStopUntilTheRunningCheckpointIsDone);
    }

    void testWaitOnStopUntilTheRunningCheckpointIsDone() throws Throwable {
        // given
        final AtomicReference<Throwable> ex = new AtomicReference<>();
        final AtomicBoolean stoppedCompleted = new AtomicBoolean();
        final DoubleLatch checkPointerLatch = new DoubleLatch(1);
        OtherThreadExecutor otherThreadExecutor = new OtherThreadExecutor("scheduler stopper");
        CheckPointer checkPointer = new CheckPointer() {
            @Override
            public long checkPointIfNeeded(TriggerInfo triggerInfo) {
                checkPointerLatch.startAndWaitForAllToStart();
                checkPointerLatch.waitForAllToFinish();
                return 42;
            }

            @Override
            public long tryCheckPoint(TriggerInfo triggerInfo) {
                throw new RuntimeException("this should have not been called");
            }

            @Override
            public long tryCheckPoint(TriggerInfo triggerInfo, BooleanSupplier timeout) {
                throw new RuntimeException("this should have not been called");
            }

            @Override
            public long tryCheckPointNoWait(TriggerInfo triggerInfo) {
                throw new RuntimeException("this should have not been called");
            }

            @Override
            public long forceCheckPoint(TriggerInfo triggerInfo) {
                throw new RuntimeException("this should have not been called");
            }

            @Override
            public long forceCheckPoint(
                    TransactionId transactionId, long appendIndex, LogPosition position, TriggerInfo triggerInfo) {
                return 0;
            }

            @Override
            public LatestCheckpointInfo latestCheckPointInfo() {
                return LatestCheckpointInfo.UNKNOWN_CHECKPOINT_INFO;
            }

            @Override
            public void shutdown() {}
        };

        final CheckPointScheduler scheduler =
                new CheckPointScheduler(checkPointer, jobScheduler, 20L, panic, "test db");

        // when
        scheduler.start();

        Thread runCheckPointer = new Thread(jobScheduler::runJob);
        runCheckPointer.start();

        checkPointerLatch.waitForAllToStart();

        otherThreadExecutor.executeDontWait(() -> {
            try {
                scheduler.stop();
                stoppedCompleted.set(true);
            } catch (Throwable throwable) {
                ex.set(throwable);
            }
            return null;
        });
        otherThreadExecutor.waitUntilWaiting(
                details -> details.isAt(CheckPointScheduler.class, "waitOngoingCheckpointCompletion"));

        // then
        assertFalse(stoppedCompleted.get());

        checkPointerLatch.finish();
        runCheckPointer.join();

        while (!stoppedCompleted.get()) {
            Thread.sleep(1);
        }
        otherThreadExecutor.close();

        assertNull(ex.get());
    }

    @Test
    void shouldContinueThroughSporadicFailures() {
        // GIVEN
        ControlledCheckPointer checkPointer = new ControlledCheckPointer();
        CheckPointScheduler scheduler = new CheckPointScheduler(checkPointer, jobScheduler, 1, panic, "test db");
        scheduler.start();

        // WHEN/THEN
        for (int i = 0; i < CheckPointScheduler.MAX_CONSECUTIVE_FAILURES_TOLERANCE * 2; i++) {
            // Fail
            checkPointer.fail = true;
            jobScheduler.runJob();
            verifyNoInteractions(panic);

            // Succeed
            checkPointer.fail = false;
            jobScheduler.runJob();
            verifyNoInteractions(panic);
        }
    }

    @Test
    void checkpointOnStopShouldFlushAsFastAsPossibleWithDefaultController() {
        assertTimeoutPreemptively(ofSeconds(10), this::testCheckpointOnStopShouldFlushAsFastAsPossible);
    }

    void testCheckpointOnStopShouldFlushAsFastAsPossible() throws Throwable {
        CountDownLatch checkPointerLatch = new CountDownLatch(1);
        WaitUnlimitedCheckPointer checkPointer =
                new WaitUnlimitedCheckPointer(IOController.DISABLED, checkPointerLatch);
        CheckPointScheduler scheduler = new CheckPointScheduler(checkPointer, jobScheduler, 0L, panic, "test db");
        scheduler.start();

        Future<?> checkpointerStarter = executor.submit(jobScheduler::runJob);

        checkPointerLatch.await();
        scheduler.stop();
        checkpointerStarter.get();

        assertTrue(checkPointer.isCheckpointCreated(), "Checkpointer should be created.");
    }

    @Test
    void shouldCausePanicAfterSomeFailures() throws Throwable {
        // GIVEN
        RuntimeException[] failures = new RuntimeException[] {
            new RuntimeException("First"), new RuntimeException("Second"), new RuntimeException("Third")
        };
        when(checkPointer.checkPointIfNeeded(any(TriggerInfo.class))).thenThrow(failures);
        CheckPointScheduler scheduler = new CheckPointScheduler(checkPointer, jobScheduler, 1, panic, "test db");
        scheduler.start();

        // WHEN
        for (int i = 0; i < CheckPointScheduler.MAX_CONSECUTIVE_FAILURES_TOLERANCE - 1; i++) {
            jobScheduler.runJob();
            verifyNoInteractions(panic);
        }

        UnderlyingStorageException error = assertThrows(UnderlyingStorageException.class, jobScheduler::runJob);

        // THEN
        assertEquals(Iterators.asSet(failures), Iterators.asSet(error.getSuppressed()));
        verify(panic).panic(error);
    }

    private static class ControlledCheckPointer implements CheckPointer {
        volatile boolean fail;

        @Override
        public long checkPointIfNeeded(TriggerInfo triggerInfo) throws IOException {
            if (fail) {
                throw new IOException("Just failing");
            }
            return 1;
        }

        @Override
        public long tryCheckPoint(TriggerInfo triggerInfo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long tryCheckPoint(TriggerInfo triggerInfo, BooleanSupplier timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long tryCheckPointNoWait(TriggerInfo triggerInfo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long forceCheckPoint(TriggerInfo triggerInfo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long forceCheckPoint(
                TransactionId transactionId, long appendIndex, LogPosition position, TriggerInfo triggerInfo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LatestCheckpointInfo latestCheckPointInfo() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {}
    }

    private static class WaitUnlimitedCheckPointer implements CheckPointer {
        private final IOController ioLimiter;
        private final CountDownLatch latch;
        private volatile boolean checkpointCreated;

        WaitUnlimitedCheckPointer(IOController ioLimiter, CountDownLatch latch) {
            this.ioLimiter = ioLimiter;
            this.latch = latch;
            checkpointCreated = false;
        }

        @Override
        public long checkPointIfNeeded(TriggerInfo triggerInfo) {
            latch.countDown();
            while (ioLimiter.isEnabled()) {
                // spin while limiter enabled
            }
            checkpointCreated = true;
            return 42;
        }

        @Override
        public long tryCheckPoint(TriggerInfo triggerInfo) {
            throw new UnsupportedOperationException("This should have not been called");
        }

        @Override
        public long tryCheckPoint(TriggerInfo triggerInfo, BooleanSupplier timeout) {
            throw new UnsupportedOperationException("This should have not been called");
        }

        @Override
        public long tryCheckPointNoWait(TriggerInfo triggerInfo) {
            throw new UnsupportedOperationException("This should have not been called");
        }

        @Override
        public long forceCheckPoint(TriggerInfo triggerInfo) {
            throw new UnsupportedOperationException("This should have not been called");
        }

        @Override
        public long forceCheckPoint(
                TransactionId transactionId, long appendIndex, LogPosition position, TriggerInfo triggerInfo) {
            throw new UnsupportedOperationException("This should have not been called");
        }

        @Override
        public LatestCheckpointInfo latestCheckPointInfo() {
            return LatestCheckpointInfo.UNKNOWN_CHECKPOINT_INFO;
        }

        @Override
        public void shutdown() {}

        boolean isCheckpointCreated() {
            return checkpointCreated;
        }
    }
}
