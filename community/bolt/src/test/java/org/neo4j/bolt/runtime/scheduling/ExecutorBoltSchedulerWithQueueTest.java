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
package org.neo4j.bolt.runtime.scheduling;

public class ExecutorBoltSchedulerWithQueueTest {
    //    private static final String CONNECTOR_KEY = "connector-id";
    //    private static final int threadPoolSize = 1;
    //    private static final int queueSize = 1;
    //
    //    private final CountDownLatch beforeExecuteEvent = new CountDownLatch(1);
    //    private final CountDownLatch beforeExecuteBarrier = new CountDownLatch(threadPoolSize);
    //    private final CountDownLatch afterExecuteEvent = new CountDownLatch(1);
    //    private final CountDownLatch afterExecuteBarrier = new CountDownLatch(threadPoolSize);
    //
    //    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    //    private final LogService logService = new SimpleLogService(logProvider);
    //    private final ExecutorFactory executorFactory = new NotifyingThreadPoolWithBlockingQueueFactory();
    //    private final JobScheduler jobScheduler = mock(JobScheduler.class);
    //    private final ExecutorBoltScheduler boltSchedulerWithQueue = new ExecutorBoltScheduler(
    //            CONNECTOR_KEY,
    //            executorFactory,
    //            jobScheduler,
    //            logService,
    //            threadPoolSize,
    //            threadPoolSize,
    //            Duration.ofMinutes(1),
    //            queueSize,
    //            ForkJoinPool.commonPool(),
    //            Duration.ZERO,
    //            BoltConnector.KeepAliveRequestType.OFF,
    //            Duration.ZERO);
    //
    //    @BeforeEach
    //    void setup() throws Throwable {
    //        when(jobScheduler.threadFactory(any())).thenReturn(Executors.defaultThreadFactory());
    //
    //        boltSchedulerWithQueue.init();
    //        boltSchedulerWithQueue.start();
    //    }
    //
    //    @AfterEach
    //    void cleanup() throws Throwable {
    //        boltSchedulerWithQueue.stop();
    //        boltSchedulerWithQueue.shutdown();
    //    }
    //
    //    @Test
    //    void shouldInvokeHandleSchedulingErrorIfNoThreadsAvailableAndFullQueue() throws Throwable {
    //        AtomicInteger handleSchedulingErrorCounter = new AtomicInteger(0);
    //        BoltConnection newConnection = newConnection(UUID.randomUUID().toString());
    //        doAnswer(newCountingAnswer(handleSchedulingErrorCounter))
    //                .when(newConnection)
    //                .handleSchedulingError(any());
    //
    //        submitWork(threadPoolSize + queueSize);
    //
    //        // register connection
    //        boltSchedulerWithQueue.created(newConnection);
    //
    //        // send a job and wait for it to enter handleSchedulingError and block there
    //        CompletableFuture.runAsync(() -> boltSchedulerWithQueue.enqueued(newConnection, Jobs.noop()));
    //        Predicates.awaitForever(() -> handleSchedulingErrorCounter.get() > 0, 500, MILLISECONDS);
    //
    //        // verify that handleSchedulingError is called once
    //        assertEquals(1, handleSchedulingErrorCounter.get());
    //
    //        // allow all threads to complete
    //        afterExecuteEvent.countDown();
    //        afterExecuteBarrier.await();
    //    }
    //
    //    @Test
    //    void shouldStartQueueingWorkIfThreadPoolIsOccupied() throws Throwable {
    //        AtomicInteger handleSchedulingErrorCounter = new AtomicInteger(0);
    //        BoltConnection newConnection = newConnection(UUID.randomUUID().toString());
    //        doAnswer(newCountingAnswer(handleSchedulingErrorCounter))
    //                .when(newConnection)
    //                .handleSchedulingError(any());
    //
    //        submitWork(threadPoolSize);
    //
    //        // register connection
    //        boltSchedulerWithQueue.created(newConnection);
    //
    //        // send a job which we expect to be queued
    //        assertDoesNotThrow(() -> boltSchedulerWithQueue.enqueued(newConnection, Jobs.noop()));
    //
    //        // verify that handleSchedulingError is not called
    //        assertEquals(0, handleSchedulingErrorCounter.get());
    //
    //        // allow all threads to complete
    //        afterExecuteEvent.countDown();
    //        afterExecuteBarrier.await();
    //    }
    //
    //    @Test
    //    void shouldNotScheduleNewJobIfHandlingSchedulingError() throws Throwable {
    //        AtomicInteger handleSchedulingErrorCounter = new AtomicInteger(0);
    //        AtomicBoolean exitCondition = new AtomicBoolean();
    //        BoltConnection newConnection = newConnection(UUID.randomUUID().toString());
    //        doAnswer(newBlockingAnswer(handleSchedulingErrorCounter, exitCondition))
    //                .when(newConnection)
    //                .handleSchedulingError(any());
    //
    //        submitWork(threadPoolSize + queueSize);
    //
    //        // register connection
    //        boltSchedulerWithQueue.created(newConnection);
    //
    //        // send a job and wait for it to enter handleSchedulingError and block there
    //        CompletableFuture.runAsync(() -> boltSchedulerWithQueue.enqueued(newConnection, Jobs.noop()));
    //        Predicates.awaitForever(() -> handleSchedulingErrorCounter.get() > 0, 500, MILLISECONDS);
    //
    //        // allow all threads to complete
    //        afterExecuteEvent.countDown();
    //        afterExecuteBarrier.await();
    //
    //        // post a job
    //        boltSchedulerWithQueue.enqueued(newConnection, Jobs.noop());
    //
    //        // exit handleSchedulingError
    //        exitCondition.set(true);
    //
    //        // verify that handleSchedulingError is called once and processNextBatch never.
    //        assertEquals(1, handleSchedulingErrorCounter.get());
    //        verify(newConnection, never()).processNextBatch();
    //    }
    //
    //    private void submitWork(int noOfWorkItemsToSubmit) throws InterruptedException {
    //        for (int i = 0; i < noOfWorkItemsToSubmit; i++) {
    //            BoltConnection connection = newConnection(UUID.randomUUID().toString());
    //            boltSchedulerWithQueue.created(connection);
    //            boltSchedulerWithQueue.enqueued(connection, Jobs.noop());
    //        }
    //
    //        beforeExecuteEvent.countDown();
    //        beforeExecuteBarrier.await();
    //    }
    //
    //    private <T> Answer<T> newCountingAnswer(AtomicInteger counter) {
    //        return invocationOnMock -> {
    //            counter.incrementAndGet();
    //            return null;
    //        };
    //    }
    //
    //    private <T> Answer<T> newBlockingAnswer(AtomicInteger counter, AtomicBoolean exitCondition) {
    //        return invocationOnMock -> {
    //            counter.incrementAndGet();
    //            Predicates.awaitForever(
    //                    () -> Thread.currentThread().isInterrupted() || exitCondition.get(), 500, MILLISECONDS);
    //            return null;
    //        };
    //    }
    //
    //    private BoltConnection newConnection(String id) {
    //        BoltConnection result = mock(BoltConnection.class);
    //        when(result.id()).thenReturn(id);
    //        when(result.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 32_000));
    //        return result;
    //    }
    //
    //    private class NotifyingThreadPoolWithBlockingQueueFactory implements ExecutorFactory {
    //        @Override
    //        public ExecutorService create(
    //                int corePoolSize,
    //                int maxPoolSize,
    //                Duration keepAlive,
    //                int queueSize,
    //                boolean startCoreThreads,
    //                ThreadFactory threadFactory) {
    //            return new ExecutorBoltSchedulerWithQueueTest.NotifyingThreadPoolExecutor(
    //                    corePoolSize,
    //                    maxPoolSize,
    //                    keepAlive,
    //                    new ArrayBlockingQueue<>(queueSize),
    //                    threadFactory,
    //                    new ThreadPoolExecutor.AbortPolicy());
    //        }
    //    }
    //
    //    private class NotifyingThreadPoolExecutor extends ThreadPoolExecutor {
    //
    //        private NotifyingThreadPoolExecutor(
    //                int corePoolSize,
    //                int maxPoolSize,
    //                Duration keepAlive,
    //                BlockingQueue<Runnable> workQueue,
    //                ThreadFactory threadFactory,
    //                RejectedExecutionHandler rejectionHandler) {
    //            super(
    //                    corePoolSize,
    //                    maxPoolSize,
    //                    keepAlive.toMillis(),
    //                    MILLISECONDS,
    //                    workQueue,
    //                    threadFactory,
    //                    rejectionHandler);
    //        }
    //
    //        @Override
    //        protected void beforeExecute(Thread t, Runnable r) {
    //            try {
    //                beforeExecuteEvent.await();
    //                super.beforeExecute(t, r);
    //                beforeExecuteBarrier.countDown();
    //            } catch (Throwable ex) {
    //                throw new RuntimeException(ex);
    //            }
    //        }
    //
    //        @Override
    //        protected void afterExecute(Runnable r, Throwable t) {
    //            try {
    //                afterExecuteEvent.await();
    //                super.afterExecute(r, t);
    //                afterExecuteBarrier.countDown();
    //            } catch (Throwable ex) {
    //                throw new RuntimeException(ex);
    //            }
    //        }
    //    }
}
