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

class ExecutorBoltSchedulerTest {
    //    private static final String CONNECTOR_KEY = "connector-id";
    //    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    //    private final LogService logService = new SimpleLogService(logProvider);
    //    private final ExecutorFactory executorFactory = new CachedThreadPoolExecutorFactory();
    //    private final JobScheduler jobScheduler = mock(JobScheduler.class);
    //    private final ExecutorBoltScheduler boltScheduler = new ExecutorBoltScheduler(
    //            CONNECTOR_KEY,
    //            executorFactory,
    //            jobScheduler,
    //            logService,
    //            0,
    //            10,
    //            Duration.ofMinutes(1),
    //            0,
    //            ForkJoinPool.commonPool(),
    //            Duration.ZERO,
    //            BoltConnector.KeepAliveRequestType.OFF,
    //            Duration.ZERO);
    //
    //    @BeforeEach
    //    void setup() {
    //        when(jobScheduler.threadFactory(any())).thenReturn(Executors.defaultThreadFactory());
    //    }
    //
    //    @AfterEach
    //    void cleanup() throws Throwable {
    //        boltScheduler.stop();
    //        boltScheduler.shutdown();
    //    }
    //
    //    @Test
    //    void initShouldCreateThreadPool() throws Throwable {
    //        ExecutorFactory mockExecutorFactory = mock(ExecutorFactory.class);
    //        when(mockExecutorFactory.create(anyInt(), anyInt(), any(), anyInt(), anyBoolean(), any()))
    //                .thenReturn(Executors.newCachedThreadPool());
    //        ExecutorBoltScheduler scheduler = new ExecutorBoltScheduler(
    //                CONNECTOR_KEY,
    //                mockExecutorFactory,
    //                jobScheduler,
    //                logService,
    //                0,
    //                10,
    //                Duration.ofMinutes(1),
    //                0,
    //                ForkJoinPool.commonPool(),
    //                Duration.ZERO,
    //                BoltConnector.KeepAliveRequestType.OFF,
    //                Duration.ZERO);
    //
    //        scheduler.init();
    //
    //        verify(jobScheduler).threadFactory(Group.BOLT_WORKER);
    //        verify(mockExecutorFactory)
    //                .create(anyInt(), anyInt(), any(Duration.class), anyInt(), anyBoolean(),
    // any(ThreadFactory.class));
    //    }
    //
    //    @Test
    //    void shutdownShouldTerminateThreadPool() throws Throwable {
    //        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
    //        ExecutorFactory mockExecutorFactory = mock(ExecutorFactory.class);
    //        when(mockExecutorFactory.create(anyInt(), anyInt(), any(), anyInt(), anyBoolean(), any()))
    //                .thenReturn(cachedThreadPool);
    //        ExecutorBoltScheduler scheduler = new ExecutorBoltScheduler(
    //                CONNECTOR_KEY,
    //                mockExecutorFactory,
    //                jobScheduler,
    //                logService,
    //                0,
    //                10,
    //                Duration.ofMinutes(1),
    //                0,
    //                ForkJoinPool.commonPool(),
    //                Duration.ZERO,
    //                BoltConnector.KeepAliveRequestType.OFF,
    //                Duration.ZERO);
    //
    //        scheduler.init();
    //        scheduler.shutdown();
    //
    //        assertTrue(cachedThreadPool.isShutdown());
    //    }
    //
    //    @Test
    //    void createdShouldAddConnectionToActiveConnections() throws Throwable {
    //        String id = UUID.randomUUID().toString();
    //        BoltConnection connection = newConnection(id);
    //
    //        boltScheduler.init();
    //        boltScheduler.start();
    //        boltScheduler.created(connection);
    //
    //        verify(connection).id();
    //        assertTrue(boltScheduler.isRegistered(connection));
    //    }
    //
    //    @Test
    //    void destroyedShouldRemoveConnectionFromActiveConnections() throws Throwable {
    //        String id = UUID.randomUUID().toString();
    //        BoltConnection connection = newConnection(id);
    //
    //        boltScheduler.init();
    //        boltScheduler.start();
    //        boltScheduler.created(connection);
    //        boltScheduler.closed(connection);
    //
    //        assertFalse(boltScheduler.isRegistered(connection));
    //    }
    //
    //    @Test
    //    void enqueuedShouldScheduleJob() throws Throwable {
    //        String id = UUID.randomUUID().toString();
    //        AtomicBoolean exitCondition = new AtomicBoolean();
    //        BoltConnection connection = newConnection(id);
    //        when(connection.processNextBatch()).thenAnswer(inv -> awaitExit(exitCondition));
    //
    //        boltScheduler.init();
    //        boltScheduler.start();
    //        boltScheduler.created(connection);
    //        boltScheduler.enqueued(connection, Jobs.noop());
    //
    //        Predicates.await(() -> boltScheduler.isActive(connection), 1, MINUTES);
    //        exitCondition.set(true);
    //        Predicates.await(() -> !boltScheduler.isActive(connection), 1, MINUTES);
    //
    //        verify(connection).processNextBatch();
    //    }
    //
    //    @Test
    //    void enqueuedShouldNotScheduleJobWhenActiveWorkItemExists() throws Throwable {
    //        String id = UUID.randomUUID().toString();
    //        BoltConnection connection = newConnection(id);
    //        AtomicBoolean exitCondition = new AtomicBoolean();
    //        when(connection.processNextBatch()).thenAnswer(inv -> awaitExit(exitCondition));
    //
    //        boltScheduler.init();
    //        boltScheduler.start();
    //        boltScheduler.created(connection);
    //        boltScheduler.enqueued(connection, Jobs.noop());
    //
    //        Predicates.await(() -> boltScheduler.isActive(connection), 1, MINUTES);
    //        boltScheduler.enqueued(connection, Jobs.noop());
    //        exitCondition.set(true);
    //        Predicates.await(() -> !boltScheduler.isActive(connection), 1, MINUTES);
    //
    //        verify(connection).processNextBatch();
    //    }
    //
    //    @Test
    //    void failingJobShouldLogAndStopConnection() throws Throwable {
    //        AtomicBoolean stopped = new AtomicBoolean();
    //        String id = UUID.randomUUID().toString();
    //        BoltConnection connection = newConnection(id);
    //        var unexpectedError = new RuntimeException("some unexpected error");
    //        doThrow(unexpectedError).when(connection).processNextBatch();
    //        doAnswer(inv -> stopped.getAndSet(true)).when(connection).stop();
    //
    //        boltScheduler.init();
    //        boltScheduler.start();
    //        boltScheduler.created(connection);
    //        boltScheduler.enqueued(connection, Jobs.noop());
    //
    //        Predicates.await(stopped::get, 1, MINUTES);
    //
    //        assertFalse(boltScheduler.isActive(connection));
    //        verify(connection).processNextBatch();
    //        verify(connection).stop();
    //
    //        assertThat(logProvider)
    //                .forClass(ExecutorBoltScheduler.class)
    //                .forLevel(ERROR)
    //                .assertExceptionForLogMessage("Unexpected error during job scheduling for session")
    //                .hasCause(unexpectedError);
    //    }
    //
    //    @Test
    //    void successfulJobsShouldTriggerSchedulingOfPendingJobs() throws Throwable {
    //        AtomicInteger counter = new AtomicInteger();
    //        String id = UUID.randomUUID().toString();
    //        BoltConnection connection = newConnection(id);
    //        when(connection.processNextBatch()).thenAnswer(inv -> counter.incrementAndGet() > 0);
    //        when(connection.hasPendingJobs()).thenReturn(true).thenReturn(false);
    //
    //        boltScheduler.init();
    //        boltScheduler.start();
    //        boltScheduler.created(connection);
    //        boltScheduler.enqueued(connection, Jobs.noop());
    //
    //        Predicates.await(() -> counter.get() > 1, 1, MINUTES);
    //
    //        verify(connection, times(2)).processNextBatch();
    //    }
    //
    //    @Test
    //    void destroyedShouldCancelActiveWorkItem() throws Throwable {
    //        AtomicInteger processNextBatchCount = new AtomicInteger();
    //        String id = UUID.randomUUID().toString();
    //        BoltConnection connection = newConnection(id);
    //        AtomicBoolean exitCondition = new AtomicBoolean();
    //        when(connection.processNextBatch()).thenAnswer(inv -> {
    //            processNextBatchCount.incrementAndGet();
    //            return awaitExit(exitCondition);
    //        });
    //
    //        boltScheduler.init();
    //        boltScheduler.start();
    //        boltScheduler.created(connection);
    //        boltScheduler.enqueued(connection, Jobs.noop());
    //
    //        Predicates.await(() -> processNextBatchCount.get() > 0, 1, MINUTES);
    //
    //        boltScheduler.closed(connection);
    //
    //        Predicates.await(() -> !boltScheduler.isActive(connection), 1, MINUTES);
    //
    //        assertFalse(boltScheduler.isActive(connection));
    //        assertEquals(1, processNextBatchCount.get());
    //
    //        exitCondition.set(true);
    //    }
    //
    //    @Test
    //    void createdWorkerThreadsShouldContainConnectorName() throws Exception {
    //        AtomicInteger executeBatchCompletionCount = new AtomicInteger();
    //        AtomicReference<Thread> poolThread = new AtomicReference<>();
    //        AtomicReference<String> poolThreadName = new AtomicReference<>();
    //
    //        String id = UUID.randomUUID().toString();
    //        BoltConnection connection = newConnection(id);
    //        when(connection.hasPendingJobs()).thenAnswer(inv -> {
    //            executeBatchCompletionCount.incrementAndGet();
    //            return false;
    //        });
    //        when(connection.processNextBatch()).thenAnswer(inv -> {
    //            poolThread.set(Thread.currentThread());
    //            poolThreadName.set(Thread.currentThread().getName());
    //            return true;
    //        });
    //
    //        boltScheduler.init();
    //        boltScheduler.start();
    //        boltScheduler.created(connection);
    //        boltScheduler.enqueued(connection, Jobs.noop());
    //
    //        Predicates.await(() -> executeBatchCompletionCount.get() > 0, 1, MINUTES);
    //
    //        assertThat(poolThread.get().getName()).isNotEqualTo(poolThreadName.get());
    //        assertThat(poolThread.get().getName()).contains(String.format("[%s]", CONNECTOR_KEY));
    //        assertThat(poolThread.get().getName()).doesNotContain(String.format("[%s]", connection.remoteAddress()));
    //    }
    //
    //    @Test
    //    void createdWorkerThreadsShouldContainConnectorNameAndRemoteAddressInTheirNamesWhenActive() throws Exception {
    //        final AtomicReference<String> capturedThreadName = new AtomicReference<>();
    //
    //        AtomicInteger processNextBatchCount = new AtomicInteger();
    //        String id = UUID.randomUUID().toString();
    //        BoltConnection connection = newConnection(id);
    //        AtomicBoolean exitCondition = new AtomicBoolean();
    //        when(connection.processNextBatch()).thenAnswer(inv -> {
    //            capturedThreadName.set(Thread.currentThread().getName());
    //            processNextBatchCount.incrementAndGet();
    //            return awaitExit(exitCondition);
    //        });
    //
    //        boltScheduler.init();
    //        boltScheduler.start();
    //        boltScheduler.created(connection);
    //        boltScheduler.enqueued(connection, Jobs.noop());
    //
    //        Predicates.await(() -> processNextBatchCount.get() > 0, 1, MINUTES);
    //
    //        assertThat(capturedThreadName.get()).contains(String.format("[%s]", CONNECTOR_KEY));
    //        assertThat(capturedThreadName.get()).contains(String.format("[%s]", connection.remoteAddress()));
    //
    //        exitCondition.set(true);
    //    }
    //
    //    @Test
    //    void stopShouldStopIdleConnections() throws Exception {
    //        boltScheduler.init();
    //        boltScheduler.start();
    //
    //        var connection1 = newConnection(boltScheduler, true);
    //        var connection2 = newConnection(boltScheduler, false);
    //        var connection3 = newConnection(boltScheduler, true);
    //
    //        boltScheduler.stop();
    //
    //        verify(connection1, times(1)).stop();
    //        verify(connection2, never()).stop();
    //        verify(connection3, times(1)).stop();
    //    }
    //
    //    @Test
    //    void shutdownShouldStopAllConnections() throws Exception {
    //        boltScheduler.init();
    //        boltScheduler.start();
    //
    //        var connection1 = newConnection(boltScheduler, true);
    //        var connection2 = newConnection(boltScheduler, false);
    //        var connection3 = newConnection(boltScheduler, true);
    //
    //        boltScheduler.shutdown();
    //
    //        verify(connection1, times(1)).stop();
    //        verify(connection2, times(1)).stop();
    //        verify(connection3, times(1)).stop();
    //    }
    //
    //    private static BoltConnection newConnection(String id) {
    //        BoltConnection result = mock(BoltConnection.class);
    //        when(result.id()).thenReturn(id);
    //        when(result.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 32_000));
    //        return result;
    //    }
    //
    //    private static BoltConnection newConnection(ExecutorBoltScheduler boltScheduler, boolean isIdle) {
    //        var connection = newConnection(UUID.randomUUID().toString());
    //        boltScheduler.created(connection);
    //        when(connection.idle()).thenReturn(isIdle);
    //        return connection;
    //    }
    //
    //    private static boolean awaitExit(AtomicBoolean exitCondition) {
    //        Predicates.awaitForever(() -> Thread.currentThread().isInterrupted() || exitCondition.get(), 500,
    // MILLISECONDS);
    //        return true;
    //    }
}
