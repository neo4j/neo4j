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

public class CachedThreadPoolExecutorFactoryTest {
    //    private static final int TEST_BOUNDED_QUEUE_SIZE = 5;
    //
    //    private final ExecutorFactory factory = new CachedThreadPoolExecutorFactory();
    //    private ExecutorService executorService;
    //
    //    private static Stream<Arguments> argumentsProvider() {
    //        return Stream.of(
    //                Arguments.of(UNBOUNDED_QUEUE, "Unbounded Queue"),
    //                Arguments.of(SYNCHRONOUS_QUEUE, "Synchronous Queue"),
    //                Arguments.of(TEST_BOUNDED_QUEUE_SIZE, "Bounded Queue"));
    //    }
    //
    //    @AfterEach
    //    public void cleanup() {
    //        if (executorService != null && !executorService.isTerminated()) {
    //            executorService.shutdown();
    //        }
    //    }
    //
    //    @ParameterizedTest
    //    @MethodSource("argumentsProvider")
    //    public void createShouldAssignCorrectQueue(int queueSize, String name) {
    //        executorService = factory.create(0, 1, Duration.ZERO, queueSize, false, newThreadFactory());
    //
    //        if (executorService instanceof ThreadPoolExecutor) {
    //            BlockingQueue<Runnable> queue = ((ThreadPoolExecutor) executorService).getQueue();
    //
    //            switch (queueSize) {
    //                case UNBOUNDED_QUEUE:
    //                    assertThat(queue).isInstanceOf(LinkedBlockingQueue.class);
    //                    assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
    //                    break;
    //                case SYNCHRONOUS_QUEUE:
    //                    assertThat(queue).isInstanceOf(SynchronousQueue.class);
    //                    break;
    //                case TEST_BOUNDED_QUEUE_SIZE:
    //                    assertThat(queue).isInstanceOf(ArrayBlockingQueue.class);
    //                    assertEquals(queueSize, queue.remainingCapacity());
    //                    break;
    //                default:
    //                    fail(String.format("Unexpected queue size %d", queueSize));
    //            }
    //        }
    //    }
    //
    //    @ParameterizedTest
    //    @MethodSource("argumentsProvider")
    //    public void createShouldCreateExecutor(int queueSize, String name) {
    //        executorService = factory.create(0, 1, Duration.ZERO, queueSize, false, newThreadFactory());
    //
    //        assertNotNull(executorService);
    //        assertFalse(executorService.isShutdown());
    //        assertFalse(executorService.isTerminated());
    //    }
    //
    //    @Test
    //    public void createShouldNotCreateExecutorWhenCorePoolSizeIsNegative() {
    //        try {
    //            factory.create(-1, 10, Duration.ZERO, 0, false, newThreadFactory());
    //            fail("should throw exception");
    //        } catch (IllegalArgumentException ex) {
    //            // expected
    //        }
    //    }
    //
    //    @Test
    //    public void createShouldNotCreateExecutorWhenMaxPoolSizeIsNegative() {
    //        try {
    //            factory.create(0, -1, Duration.ZERO, 0, false, newThreadFactory());
    //            fail("should throw exception");
    //        } catch (IllegalArgumentException ex) {
    //            // expected
    //        }
    //    }
    //
    //    @Test
    //    public void createShouldNotCreateExecutorWhenMaxPoolSizeIsZero() {
    //        try {
    //            factory.create(0, 0, Duration.ZERO, 0, false, newThreadFactory());
    //            fail("should throw exception");
    //        } catch (IllegalArgumentException ex) {
    //            // expected
    //        }
    //    }
    //
    //    @Test
    //    public void createShouldStartCoreThreadsIfAsked() {
    //        AtomicInteger threadCounter = new AtomicInteger();
    //
    //        factory.create(5, 10, Duration.ZERO, 0, true, newThreadFactoryWithCounter(threadCounter));
    //
    //        assertEquals(5, threadCounter.get());
    //    }
    //
    //    @Test
    //    public void createShouldNotStartCoreThreadsIfNotAsked() {
    //        AtomicInteger threadCounter = new AtomicInteger();
    //
    //        factory.create(5, 10, Duration.ZERO, 0, false, newThreadFactoryWithCounter(threadCounter));
    //
    //        assertEquals(0, threadCounter.get());
    //    }
    //
    //    @Test
    //    public void createShouldNotCreateExecutorWhenMaxPoolSizeIsLessThanCorePoolSize() {
    //        try {
    //            factory.create(10, 5, Duration.ZERO, 0, false, newThreadFactory());
    //            fail("should throw exception");
    //        } catch (IllegalArgumentException ex) {
    //            // expected
    //        }
    //    }
    //
    //    @Test
    //    public void createdExecutorShouldExecuteSubmittedTasks() throws Exception {
    //        AtomicBoolean exitCondition = new AtomicBoolean(false);
    //        AtomicInteger threadCounter = new AtomicInteger(0);
    //
    //        executorService = factory.create(0, 1, Duration.ZERO, 0, false,
    // newThreadFactoryWithCounter(threadCounter));
    //
    //        assertNotNull(executorService);
    //        assertEquals(0, threadCounter.get());
    //
    //        Future task1 = executorService.submit(newInfiniteWaitingRunnable(exitCondition));
    //        assertEquals(1, threadCounter.get());
    //
    //        exitCondition.set(true);
    //
    //        assertNull(task1.get(1, MINUTES));
    //        assertTrue(task1.isDone());
    //        assertFalse(task1.isCancelled());
    //    }
    //
    //    @Test
    //    public void createdExecutorShouldFavorPoolSizes() {
    //        AtomicBoolean exitCondition = new AtomicBoolean(false);
    //        AtomicInteger threadCounter = new AtomicInteger(0);
    //
    //        executorService = factory.create(0, 5, Duration.ZERO, 0, false,
    // newThreadFactoryWithCounter(threadCounter));
    //
    //        assertNotNull(executorService);
    //        assertEquals(0, threadCounter.get());
    //
    //        try {
    //            for (int i = 0; i < 6; i++) {
    //                executorService.submit(newInfiniteWaitingRunnable(exitCondition));
    //            }
    //
    //            fail("should throw exception");
    //        } catch (RejectedExecutionException ex) {
    //            // expected
    //        }
    //
    //        assertEquals(5, threadCounter.get());
    //    }
    //
    //    private static Runnable newInfiniteWaitingRunnable(AtomicBoolean exitCondition) {
    //        return () -> Predicates.awaitForever(
    //                () -> Thread.currentThread().isInterrupted() || exitCondition.get(), 500, MILLISECONDS);
    //    }
    //
    //    private static ThreadFactory newThreadFactory() {
    //        return Executors.defaultThreadFactory();
    //    }
    //
    //    private static ThreadFactory newThreadFactoryWithCounter(AtomicInteger counter) {
    //        return job -> {
    //            counter.incrementAndGet();
    //            return Executors.defaultThreadFactory().newThread(job);
    //        };
    //    }
}
