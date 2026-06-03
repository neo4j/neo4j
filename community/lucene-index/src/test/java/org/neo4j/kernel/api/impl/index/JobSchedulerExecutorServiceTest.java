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
package org.neo4j.kernel.api.impl.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.scheduler.Group;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

class JobSchedulerExecutorServiceTest {
    private ThreadPoolJobScheduler scheduler;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        scheduler = new ThreadPoolJobScheduler();
        executor = JobSchedulerExecutorService.nonShutdownable(scheduler, Group.VECTOR_INDEX_MERGE);
    }

    @AfterEach
    void tearDown() throws Exception {
        scheduler.close();
    }

    @Test
    void shouldRunRunnableViaExecute() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        executor.execute(latch::countDown);
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldRunCallableViaSubmit() throws Exception {
        Future<Integer> future = executor.submit((Callable<Integer>) () -> 42);
        assertThat(future.get(5, TimeUnit.SECONDS)).isEqualTo(42);
    }

    @Test
    void shouldRunMultipleTasksViaInvokeAll() throws Exception {
        List<Callable<Integer>> tasks = List.of(() -> 1, () -> 2, () -> 3);
        List<Future<Integer>> futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
        assertThat(futures).hasSize(3);
        int sum = 0;
        for (Future<Integer> f : futures) {
            sum += f.get();
        }
        assertThat(sum).isEqualTo(6);
    }

    @Test
    void shutdownShouldBeNoOp() {
        assertThat(executor.isShutdown()).isFalse();
        assertThatNoException().isThrownBy(executor::shutdown);
        assertThat(executor.isShutdown()).isFalse();
        assertThat(executor.shutdownNow()).isEmpty();
        assertThat(executor.isTerminated()).isFalse();
    }

    @Test
    void awaitTerminationShouldReturnFalseWithoutBlocking() throws Exception {
        long start = System.nanoTime();
        boolean terminated = executor.awaitTermination(50, TimeUnit.MILLISECONDS);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(terminated).isFalse();
        // It must not actually wait — should return promptly since the executor cannot terminate.
        assertThat(elapsedMs).isLessThan(50);
    }

    @Test
    void executorMustStillFunctionAfterShutdownIsCalled() throws Exception {
        executor.shutdown();
        Future<String> future = executor.submit(() -> "still works");
        assertThat(future.get(5, TimeUnit.SECONDS)).isEqualTo("still works");
    }
}
