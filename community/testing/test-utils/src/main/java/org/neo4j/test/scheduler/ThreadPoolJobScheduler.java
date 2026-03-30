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
package org.neo4j.test.scheduler;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.ActiveGroup;
import org.neo4j.scheduler.CallableExecutor;
import org.neo4j.scheduler.CallableExecutorService;
import org.neo4j.scheduler.FailedJobRun;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.MonitoredJobExecutor;
import org.neo4j.scheduler.MonitoredJobInfo;
import org.neo4j.scheduler.SchedulerThreadFactoryFactory;

/**
 * Hybrid test scheduler:
 * - Cached thread pool for immediate one-off jobs
 * - Scheduled thread pool for delayed/recurring jobs
 */
public class ThreadPoolJobScheduler extends LifecycleAdapter implements JobScheduler {
    private final ExecutorService oneOffExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    private final ThreadFactory threadFactory;

    public ThreadPoolJobScheduler() {
        this("ThreadPoolScheduler");
    }

    public ThreadPoolJobScheduler(String prefix) {
        this(
                Executors.newCachedThreadPool(new DaemonThreadFactory(prefix + "-oneoff")),
                Executors.newScheduledThreadPool(
                        Runtime.getRuntime().availableProcessors(), new DaemonThreadFactory(prefix + "-scheduled")));
    }

    public ThreadPoolJobScheduler(ExecutorService oneOffExecutor, ScheduledExecutorService scheduledExecutor) {
        this.oneOffExecutor = oneOffExecutor;
        this.scheduledExecutor = scheduledExecutor;
        this.threadFactory = new DaemonThreadFactory();
    }

    public ThreadPoolJobScheduler(ExecutorService oneOffExecutor) {
        this(oneOffExecutor, null);
    }

    @Override
    public void setTopLevelGroupName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setParallelism(Group group, int parallelism) {
        // no-op
    }

    @Override
    public void setThreadFactory(Group group, SchedulerThreadFactoryFactory threadFactory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableExecutor executor(Group group) {
        return new CallableExecutorService(oneOffExecutor);
    }

    @Override
    public MonitoredJobExecutor monitoredJobExecutor(Group group) {
        return (monitoringParams, command) -> oneOffExecutor.execute(command);
    }

    @Override
    public ThreadFactory threadFactory(Group group) {
        return threadFactory;
    }

    // -------- One-off jobs --------
    @Override
    public <T> JobHandle<T> schedule(Group group, JobMonitoringParams jobMonitoringParams, Callable<T> job) {
        return new FutureJobHandle<>(oneOffExecutor.submit(job));
    }

    @Override
    public JobHandle<?> schedule(Group group, Runnable job) {
        return new FutureJobHandle<>(oneOffExecutor.submit(job));
    }

    @Override
    public JobHandle<?> schedule(Group group, JobMonitoringParams monitoredJobParams, Runnable job) {
        return new FutureJobHandle<>(oneOffExecutor.submit(job));
    }

    // -------- Delayed jobs --------
    @Override
    public JobHandle<?> schedule(Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit) {
        return new FutureJobHandle<>(scheduledExecutor.schedule(runnable, initialDelay, timeUnit));
    }

    @Override
    public JobHandle<?> schedule(
            Group group,
            JobMonitoringParams monitoredJobParams,
            Runnable runnable,
            long initialDelay,
            TimeUnit timeUnit) {
        return new FutureJobHandle<>(scheduledExecutor.schedule(runnable, initialDelay, timeUnit));
    }

    // -------- Recurring jobs --------
    @Override
    public JobHandle<?> scheduleRecurring(Group group, Runnable runnable, long period, TimeUnit timeUnit) {
        throwIfScheduledExecutorIsNull();
        return new FutureJobHandle<>(scheduledExecutor.scheduleAtFixedRate(runnable, period, period, timeUnit));
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group, JobMonitoringParams monitoredJobParams, Runnable runnable, long period, TimeUnit timeUnit) {
        throwIfScheduledExecutorIsNull();
        return new FutureJobHandle<>(scheduledExecutor.scheduleAtFixedRate(runnable, period, period, timeUnit));
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit) {
        throwIfScheduledExecutorIsNull();
        return new FutureJobHandle<>(scheduledExecutor.scheduleAtFixedRate(runnable, initialDelay, period, timeUnit));
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group,
            JobMonitoringParams monitoredJobParams,
            Runnable runnable,
            long initialDelay,
            long period,
            TimeUnit timeUnit) {
        throwIfScheduledExecutorIsNull();
        return new FutureJobHandle<>(scheduledExecutor.scheduleAtFixedRate(runnable, initialDelay, period, timeUnit));
    }

    // -------- Unsupported --------

    @Override
    public Stream<ActiveGroup> activeGroups() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<MonitoredJobInfo> getMonitoredJobs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<FailedJobRun> getFailedJobRuns() {
        throw new UnsupportedOperationException();
    }

    // -------- Lifecycle --------

    @Override
    public void close() {
        shutdown();
    }

    @Override
    public void shutdown() {
        oneOffExecutor.shutdown();
        ensureExecutorShutDown(oneOffExecutor);
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
            ensureExecutorShutDown(scheduledExecutor);
        }
    }

    private void ensureExecutorShutDown(ExecutorService oneOffExecutor) {
        if (isNotShutdown(oneOffExecutor)) {
            oneOffExecutor.shutdownNow();
            if (isNotShutdown(oneOffExecutor)) {
                throw new IllegalStateException("Executors did not shutdown in time.");
            }
        }
    }

    private void throwIfScheduledExecutorIsNull() {
        if (scheduledExecutor == null) {
            throw new IllegalStateException("ScheduledExecutorService cannot be null when scheduling recurring tasks");
        }
    }

    private static boolean isNotShutdown(ExecutorService executor) {
        try {
            return !executor.awaitTermination(20, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        }
    }

    // -------- JobHandle --------
    private static class FutureJobHandle<V> implements JobHandle<V> {
        private final Future<V> future;

        FutureJobHandle(Future<V> future) {
            this.future = future;
        }

        @Override
        public void cancel() {
            future.cancel(false);
        }

        @Override
        public void waitTermination() throws InterruptedException, ExecutionException {
            future.get();
        }

        @Override
        public void waitTermination(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            future.get(timeout, unit);
        }

        @Override
        public V get() throws ExecutionException, InterruptedException {
            return future.get();
        }
    }
}
