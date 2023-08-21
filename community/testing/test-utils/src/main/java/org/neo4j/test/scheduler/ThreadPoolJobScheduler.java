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

import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
 * Simple test scheduler implementation that is based on a cached thread pool.
 * All threads created by this scheduler can be identified by <i>ThreadPoolScheduler</i> prefix.
 */
public class ThreadPoolJobScheduler extends LifecycleAdapter implements JobScheduler {
    private final ExecutorService executor;
    private final ThreadFactory threadFactory;

    public ThreadPoolJobScheduler() {
        this("ThreadPoolScheduler");
    }

    public ThreadPoolJobScheduler(String prefix) {
        this(newCachedThreadPool(new DaemonThreadFactory(prefix)));
    }

    public ThreadPoolJobScheduler(ExecutorService executor) {
        this.executor = executor;
        this.threadFactory = new DaemonThreadFactory();
    }

    @Override
    public void setTopLevelGroupName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setParallelism(Group group, int parallelism) {
        // has no effect
    }

    @Override
    public void setThreadFactory(Group group, SchedulerThreadFactoryFactory threadFactory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableExecutor executor(Group group) {
        return new CallableExecutorService(executor);
    }

    @Override
    public MonitoredJobExecutor monitoredJobExecutor(Group group) {
        return (monitoringParams, command) -> executor.execute(command);
    }

    @Override
    public ThreadFactory threadFactory(Group group) {
        return threadFactory;
    }

    @Override
    public <T> JobHandle<T> schedule(Group group, JobMonitoringParams jobMonitoringParams, Callable<T> job) {
        return new FutureJobHandle<>(executor.submit(job));
    }

    @Override
    public JobHandle<?> schedule(Group group, Runnable job) {
        return new FutureJobHandle<>(executor.submit(job));
    }

    @Override
    public JobHandle<?> schedule(Group group, JobMonitoringParams monitoredJobParams, Runnable job) {
        return new FutureJobHandle<>(executor.submit(job));
    }

    @Override
    public JobHandle<?> schedule(Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle<?> schedule(
            Group group,
            JobMonitoringParams monitoredJobParams,
            Runnable runnable,
            long initialDelay,
            TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle<?> scheduleRecurring(Group group, Runnable runnable, long period, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group, JobMonitoringParams monitoredJobParams, Runnable runnable, long period, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group,
            JobMonitoringParams monitoredJobParams,
            Runnable runnable,
            long initialDelay,
            long period,
            TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

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

    @Override
    public void close() {
        shutdown();
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        if (isNotShutdown(executor)) {
            executor.shutdownNow();
            if (isNotShutdown(executor)) {
                throw new IllegalStateException("Executor did not shutdown in time: " + executor);
            }
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
