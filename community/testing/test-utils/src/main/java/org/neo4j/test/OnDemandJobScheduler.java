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
package org.neo4j.test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.neo4j.scheduler.CallableExecutor;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.test.scheduler.JobSchedulerAdapter;

public class OnDemandJobScheduler extends JobSchedulerAdapter {
    private final List<OnDemandJobHandle<?>> jobs = new CopyOnWriteArrayList<>();

    private final boolean removeJobsAfterExecution;

    public OnDemandJobScheduler() {
        this(true);
    }

    public OnDemandJobScheduler(boolean removeJobsAfterExecution) {
        this.removeJobsAfterExecution = removeJobsAfterExecution;
    }

    @Override
    public CallableExecutor executor(Group group) {
        return new OnDemandExecutor();
    }

    @Override
    public JobHandle<?> schedule(Group group, Runnable job) {
        return schedule(job);
    }

    @Override
    public JobHandle<?> schedule(Group group, JobMonitoringParams jobMonitoringParams, Runnable job) {
        return schedule(job);
    }

    @Override
    public JobHandle<?> schedule(Group group, Runnable job, long initialDelay, TimeUnit timeUnit) {
        return schedule(job);
    }

    @Override
    public JobHandle<?> schedule(
            Group group, JobMonitoringParams jobMonitoringParams, Runnable job, long initialDelay, TimeUnit timeUnit) {
        return schedule(job);
    }

    @Override
    public JobHandle<?> scheduleRecurring(Group group, Runnable runnable, long period, TimeUnit timeUnit) {
        return schedule(runnable);
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group, JobMonitoringParams jobMonitoringParams, Runnable runnable, long period, TimeUnit timeUnit) {
        return schedule(runnable);
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit) {
        return schedule(runnable);
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group,
            JobMonitoringParams jobMonitoringParams,
            Runnable runnable,
            long initialDelay,
            long period,
            TimeUnit timeUnit) {
        return schedule(runnable);
    }

    public Object getJob() {
        if (jobs.isEmpty()) {
            return null;
        } else {
            OnDemandJobHandle<?> job = jobs.get(0);
            return job.callable != null ? job.callable : job.runnable;
        }
    }

    public List<?> getJobs() {
        return jobs;
    }

    public void runJob() {
        for (OnDemandJobHandle job : jobs) {
            job.run();
            if (removeJobsAfterExecution) {
                jobs.remove(job);
            }
        }
    }

    private OnDemandJobHandle<?> schedule(Runnable runnable) {
        OnDemandJobHandle jobHandle = new OnDemandJobHandle(runnable);
        jobs.add(jobHandle);
        return jobHandle;
    }

    private <T> OnDemandJobHandle<T> schedule(Callable<T> callable) {
        OnDemandJobHandle<T> jobHandle = new OnDemandJobHandle<>(callable);
        jobs.add(jobHandle);
        return jobHandle;
    }

    private class OnDemandExecutor implements CallableExecutor {
        @Override
        public <T> Future<T> submit(Callable<T> callable) {
            return schedule(callable);
        }

        @Override
        public void execute(Runnable runnable) {
            schedule(runnable);
        }
    }

    public class OnDemandJobHandle<T> implements JobHandle<T>, Future<T> {
        private Runnable runnable;
        private Callable<T> callable;

        OnDemandJobHandle(Runnable runnable) {
            this.runnable = runnable;
            this.callable = null;
        }

        OnDemandJobHandle(Callable<T> callable) {
            this.runnable = null;
            this.callable = callable;
        }

        /* JobHandle methods */

        @Override
        public void cancel() {
            jobs.remove(this);
        }

        @Override
        public void waitTermination() {
            // on demand
        }

        @Override
        public void waitTermination(long timeout, TimeUnit unit) {
            // on demand
        }

        @Override
        public T get() throws ExecutionException, InterruptedException {
            // on demand
            return null;
        }

        /* Future methods */

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return jobs.remove(this);
        }

        @Override
        public boolean isCancelled() {
            return !jobs.contains(this);
        }

        @Override
        public boolean isDone() {
            return !jobs.contains(this);
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            // on demand
            return null;
        }

        /* Internal methods */

        void run() {
            if (runnable != null) {
                runnable.run();
            }
            if (callable != null) {
                try {
                    callable.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
