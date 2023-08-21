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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.neo4j.scheduler.ActiveGroup;
import org.neo4j.scheduler.CallableExecutor;
import org.neo4j.scheduler.FailedJobRun;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.MonitoredJobExecutor;
import org.neo4j.scheduler.MonitoredJobInfo;
import org.neo4j.scheduler.SchedulerThreadFactoryFactory;
import org.neo4j.time.FakeClock;

/**
 * N.B - Do not use this with time resolutions of less than 1 ms!
 */
public class FakeClockJobScheduler extends FakeClock implements JobScheduler {
    private final AtomicLong jobIdGen = new AtomicLong();
    private final Collection<JobTrigger> jobs = new CopyOnWriteArrayList<>();

    public FakeClockJobScheduler() {
        super();
    }

    private <V> JobTrigger<V> schedule(Callable<V> job, long firstDeadline) {
        JobTrigger<V> jobTrigger = new JobTrigger<>(job, firstDeadline, 0);
        jobs.add(jobTrigger);
        return jobTrigger;
    }

    private JobTrigger schedule(Runnable job, long firstDeadline) {
        JobTrigger jobTrigger = new JobTrigger(job, firstDeadline, 0);
        jobs.add(jobTrigger);
        return jobTrigger;
    }

    private JobTrigger scheduleRecurring(Runnable job, long firstDeadline, long period) {
        JobTrigger jobTrigger = new JobTrigger(job, firstDeadline, period);
        jobs.add(jobTrigger);
        return jobTrigger;
    }

    @Override
    public FakeClock forward(long delta, TimeUnit unit) {
        super.forward(delta, unit);
        processSchedule();
        return this;
    }

    private void processSchedule() {
        boolean anyTriggered;
        do {
            anyTriggered = false;
            for (JobTrigger job : jobs) {
                if (job.tryTrigger()) {
                    anyTriggered = true;
                }
            }
        } while (anyTriggered);
    }

    private long now() {
        return instant().toEpochMilli();
    }

    @Override
    public void setTopLevelGroupName(String name) {}

    @Override
    public void setParallelism(Group group, int parallelism) {}

    @Override
    public void setThreadFactory(Group group, SchedulerThreadFactoryFactory threadFactory) {}

    @Override
    public CallableExecutor executor(Group group) {
        return new FakeClockExecutor();
    }

    @Override
    public MonitoredJobExecutor monitoredJobExecutor(Group group) {
        var executor = new FakeClockExecutor();
        return (monitoringParams, command) -> executor.execute(command);
    }

    @Override
    public ThreadFactory threadFactory(Group group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> JobHandle<T> schedule(Group group, JobMonitoringParams jobMonitoringParams, Callable<T> job) {
        JobTrigger<T> handle = schedule(job, now());
        processSchedule();
        return handle;
    }

    @Override
    public JobHandle<?> schedule(Group group, Runnable job) {
        JobTrigger handle = schedule(job, now());
        processSchedule();
        return handle;
    }

    @Override
    public JobHandle<?> schedule(Group group, JobMonitoringParams monitoredJobParams, Runnable job) {
        return schedule(group, job);
    }

    @Override
    public JobHandle<?> schedule(Group group, Runnable job, long initialDelay, TimeUnit timeUnit) {
        JobTrigger handle = schedule(job, now() + timeUnit.toMillis(initialDelay));
        if (initialDelay <= 0) {
            processSchedule();
        }
        return handle;
    }

    @Override
    public JobHandle<?> schedule(
            Group group, JobMonitoringParams monitoredJobParams, Runnable job, long initialDelay, TimeUnit timeUnit) {
        return schedule(group, job, initialDelay, timeUnit);
    }

    @Override
    public JobHandle<?> scheduleRecurring(Group group, Runnable job, long period, TimeUnit timeUnit) {
        JobTrigger handle = scheduleRecurring(job, now(), timeUnit.toMillis(period));
        processSchedule();
        return handle;
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group, JobMonitoringParams monitoredJobParams, Runnable job, long period, TimeUnit timeUnit) {
        return scheduleRecurring(group, job, period, timeUnit);
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group, Runnable job, long initialDelay, long period, TimeUnit timeUnit) {
        JobTrigger handle = scheduleRecurring(job, now() + timeUnit.toMillis(initialDelay), timeUnit.toMillis(period));
        if (initialDelay <= 0) {
            processSchedule();
        }
        return handle;
    }

    @Override
    public JobHandle<?> scheduleRecurring(
            Group group,
            JobMonitoringParams monitoredJobParams,
            Runnable job,
            long initialDelay,
            long period,
            TimeUnit timeUnit) {
        return scheduleRecurring(group, job, initialDelay, period, timeUnit);
    }

    @Override
    public Stream<ActiveGroup> activeGroups() {
        return Stream.empty();
    }

    @Override
    public List<MonitoredJobInfo> getMonitoredJobs() {
        return List.of();
    }

    @Override
    public List<FailedJobRun> getFailedJobRuns() {
        return List.of();
    }

    @Override
    public void init() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        shutdown();
    }

    class JobTrigger<T> implements JobHandle<T>, Future<T> {
        private final long id = jobIdGen.incrementAndGet();
        private final Runnable runnable;
        private final Callable<T> callable;
        private final long period;

        private long deadline;

        JobTrigger(Callable<T> callable, long firstDeadline, long period) {
            this.runnable = null;
            this.callable = callable;
            this.deadline = firstDeadline;
            this.period = period;
        }

        JobTrigger(Runnable runnable, long firstDeadline, long period) {
            this.runnable = runnable;
            this.callable = null;
            this.deadline = firstDeadline;
            this.period = period;
        }

        boolean tryTrigger() {
            if (now() >= deadline) {
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
                if (period != 0) {
                    deadline += period;
                } else {
                    jobs.remove(this);
                }
                return true;
            }
            return false;
        }

        @Override
        public void cancel() {
            cancel(false);
        }

        @Override
        public void waitTermination() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void waitTermination(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

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
            throw new UnsupportedOperationException();
        }

        @Override
        public T get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public T get(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JobTrigger jobTrigger = (JobTrigger) o;
            return id == jobTrigger.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private class FakeClockExecutor implements CallableExecutor {
        @Override
        public <T> Future<T> submit(Callable<T> callable) {
            return schedule(callable, now());
        }

        @Override
        public void execute(Runnable command) {
            schedule(command, now());
        }
    }
}
