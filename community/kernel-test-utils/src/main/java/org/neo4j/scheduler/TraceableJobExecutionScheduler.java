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
package org.neo4j.scheduler;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;

class TraceableJobExecutionScheduler extends JobSchedulerExtension {
    @Override
    protected JobScheduler createField(ExtensionContext context) {
        return new TestJobScheduler(super.createField(context));
    }

    private static class TestJobScheduler implements JobScheduler {

        private final JobScheduler originalScheduler;

        TestJobScheduler(JobScheduler originalScheduler) {
            this.originalScheduler = originalScheduler;
        }

        @Override
        public void init() throws Exception {
            originalScheduler.init();
        }

        @Override
        public void start() throws Exception {
            originalScheduler.start();
        }

        @Override
        public void stop() throws Exception {
            originalScheduler.stop();
        }

        @Override
        public void shutdown() throws Exception {
            originalScheduler.shutdown();
        }

        @Override
        public void setTopLevelGroupName(String name) {
            originalScheduler.setTopLevelGroupName(name);
        }

        @Override
        public void setParallelism(Group group, int parallelism) {
            originalScheduler.setParallelism(group, parallelism);
        }

        @Override
        public void setThreadFactory(Group group, SchedulerThreadFactoryFactory threadFactory) {
            originalScheduler.setThreadFactory(group, threadFactory);
        }

        @Override
        public CallableExecutor executor(Group group) {
            return originalScheduler.executor(group);
        }

        @Override
        public MonitoredJobExecutor monitoredJobExecutor(Group group) {
            return originalScheduler.monitoredJobExecutor(group);
        }

        @Override
        public ThreadFactory threadFactory(Group group) {
            return originalScheduler.threadFactory(group);
        }

        @Override
        public <T> JobHandle<T> schedule(Group group, JobMonitoringParams jobMonitoringParams, Callable<T> job) {
            return originalScheduler.schedule(group, jobMonitoringParams, job);
        }

        @Override
        public JobHandle<?> schedule(Group group, Runnable job) {
            return originalScheduler.schedule(group, job);
        }

        @Override
        public JobHandle<?> schedule(Group group, JobMonitoringParams monitoredJobParams, Runnable job) {
            return originalScheduler.schedule(group, monitoredJobParams, job);
        }

        @Override
        public JobHandle<?> schedule(Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit) {
            return originalScheduler.schedule(group, runnable, initialDelay, timeUnit);
        }

        @Override
        public JobHandle<?> schedule(
                Group group,
                JobMonitoringParams monitoredJobParams,
                Runnable runnable,
                long initialDelay,
                TimeUnit timeUnit) {
            return originalScheduler.schedule(group, monitoredJobParams, runnable, initialDelay, timeUnit);
        }

        @Override
        public JobHandle<?> scheduleRecurring(Group group, Runnable runnable, long period, TimeUnit timeUnit) {
            return originalScheduler.scheduleRecurring(group, runnable, period, timeUnit);
        }

        @Override
        public JobHandle<?> scheduleRecurring(
                Group group,
                JobMonitoringParams monitoredJobParams,
                Runnable runnable,
                long period,
                TimeUnit timeUnit) {
            return originalScheduler.scheduleRecurring(group, monitoredJobParams, runnable, period, timeUnit);
        }

        @Override
        public JobHandle<?> scheduleRecurring(
                Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit) {
            return originalScheduler.scheduleRecurring(group, runnable, initialDelay, period, timeUnit);
        }

        @Override
        public JobHandle<?> scheduleRecurring(
                Group group,
                JobMonitoringParams monitoredJobParams,
                Runnable runnable,
                long initialDelay,
                long period,
                TimeUnit timeUnit) {
            return originalScheduler.scheduleRecurring(
                    group, monitoredJobParams, runnable, initialDelay, period, timeUnit);
        }

        @Override
        public Stream<ActiveGroup> activeGroups() {
            return originalScheduler.activeGroups();
        }

        @Override
        public List<MonitoredJobInfo> getMonitoredJobs() {
            return originalScheduler.getMonitoredJobs();
        }

        @Override
        public List<FailedJobRun> getFailedJobRuns() {
            return originalScheduler.getFailedJobRuns();
        }

        @Override
        public void close() throws Exception {
            originalScheduler.close();
            throw new RuntimeException("Shutdown called.");
        }
    }
}
