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
package org.neo4j.index.internal.gbptree;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.MonitoredJobExecutor;
import org.neo4j.test.scheduler.JobSchedulerAdapter;

class GroupingRecoveryCleanupWorkCollectorTest {
    private static final Group GROUP = Group.INDEX_CLEANUP;
    private static final Group WORK_GROUP = Group.INDEX_CLEANUP_WORK;
    private final SingleGroupJobScheduler jobScheduler = new SingleGroupJobScheduler(GROUP, WORK_GROUP);
    private final GroupingRecoveryCleanupWorkCollector collector =
            new GroupingRecoveryCleanupWorkCollector(jobScheduler, GROUP, WORK_GROUP, "test db");

    @Test
    void shouldNotAcceptJobsAfterStart() {
        // given
        collector.init();
        collector.start();

        // when/then
        assertThrows(IllegalStateException.class, () -> collector.add(new DummyJob("A", new ArrayList<>())));
    }

    @Test
    void shouldRunAllJobsBeforeOrDuringStop() throws Exception {
        // given
        List<DummyJob> allRuns = new CopyOnWriteArrayList<>();
        List<DummyJob> expectedJobs = someJobs(allRuns);
        collector.init();

        // when
        addAll(expectedJobs);
        collector.start();
        collector.stop();

        // then
        assertThat(allRuns).containsExactlyInAnyOrderElementsOf(expectedJobs);
    }

    @Test
    void mustThrowIfStartedMultipleTimes() throws Exception {
        // given
        List<DummyJob> allRuns = new CopyOnWriteArrayList<>();
        List<DummyJob> someJobs = someJobs(allRuns);
        addAll(someJobs);
        collector.start();

        // when
        collector.stop();
        assertThrows(IllegalStateException.class, collector::start);

        // then
        collector.stop();
    }

    @Test
    void mustCloseOldJobsOnStop() throws Exception {
        // given
        List<DummyJob> allRuns = new CopyOnWriteArrayList<>();
        List<DummyJob> someJobs = someJobs(allRuns);

        // when
        collector.init();
        addAll(someJobs);
        collector.stop();

        // then
        for (DummyJob job : someJobs) {
            assertTrue(job.isClosed(), "Expected all jobs to be closed");
        }
    }

    @Test
    void shouldExecuteAllTheJobsWhenSeparateJobFails() throws Exception {
        List<DummyJob> allRuns = new CopyOnWriteArrayList<>();

        DummyJob firstJob = new DummyJob("first", allRuns);
        DummyJob thirdJob = new DummyJob("third", allRuns);
        DummyJob fourthJob = new DummyJob("fourth", allRuns);
        List<DummyJob> expectedJobs = Arrays.asList(firstJob, thirdJob, fourthJob);
        collector.init();

        collector.add(firstJob);
        collector.add(new EvilJob());
        collector.add(thirdJob);
        collector.add(fourthJob);

        collector.start();
        collector.stop();

        assertThat(allRuns).containsExactlyInAnyOrderElementsOf(expectedJobs);
    }

    @Test
    void throwOnAddingJobsAfterStart() {
        collector.init();
        collector.start();

        assertThrows(IllegalStateException.class, () -> collector.add(new DummyJob("first", new ArrayList<>())));
    }

    private void addAll(Collection<DummyJob> jobs) {
        jobs.forEach(collector::add);
    }

    private static List<DummyJob> someJobs(List<DummyJob> allRuns) {
        return new ArrayList<>(
                Arrays.asList(new DummyJob("A", allRuns), new DummyJob("B", allRuns), new DummyJob("C", allRuns)));
    }

    private static class SingleGroupJobScheduler extends JobSchedulerAdapter {
        private final ExecutorService executorService = Executors.newSingleThreadExecutor();
        private final Group mainGroup;
        private final Group workGroup;

        SingleGroupJobScheduler(Group mainGroup, Group workGroup) {
            this.mainGroup = mainGroup;
            this.workGroup = workGroup;
        }

        @Override
        public MonitoredJobExecutor monitoredJobExecutor(Group group) {
            assertGroup(group, workGroup);
            return (monitoringParams, job) -> executorService.submit(job);
        }

        @Override
        public JobHandle<?> schedule(Group group, JobMonitoringParams jobMonitoringParams, Runnable job) {
            assertGroup(group, mainGroup);
            Future<?> future = executorService.submit(job);
            return new JobHandle<>() {
                @Override
                public void cancel() {
                    future.cancel(false);
                }

                @Override
                public void waitTermination() throws InterruptedException, ExecutionException, CancellationException {
                    future.get();
                }

                @Override
                public void waitTermination(long timeout, TimeUnit unit)
                        throws InterruptedException, ExecutionException, TimeoutException {
                    future.get(timeout, unit);
                }

                @Override
                public Object get() throws ExecutionException, InterruptedException {
                    return future.get();
                }
            };
        }

        private static void assertGroup(Group group, Group expectedGroup) {
            assertThat(group).as("use only target group").isSameAs(expectedGroup);
        }

        @Override
        public void shutdown() {
            executorService.shutdown();
        }
    }

    private static class EvilJob extends CleanupJob.Adaptor {
        @Override
        public void run(Executor executor) {
            throw new RuntimeException("Resilient to run attempts");
        }
    }

    private static class DummyJob extends CleanupJob.Adaptor {
        private final String name;
        private final List<DummyJob> allRuns;
        private boolean closed;

        DummyJob(String name, List<DummyJob> allRuns) {
            this.name = name;
            this.allRuns = allRuns;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public void run(Executor executor) {
            allRuns.add(this);
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
