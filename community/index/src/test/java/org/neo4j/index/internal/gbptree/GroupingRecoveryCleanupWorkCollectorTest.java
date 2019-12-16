/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.test.scheduler.JobSchedulerAdapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroupingRecoveryCleanupWorkCollectorTest
{
    private static final Group GROUP = Group.STORAGE_MAINTENANCE;
    private final SingleGroupJobScheduler jobScheduler = new SingleGroupJobScheduler( GROUP );
    private final GroupingRecoveryCleanupWorkCollector collector = new GroupingRecoveryCleanupWorkCollector( jobScheduler, GROUP );

    @Test
    void shouldNotAcceptJobsAfterStart()
    {
        // given
        collector.init();
        collector.start();

        // when/then
        assertThrows( IllegalStateException.class, () -> collector.add( new DummyJob( "A", new ArrayList<>() ) ) );
    }

    @Test
    void shouldRunAllJobsBeforeOrDuringShutdown() throws Exception
    {
        // given
        List<DummyJob> allRuns = new ArrayList<>();
        List<DummyJob> expectedJobs = someJobs( allRuns );
        collector.init();

        // when
        addAll( expectedJobs );
        collector.start();
        collector.shutdown();

        // then
        assertEquals( allRuns, expectedJobs );
    }

    @Test
    void mustThrowIfStartedMultipleTimes() throws ExecutionException, InterruptedException
    {
        // given
        List<DummyJob> allRuns = new ArrayList<>();
        List<DummyJob> someJobs = someJobs( allRuns );
        addAll( someJobs );
        collector.start();

        // when
        collector.shutdown();
        assertThrows( IllegalStateException.class, collector::start );

        // then
        collector.shutdown();
    }

    @Test
    void mustCloseOldJobsOnShutdown() throws ExecutionException, InterruptedException
    {
        // given
        List<DummyJob> allRuns = new ArrayList<>();
        List<DummyJob> someJobs = someJobs( allRuns );

        // when
        collector.init();
        addAll( someJobs );
        collector.shutdown();

        // then
        for ( DummyJob job : someJobs )
        {
            assertTrue( job.isClosed(), "Expected all jobs to be closed" );
        }
    }

    @Test
    void shouldExecuteAllTheJobsWhenSeparateJobFails() throws Exception
    {
        List<DummyJob> allRuns = new ArrayList<>();

        DummyJob firstJob = new DummyJob( "first", allRuns );
        DummyJob thirdJob = new DummyJob( "third", allRuns );
        DummyJob fourthJob = new DummyJob( "fourth", allRuns );
        List<DummyJob> expectedJobs = Arrays.asList( firstJob, thirdJob, fourthJob );
        collector.init();

        collector.add( firstJob );
        collector.add( new EvilJob() );
        collector.add( thirdJob );
        collector.add( fourthJob );

        collector.start();
        collector.shutdown();

        assertSame( expectedJobs, allRuns );
    }

    @Test
    void throwOnAddingJobsAfterStart()
    {
        collector.init();
        collector.start();

        assertThrows( IllegalStateException.class, () -> collector.add( new DummyJob( "first", new ArrayList<>() ) ) );
    }

    @Test
    void shouldScheduleCleanupTasksToJobScheduler() throws ExecutionException, InterruptedException
    {
        TrackingJob job = new TrackingJob();
        collector.init();
        collector.add( job );
        collector.start();
        collector.shutdown();

        assertThat( job.targetExecutor ).isNotNull();
        assertThat( job.targetExecutor ).isSameAs( jobScheduler.executor( GROUP ) );
    }

    private void addAll( Collection<DummyJob> jobs )
    {
        jobs.forEach( collector::add );
    }

    private void assertSame( List<DummyJob> someJobs, List<DummyJob> actual )
    {
        assertTrue( actual.containsAll( someJobs ) );
        assertTrue( someJobs.containsAll( actual ) );
    }

    private List<DummyJob> someJobs( List<DummyJob> allRuns )
    {
        return new ArrayList<>( Arrays.asList(
                new DummyJob( "A", allRuns ),
                new DummyJob( "B", allRuns ),
                new DummyJob( "C", allRuns )
        ) );
    }

    private static class SingleGroupJobScheduler extends JobSchedulerAdapter
    {
        private final ExecutorService executorService = Executors.newSingleThreadExecutor();
        private final Group group;

        SingleGroupJobScheduler( Group group )
        {
            this.group = group;
        }

        @Override
        public Executor executor( Group group )
        {
            assertGroup( group );
            return executorService;
        }

        @Override
        public JobHandle<?> schedule( Group group, Runnable job )
        {
            assertGroup( group );
            Future<?> future = executorService.submit( job );
            return new JobHandle<>()
            {
                @Override
                public void cancel()
                {
                    future.cancel( false );
                }

                @Override
                public void waitTermination() throws InterruptedException, ExecutionException, CancellationException
                {
                    future.get();
                }

                @Override
                public void waitTermination( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException
                {
                    future.get( timeout, unit );
                }

                @Override
                public Object get() throws ExecutionException, InterruptedException
                {
                    return future.get();
                }
            };
        }

        private void assertGroup( Group group )
        {
            assertThat( group ).as( "use only target group" ).isSameAs( this.group );
        }

        @Override
        public void shutdown()
        {
            executorService.shutdown();
        }
    }

    private static class EvilJob extends CleanupJob.Adaptor
    {
        @Override
        public void run( Executor executor )
        {
            throw new RuntimeException( "Resilient to run attempts" );
        }
    }

    private static class DummyJob extends CleanupJob.Adaptor
    {
        private final String name;
        private final List<DummyJob> allRuns;
        private boolean closed;

        DummyJob( String name, List<DummyJob> allRuns )
        {
            this.name = name;
            this.allRuns = allRuns;
        }

        @Override
        public String toString()
        {
            return name;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public void run( Executor executor )
        {
            allRuns.add( this );
        }

        public boolean isClosed()
        {
            return closed;
        }
    }

    private static class TrackingJob extends CleanupJob.Adaptor
    {
        private Executor targetExecutor;

        @Override
        public void run( Executor executor )
        {
            targetExecutor = executor;
        }
    }
}
