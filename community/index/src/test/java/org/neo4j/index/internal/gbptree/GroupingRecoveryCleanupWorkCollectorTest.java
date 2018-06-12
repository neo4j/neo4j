/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.neo4j.scheduler.JobSchedulerAdapter;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GroupingRecoveryCleanupWorkCollectorTest
{
    private final ImmediateJobScheduler jobScheduler = new ImmediateJobScheduler();
    private final GroupingRecoveryCleanupWorkCollector collector =
            new GroupingRecoveryCleanupWorkCollector( jobScheduler );

    @Test
    public void mustNotScheduleAnyJobsBeforeStart() throws Throwable
    {
        // given
        List<DummyJob> allRuns = new ArrayList<>();
        List<DummyJob> expectedJobs = someJobs( allRuns );

        // when
        collector.init();
        addAll( expectedJobs );

        // then
        assertTrue( allRuns.isEmpty() );
    }

    @Test
    public void mustScheduleAllJobs() throws Throwable
    {
        // given
        List<DummyJob> allRuns = new ArrayList<>();
        List<DummyJob> expectedJobs = someJobs( allRuns );

        // when
        collector.init();
        addAll( expectedJobs );
        collector.start();

        // then
        assertSame( expectedJobs, allRuns );
    }

    @Test
    public void mustThrowIfOldJobsDuringInit() throws Throwable
    {
        // given
        List<DummyJob> allRuns = new ArrayList<>();
        List<DummyJob> someJobs = someJobs( allRuns );

        // when
        addAll( someJobs );
        try
        {
            collector.init();
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // then
        }
    }

    @Test
    public void mustCloseOldJobsOnShutdown() throws Throwable
    {
        // given
        List<DummyJob> allRuns = new ArrayList<>();
        List<DummyJob> someJobs = someJobs( allRuns );

        // when
        collector.init();
        addAll( someJobs );
        collector.shutdown();

        // then
        assertTrue( "Expected no jobs to run", allRuns.isEmpty() );
        for ( DummyJob job : someJobs )
        {
            assertTrue( "Expected all jobs to be closed", job.isClosed() );
        }
    }

    @Test
    public void mustNotScheduleOldJobsOnMultipleStart() throws Throwable
    {
        // given
        List<DummyJob> allRuns = new ArrayList<>();
        List<DummyJob> expectedJobs = someJobs( allRuns );

        // when
        collector.init();
        addAll( expectedJobs );
        collector.start();
        collector.start();

        // then
        assertSame( expectedJobs, allRuns );
    }

    @Test
    public void mustNotScheduleOldJobsOnStartStopStart() throws Throwable
    {
        // given
        List<DummyJob> allRuns = new ArrayList<>();
        List<DummyJob> expectedJobs = someJobs( allRuns );

        // when
        collector.init();
        addAll( expectedJobs );
        collector.start();
        collector.stop();
        collector.start();

        // then
        assertSame( expectedJobs, allRuns );
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

    private class ImmediateJobScheduler extends JobSchedulerAdapter
    {
        @Override
        public JobHandle schedule( Group group, Runnable job )
        {
            job.run();
            return super.schedule( group, job );
        }
    }

    private class DummyJob implements CleanupJob
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
        public boolean needed()
        {
            return false;
        }

        @Override
        public boolean hasFailed()
        {
            return false;
        }

        @Override
        public Throwable getCause()
        {
            return null;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public void run()
        {
            allRuns.add( this );
        }

        boolean isClosed()
        {
            return closed;
        }
    }
}
