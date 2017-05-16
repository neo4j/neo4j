/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.LinkedList;
import java.util.List;

import org.neo4j.kernel.impl.util.JobSchedulerAdapter;

import static org.junit.Assert.assertTrue;

public class GroupingRecoveryCleanupWorkCollectorTest
{
    private final RegisteringJobScheduler jobScheduler = new RegisteringJobScheduler();
    private final GroupingRecoveryCleanupWorkCollector collector =
            new GroupingRecoveryCleanupWorkCollector( jobScheduler );

    @Test
    public void mustNotScheduleAnyJobsBeforeStart() throws Throwable
    {
        // given
        List<CleanupJob> expectedJobs = someJobs();

        // when
        collector.init();
        addAll( expectedJobs );

        // then
        assertTrue( jobScheduler.registeredJobs.isEmpty() );
    }

    @Test
    public void mustScheduleAllJobs() throws Throwable
    {
        // given
        List<CleanupJob> expectedJobs = someJobs();

        // when
        collector.init();
        addAll( expectedJobs );
        collector.start();

        // then
        assertSame( expectedJobs, jobScheduler.registeredJobs );
    }

    @Test
    public void mustNotScheduleOldJobsAfterRestart() throws Throwable
    {
        // given
        List<CleanupJob> someJobs = someJobs();

        // when
        collector.init();
        addAll( someJobs );
        collector.init();
        collector.start();

        // then
        assertTrue( jobScheduler.registeredJobs.isEmpty() );
    }

    @Test
    public void mustNotScheduleOldJobsOnMultipleStart() throws Throwable
    {
        // given
        List<CleanupJob> expectedJobs = someJobs();

        // when
        collector.init();
        addAll( expectedJobs );
        collector.start();
        collector.start();

        // then
        assertSame( expectedJobs, jobScheduler.registeredJobs );
    }

    @Test
    public void mustNotScheduleOldJobsOnStartStopStart() throws Throwable
    {
        // given
        List<CleanupJob> expectedJobs = someJobs();

        // when
        collector.init();
        addAll( expectedJobs );
        collector.start();
        collector.stop();
        collector.start();

        // then
        assertSame( expectedJobs, jobScheduler.registeredJobs );
    }

    private void addAll( Collection<CleanupJob> jobs )
    {
        jobs.forEach( collector::add );
    }

    private void assertSame( List<CleanupJob> someJobs, List<Runnable> actual )
    {
        assertTrue( actual.containsAll( someJobs ) );
        assertTrue( someJobs.containsAll( actual ) );
    }

    private List<CleanupJob> someJobs()
    {
        return new ArrayList<>( Arrays.asList(
                new DummyJob( "A" ),
                new DummyJob( "B" ),
                new DummyJob( "C" )
        ) );
    }

    private class RegisteringJobScheduler extends JobSchedulerAdapter
    {
        List<Runnable> registeredJobs = new LinkedList<>();

        @Override
        public JobHandle schedule( Group group, Runnable job )
        {
            registeredJobs.add( job );
            return super.schedule( group, job );
        }
    }

    private class DummyJob implements CleanupJob
    {
        private final String name;

        DummyJob( String name )
        {
            this.name = name;
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
        public Exception getCause()
        {
            return null;
        }

        @Override
        public void run()
        {   // no-op
        }
    }
}