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
package org.neo4j.test;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.FakeClock;

/**
 * N.B - Do not use this with time resolutions of less than 1 ms!
 */
public class FakeClockJobScheduler extends FakeClock implements JobScheduler
{
    private final AtomicLong jobIdGen = new AtomicLong();
    private final Collection<JobHandle> jobs = new CopyOnWriteArrayList<>();

    public FakeClockJobScheduler()
    {
        super();
    }

    private JobHandle schedule( Runnable job, long firstDeadline )
    {
        JobHandle jobHandle = new JobHandle( job, firstDeadline, 0 );
        jobs.add( jobHandle );
        return jobHandle;
    }

    private JobHandle scheduleRecurring( Runnable job, long firstDeadline, long period )
    {
        JobHandle jobHandle = new JobHandle( job, firstDeadline, period );
        jobs.add( jobHandle );
        return jobHandle;
    }

    @Override
    public FakeClock forward( long delta, TimeUnit unit )
    {
        super.forward( delta, unit );
        processSchedule();
        return this;
    }

    private void processSchedule()
    {
        boolean anyTriggered;
        do
        {
            anyTriggered = false;
            for ( JobHandle job : jobs )
            {
                if ( job.tryTrigger() )
                {
                    anyTriggered = true;
                }
            }
        }
        while ( anyTriggered );
    }

    private long now()
    {
        return instant().toEpochMilli();
    }

    @Override
    public void setTopLevelGroupName( String name )
    {
    }

    @Override
    public Executor executor( Group group )
    {
        return job -> schedule( job, now() );
    }

    @Override
    public ExecutorService workStealingExecutor( Group group, int parallelism )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ThreadFactory threadFactory( Group group )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        JobHandle handle = schedule( job, now() );
        processSchedule();
        return handle;
    }

    @Override
    public JobHandle schedule( Group group, Runnable job, long initialDelay, TimeUnit timeUnit )
    {
        JobHandle handle = schedule( job, now() + timeUnit.toMillis( initialDelay ) );
        if ( initialDelay <= 0 )
        {
            processSchedule();
        }
        return handle;
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable job, long period, TimeUnit timeUnit )
    {
        JobHandle handle = scheduleRecurring( job, now(), timeUnit.toMillis( period ) );
        processSchedule();
        return handle;
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable job, long initialDelay, long period, TimeUnit timeUnit )
    {
        JobHandle handle = scheduleRecurring( job, now() + timeUnit.toMillis( initialDelay ), timeUnit.toMillis( period ) );
        if ( initialDelay <= 0 )
        {
            processSchedule();
        }
        return handle;
    }

    @Override
    public void init()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown()
    {
        throw new UnsupportedOperationException();
    }

    class JobHandle implements JobScheduler.JobHandle
    {
        private final long id = jobIdGen.incrementAndGet();
        private final Runnable runnable;
        private final long period;

        private long deadline;

        JobHandle( Runnable runnable, long firstDeadline, long period )
        {
            this.runnable = runnable;
            this.deadline = firstDeadline;
            this.period = period;
        }

        boolean tryTrigger()
        {
            if ( now() >= deadline )
            {
                runnable.run();
                if ( period != 0 )
                {
                    deadline += period;
                }
                else
                {
                    jobs.remove( this );
                }
                return true;
            }
            return false;
        }

        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {
            jobs.remove( this );
        }

        @Override
        public void waitTermination()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            JobHandle jobHandle = (JobHandle) o;
            return id == jobHandle.id;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( id );
        }
    }
}
