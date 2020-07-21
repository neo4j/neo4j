/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.scheduler;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.impl.scheduler.ThreadPool.ThreadPoolParameters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.resources.Profiler;
import org.neo4j.scheduler.ActiveGroup;
import org.neo4j.scheduler.CancelListener;
import org.neo4j.scheduler.CallableExecutor;
import org.neo4j.scheduler.CallableExecutorService;
import org.neo4j.scheduler.FailedJobRun;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.MonitoredJobExecutor;
import org.neo4j.scheduler.MonitoredJobInfo;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.SchedulerThreadFactoryFactory;
import org.neo4j.time.SystemNanoClock;

public class CentralJobScheduler extends LifecycleAdapter implements JobScheduler, AutoCloseable
{
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();

    private final TimeBasedTaskScheduler scheduler;
    private final Thread schedulerThread;
    private final TopLevelGroup topLevelGroup;
    private final ThreadPoolManager pools;
    private final ConcurrentHashMap<Group,ThreadPoolParameters> extraParameters;
    private final FailedJobRunsStore failedJobRunsStore;

    private volatile boolean started;

    private static class TopLevelGroup extends ThreadGroup
    {
        TopLevelGroup()
        {
            super( "Neo4j-" + INSTANCE_COUNTER.incrementAndGet() );
        }

        public void setName( String name ) throws Exception
        {
            Field field = ThreadGroup.class.getDeclaredField( "name" );
            field.setAccessible( true );
            field.set( this, name );
        }
    }

    protected CentralJobScheduler( SystemNanoClock clock )
    {
        topLevelGroup = new TopLevelGroup();
        this.failedJobRunsStore = new FailedJobRunsStore( 100 );
        var jobIdCounter = new AtomicLong();
        pools = new ThreadPoolManager( topLevelGroup, clock, failedJobRunsStore, jobIdCounter::incrementAndGet );
        scheduler = new TimeBasedTaskScheduler( clock, pools, failedJobRunsStore, jobIdCounter::incrementAndGet );
        extraParameters = new ConcurrentHashMap<>();

        // The scheduler thread runs at slightly elevated priority for timeliness, and is started in init().
        ThreadFactory threadFactory = new GroupedDaemonThreadFactory( Group.TASK_SCHEDULER, topLevelGroup );
        schedulerThread = threadFactory.newThread( scheduler );
        int priority = Thread.NORM_PRIORITY + 1;
        schedulerThread.setPriority( priority );
    }

    @Override
    public void setTopLevelGroupName( String name )
    {
        try
        {
            topLevelGroup.setName( name );
        }
        catch ( Exception ignore )
        {
        }
    }

    @Override
    public void setParallelism( Group group, int parallelism )
    {
        pools.assumeNotStarted( group );
        extraParameters.computeIfAbsent( group, g -> new ThreadPoolParameters() ).desiredParallelism = parallelism;
    }

    @Override
    public void setThreadFactory( Group group, SchedulerThreadFactoryFactory threadFactory )
    {
        pools.assumeNotStarted( group );
        extraParameters.computeIfAbsent( group, g -> new ThreadPoolParameters() ).providedThreadFactory = threadFactory;
    }

    @Override
    public void init()
    {
        if ( !started )
        {
            schedulerThread.start();
            started = true;
        }
    }

    @Override
    public CallableExecutor executor( Group group )
    {
        return new CallableExecutorService( getThreadPool( group ).getExecutorService() );
    }

    @Override
    public MonitoredJobExecutor monitoredJobExecutor( Group group )
    {
        var threadPool = getThreadPool( group );
        return threadPool::submit;
    }

    @Override
    public ThreadFactory threadFactory( Group group )
    {
        return getThreadPool( group ).getThreadFactory();
    }

    private ThreadPool getThreadPool( Group group )
    {
        return pools.getThreadPool( group, extraParameters.get( group ) );
    }

    @Override
    public <T> JobHandle<T> schedule( Group group, JobMonitoringParams jobMonitoringParams, Callable<T> job )
    {
        if ( !started )
        {
            throw new RejectedExecutionException( "Scheduler is not started" );
        }
        return tryRegisterCancelListener( job, getThreadPool( group ).submit( jobMonitoringParams, job ) );
    }

    @Override
    public JobHandle<?> schedule( Group group, Runnable job )
    {
        return schedule( group, JobMonitoringParams.NOT_MONITORED, job );
    }

    @Override
    public JobHandle<?> schedule( Group group, JobMonitoringParams jobMonitoringParams, Runnable job )
    {
        if ( !started )
        {
            throw new RejectedExecutionException( "Scheduler is not started" );
        }
        return tryRegisterCancelListener( job, getThreadPool( group ).submit( jobMonitoringParams, job ) );
    }

    @Override
    public JobHandle<?> scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit )
    {
        return scheduleRecurring( group, JobMonitoringParams.NOT_MONITORED, runnable, period, timeUnit );
    }

    @Override
    public JobHandle<?> scheduleRecurring( Group group, JobMonitoringParams monitoredJobParams, Runnable runnable, long period, TimeUnit timeUnit )
    {
        return scheduleRecurring( group, monitoredJobParams, runnable, 0, period, timeUnit );
    }

    @Override
    public JobHandle<?> scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit unit )
    {
        return scheduleRecurring( group, JobMonitoringParams.NOT_MONITORED, runnable, initialDelay, period, unit );
    }

    @Override
    public JobHandle<?> scheduleRecurring( Group group, JobMonitoringParams monitoredJobParams, Runnable runnable, long initialDelay, long period,
            TimeUnit timeUnit )
    {
        return tryRegisterCancelListener( runnable,
                scheduler.submit( group, monitoredJobParams, runnable, timeUnit.toNanos( initialDelay ), timeUnit.toNanos( period ) ) );
    }

    @Override
    public Stream<ActiveGroup> activeGroups()
    {
        List<ActiveGroup> groups = new ArrayList<>();
        pools.forEachStarted( ( group, pool ) ->
        {
            int activeThreadCount = pool.activeThreadCount();
            if ( activeThreadCount > 0 )
            {
                groups.add( new ActiveGroup( group, activeThreadCount ) );
            }
        } );
        return groups.stream();
    }

    @Override
    public void profileGroup( Group group, Profiler profiler )
    {
        if ( !pools.isStarted( group ) )
        {
            return; // Don't bother profiling groups that hasn't been started.
        }
        getThreadPool( group ).activeThreads().forEach( profiler::profile );
    }

    @Override
    public JobHandle<?> schedule( Group group, Runnable runnable, long initialDelay, TimeUnit unit )
    {
        return schedule( group, JobMonitoringParams.NOT_MONITORED, runnable, initialDelay, unit );
    }

    @Override
    public JobHandle<?> schedule( Group group, JobMonitoringParams monitoredJobParams, Runnable runnable, long initialDelay, TimeUnit timeUnit )
    {
        return tryRegisterCancelListener( runnable, scheduler.submit( group, monitoredJobParams, runnable, timeUnit.toNanos( initialDelay ), 0 ) );
    }

    @Override
    public List<MonitoredJobInfo> getMonitoredJobs()
    {
        List<MonitoredJobInfo> monitoredJobInfos = new ArrayList<>( scheduler.getMonitoredJobs() );

        pools.forEachStarted( ( group, pool ) -> monitoredJobInfos.addAll( pool.getMonitoredJobs() ) );

        return monitoredJobInfos;
    }

    @Override
    public List<FailedJobRun> getFailedJobRuns()
    {
        return failedJobRunsStore.getFailedJobRuns();
    }

    @Override
    public void shutdown()
    {
        started = false;

        // First shut down the scheduler, so no new tasks are queued up in the pools.
        InterruptedException exception = shutDownScheduler();

        // Then shut down the thread pools. This involves cancelling jobs which hasn't been cancelled already,
        // so we avoid having to wait the full maximum wait time on the executor service shut-downs.
        exception = Exceptions.chain( exception, pools.shutDownAll() );

        if ( exception != null )
        {
            throw new RuntimeException( "Unable to shut down job scheduler properly.", exception );
        }
    }

    @Override
    public void close()
    {
        shutdown();
    }

    private InterruptedException shutDownScheduler()
    {
        scheduler.stop();
        try
        {
            schedulerThread.join();
        }
        catch ( InterruptedException e )
        {
            return e;
        }
        return null;
    }

    private <T> JobHandle<T> tryRegisterCancelListener( Object maybeCancelListener, JobHandle<T> handle )
    {
        if ( maybeCancelListener instanceof CancelListener )
        {
            handle.registerCancelListener( (CancelListener) maybeCancelListener );
        }
        return handle;
    }
}
