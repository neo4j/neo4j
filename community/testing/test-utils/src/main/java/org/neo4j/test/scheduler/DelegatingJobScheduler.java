package org.neo4j.test.scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.resources.Profiler;
import org.neo4j.scheduler.ActiveGroup;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.SchedulerThreadFactoryFactory;

public class DelegatingJobScheduler implements JobScheduler
{
    private final JobScheduler delegate;

    public DelegatingJobScheduler( JobScheduler delegate )
    {
        this.delegate = delegate;
    }

    public void setTopLevelGroupName( String name )
    {
        delegate.setTopLevelGroupName( name );
    }

    public void setParallelism( Group group, int parallelism )
    {
        delegate.setParallelism( group, parallelism );
    }

    public void setThreadFactory( Group group, SchedulerThreadFactoryFactory threadFactory )
    {
        delegate.setThreadFactory( group, threadFactory );
    }

    public Executor executor( Group group )
    {
        return delegate.executor( group );
    }

    public ThreadFactory threadFactory( Group group )
    {
        return delegate.threadFactory( group );
    }

    public <T> JobHandle<T> schedule( Group group, Callable<T> job )
    {
        return delegate.schedule( group, job );
    }

    public JobHandle schedule( Group group, Runnable job )
    {
        return delegate.schedule( group, job );
    }

    public JobHandle schedule( Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit )
    {
        return delegate.schedule( group, runnable, initialDelay, timeUnit );
    }

    public JobHandle scheduleRecurring( Group group, Runnable runnable, long period,
            TimeUnit timeUnit )
    {
        return delegate.scheduleRecurring( group, runnable, period, timeUnit );
    }

    public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period,
            TimeUnit timeUnit )
    {
        return delegate.scheduleRecurring( group, runnable, initialDelay, period, timeUnit );
    }

    public Stream<ActiveGroup> activeGroups()
    {
        return delegate.activeGroups();
    }

    public void profileGroup( Group group, Profiler profiler )
    {
        delegate.profileGroup( group, profiler );
    }

    public void init() throws Exception
    {
        delegate.init();
    }

    public void start() throws Exception
    {
        delegate.start();
    }

    public void stop() throws Exception
    {
        delegate.stop();
    }

    public void shutdown() throws Exception
    {
        delegate.shutdown();
    }

    public void close() throws Exception
    {
        delegate.close();
    }
}
