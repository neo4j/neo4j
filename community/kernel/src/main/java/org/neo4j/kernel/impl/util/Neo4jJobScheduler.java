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
package org.neo4j.kernel.impl.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.impl.util.DebugUtil.trackTest;
import static org.neo4j.scheduler.JobScheduler.Group.NO_METADATA;

public class Neo4jJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private static final Group SCHEDULER_GROUP = new Group( "Scheduler" );

    private ScheduledThreadPoolExecutor scheduledExecutor;

    // Contains workStealingExecutors for each group that have asked for one.
    // If threads need to be created, they need to be inside one of these pools.
    // We also need to remember to shutdown all pools when we shutdown the database to shutdown queries in an orderly fashion.
    private final ConcurrentHashMap<Group,ExecutorService> workStealingExecutors;

    private final ThreadGroup topLevelGroup;
    private final ConcurrentHashMap<Group,ThreadPool> pools;
    private final Function<Group,ThreadPool> poolBuilder;

    private volatile boolean started;

    public Neo4jJobScheduler()
    {
        workStealingExecutors = new ConcurrentHashMap<>( 1 );
        topLevelGroup = new ThreadGroup( "Neo4j-" + INSTANCE_COUNTER.incrementAndGet() + trackTest() );
        pools = new ConcurrentHashMap<>();
        poolBuilder = group -> new ThreadPool( group, topLevelGroup );
    }

    @Override
    public void init()
    {
        ThreadFactory threadFactory = new GroupedDaemonThreadFactory( SCHEDULER_GROUP, topLevelGroup );
        this.scheduledExecutor = new ScheduledThreadPoolExecutor( 1, threadFactory );
        started = true;
    }

    private ThreadPool getThreadPool( Group group )
    {
        return pools.computeIfAbsent( group, poolBuilder );
    }

    @Override
    public Executor executor( Group group )
    {
        return job -> schedule( group, job );
    }

    @Override
    public ExecutorService workStealingExecutor( Group group, int parallelism )
    {
        return workStealingExecutors.computeIfAbsent( group, g -> createNewWorkStealingExecutor( g, parallelism ) );
    }

    @Override
    public ThreadFactory threadFactory( Group group )
    {
        return getThreadPool( group ).getThreadFactory();
    }

    private ExecutorService createNewWorkStealingExecutor( Group group, int parallelism )
    {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory =
                new GroupedDaemonThreadFactory( group, topLevelGroup );
        return new ForkJoinPool( parallelism, factory, null, false );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        return schedule( group, job, NO_METADATA );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job, Map<String,String> metadata )
    {
        if ( !started )
        {
            throw new RejectedExecutionException( "Scheduler is not started" );
        }
        ThreadPool threadPool = getThreadPool( group );
        return threadPool.submit( job );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, final Runnable runnable, long period, TimeUnit timeUnit )
    {
        return scheduleRecurring( group, runnable, 0, period, timeUnit );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, final Runnable runnable, long initialDelay, long period,
            TimeUnit timeUnit )
    {
        ScheduledTask scheduledTask = new ScheduledTask( this, group, runnable );
        ScheduledFuture<?> future = scheduledExecutor.scheduleAtFixedRate(
                scheduledTask, initialDelay, period, timeUnit );
        ScheduledJobHandle handle = new ScheduledJobHandle( future, scheduledTask );
        handle.registerCancelListener( scheduledTask );
        return handle;
    }

    @Override
    public JobHandle schedule( Group group, final Runnable runnable, long initialDelay, TimeUnit timeUnit )
    {
        ScheduledTask scheduledTask = new ScheduledTask( this, group, runnable );
        ScheduledFuture<?> future = scheduledExecutor.schedule( scheduledTask, initialDelay, timeUnit );
        ScheduledJobHandle handle = new ScheduledJobHandle( future, scheduledTask );
        handle.registerCancelListener( scheduledTask );
        return handle;
    }

    @Override
    public void shutdown()
    {
        started = false;

        // Cancel jobs which hasn't been cancelled already, this to avoid having to wait the full
        // max wait time and then just leave them.
        pools.forEach( (group,pool) -> pool.cancelAllJobs() );
        pools.forEach( (group,pool) -> pool.shutDown() );
        InterruptedException exception = pools.values().stream().reduce( null,
                ( e, p ) -> Exceptions.chain( e, p.shutdownInterrupted ), Exceptions::chain );

        ScheduledThreadPoolExecutor executor = scheduledExecutor;
        scheduledExecutor = null;
        exception = shutdownPool( executor, exception );

        for ( ExecutorService workStealingExecutor : workStealingExecutors.values() )
        {
            exception = shutdownPool( workStealingExecutor, exception );
        }
        workStealingExecutors.clear();

        if ( exception != null )
        {
            throw new RuntimeException( "Unable to shut down job scheduler properly.", exception );
        }
    }

    private InterruptedException shutdownPool( ExecutorService pool, InterruptedException exception )
    {
        if ( pool != null )
        {
            pool.shutdown();
            try
            {
                pool.awaitTermination( 30, TimeUnit.SECONDS );
            }
            catch ( InterruptedException e )
            {
                return Exceptions.chain( exception, e );
            }
        }
        return exception;
    }

    private static class PooledJobHandle implements JobHandle
    {
        private final Future<?> job;
        private final List<CancelListener> cancelListeners = new CopyOnWriteArrayList<>();

        PooledJobHandle( Future<?> job )
        {
            this.job = job;
        }

        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {
            job.cancel( mayInterruptIfRunning );
            for ( CancelListener cancelListener : cancelListeners )
            {
                cancelListener.cancelled( mayInterruptIfRunning );
            }
        }

        @Override
        public void waitTermination() throws InterruptedException, ExecutionException
        {
            job.get();
        }

        @Override
        public void registerCancelListener( CancelListener listener )
        {
            cancelListeners.add( listener );
        }
    }

    private static class ScheduledJobHandle extends PooledJobHandle
    {
        private final ScheduledTask scheduledTask;
        private final ScheduledFuture<?> job;

        ScheduledJobHandle( ScheduledFuture<?> job, ScheduledTask scheduledTask )
        {
            super( job );
            this.job = job;
            this.scheduledTask = scheduledTask;
        }

        @Override
        public void waitTermination() throws InterruptedException, ExecutionException
        {
            try
            {
                scheduledTask.waitTermination();
            }
            catch ( ExecutionException e )
            {
                job.cancel( true );
                throw e;
            }
            super.waitTermination();
        }
    }

    private static class RegisteringPooledJobHandle extends PooledJobHandle
    {
        private final Set<JobHandle> register;

        RegisteringPooledJobHandle( Future<?> job, Set<JobHandle> register )
        {
            super( job );
            this.register = register;
        }

        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {
            super.cancel( mayInterruptIfRunning );
            register.remove( this );
        }
    }

    private static final class ScheduledTask implements Runnable, CancelListener
    {
        private final BinaryLatch handleRelease;
        private final JobScheduler scheduler;
        private final Group group;
        private final Runnable task;
        private volatile JobHandle latestHandle;
        private volatile Throwable lastException;

        private ScheduledTask( JobScheduler scheduler, Group group, Runnable task )
        {
            handleRelease = new BinaryLatch();
            this.scheduler = scheduler;
            this.group = group;
            this.task = () ->
            {
                try
                {
                    task.run();
                }
                catch ( Throwable e )
                {
                    lastException = e;
                }
            };
        }

        @Override
        public void run()
        {
            checkPreviousRunFailure();
            latestHandle = scheduler.schedule( group, task );
            handleRelease.release();
        }

        private void checkPreviousRunFailure()
        {
            Throwable e = lastException;
            if ( e != null )
            {
                if ( e instanceof Error )
                {
                    throw (Error) e;
                }
                if ( e instanceof RuntimeException )
                {
                    throw (RuntimeException) e;
                }
                throw new RuntimeException( e );
            }
        }

        @Override
        public void cancelled( boolean mayInterruptIfRunning )
        {
            JobHandle handle = this.latestHandle;
            if ( handle != null )
            {
                handle.cancel( mayInterruptIfRunning );
            }
        }

        public void waitTermination() throws ExecutionException, InterruptedException
        {

            handleRelease.await();

            latestHandle.waitTermination();
        }
    }

    private static class GroupedDaemonThreadFactory implements ThreadFactory, ForkJoinPool.ForkJoinWorkerThreadFactory
    {
        private final Group group;
        private final ThreadGroup threadGroup;

        private GroupedDaemonThreadFactory( Group group, ThreadGroup parentThreadGroup )
        {
            this.group = group;
            threadGroup = new ThreadGroup( parentThreadGroup, group.name() );
        }

        @Override
        public Thread newThread( @SuppressWarnings( "NullableProblems" ) Runnable job )
        {
            Thread thread = new Thread( threadGroup, job, group.threadName( NO_METADATA ) );
            thread.setDaemon( true );
            return thread;
        }

        @Override
        public ForkJoinWorkerThread newThread( ForkJoinPool pool )
        {
            // We do this complicated dance of allocating the ForkJoinThread in a separate thread,
            // because there is no way to give it a specific ThreadGroup, other than through inheritance
            // from the allocating thread.
            ForkJoinPool.ForkJoinWorkerThreadFactory factory = ForkJoinPool.defaultForkJoinWorkerThreadFactory;
            AtomicReference<ForkJoinWorkerThread> reference = new AtomicReference<>();
            Thread allocator = newThread( () ->  reference.set( factory.newThread( pool ) ) );
            allocator.start();
            do
            {
                try
                {
                    allocator.join();
                }
                catch ( InterruptedException ignore )
                {
                }
            }
            while ( reference.get() == null );
            ForkJoinWorkerThread worker = reference.get();
            worker.setName( group.threadName( NO_METADATA ) );
            return worker;
        }
    }

    private static final class ThreadPool
    {
        private final GroupedDaemonThreadFactory threadFactory;
        private final ExecutorService executor;
        private final Set<JobHandle> jobs;
        private InterruptedException shutdownInterrupted;

        private ThreadPool( Group group, ThreadGroup parentThreadGroup )
        {
            threadFactory = new GroupedDaemonThreadFactory( group, parentThreadGroup );
            executor = Executors.newCachedThreadPool( threadFactory );
            jobs = Collections.synchronizedSet( new HashSet<>() );
        }

        public ThreadFactory getThreadFactory()
        {
            return threadFactory;
        }

        public JobHandle submit( Runnable job )
        {
            Future<?> future = executor.submit( job );
            return new RegisteringPooledJobHandle( future, jobs );
        }

        void cancelAllJobs()
        {
            jobs.removeIf( handle ->
            {
                handle.cancel( true );
                return true;
            } );
        }

        void shutDown()
        {
            executor.shutdown();
            try
            {
                executor.awaitTermination( 30, TimeUnit.SECONDS );
            }
            catch ( InterruptedException e )
            {
                shutdownInterrupted = e;
            }
        }
    }
}
