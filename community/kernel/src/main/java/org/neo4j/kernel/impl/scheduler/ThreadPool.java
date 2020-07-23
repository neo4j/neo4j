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

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.scheduler.FailedJobRun;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobType;
import org.neo4j.scheduler.MonitoredJobInfo;
import org.neo4j.scheduler.SchedulerThreadFactory;
import org.neo4j.scheduler.SchedulerThreadFactoryFactory;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.FeatureToggles;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.scheduler.JobMonitoringParams.NOT_MONITORED;

final class ThreadPool
{
    private static final int SHUTDOWN_TIMEOUT_SECONDS = FeatureToggles.getInteger( ThreadPool.class, "shutdownTimeout", 30 );
    private static final int UNMONITORED_JOB_ID = -1;

    private final SchedulerThreadFactory threadFactory;
    private final ExecutorService executor;
    private final ConcurrentHashMap<Object,RegisteredJob> registry;
    private final Group group;
    private final SystemNanoClock clock;
    private final FailedJobRunsStore failedJobRunsStore;
    private final LongSupplier jobIdSupplier;
    private InterruptedException shutdownInterrupted;

    static class ThreadPoolParameters
    {
        volatile int desiredParallelism;
        volatile SchedulerThreadFactoryFactory providedThreadFactory = GroupedDaemonThreadFactory::new;
    }

    ThreadPool( Group group, ThreadGroup parentThreadGroup, ThreadPoolParameters parameters, SystemNanoClock clock, FailedJobRunsStore failedJobRunsStore,
            LongSupplier jobIdSupplier )
    {
        this.group = group;
        this.clock = clock;
        this.failedJobRunsStore = failedJobRunsStore;
        this.jobIdSupplier = jobIdSupplier;
        threadFactory = parameters.providedThreadFactory.newSchedulerThreadFactory( group, parentThreadGroup );
        executor = group.buildExecutorService( threadFactory, parameters.desiredParallelism );
        registry = new ConcurrentHashMap<>();
    }

    ThreadFactory getThreadFactory()
    {
        return threadFactory;
    }

    public ExecutorService getExecutorService()
    {
        return executor;
    }

    public <T> JobHandle<T> submit( JobMonitoringParams jobMonitoringParams, Callable<T> job )
    {
        Object registryKey = new Object();
        AtomicBoolean running = new AtomicBoolean();
        Instant submitted = clock.instant();
        long jobId;
        if ( NOT_MONITORED == jobMonitoringParams )
        {
            jobId = UNMONITORED_JOB_ID;
        }
        else
        {
            jobId = jobIdSupplier.getAsLong();
        }

        Callable<T> registeredJob = () ->
        {
            Instant executionStart = clock.instant();
            try
            {
                running.set( true );
                return job.call();
            }
            catch ( Throwable t )
            {
                recordFailedRun( jobId, jobMonitoringParams, submitted, executionStart, t );
                throw t;
            }
            finally
            {
                registry.remove( registryKey );
            }
        };

        var placeHolder = new RegisteredJob( -1, completedFuture( Void.TYPE ), NOT_MONITORED, Instant.now(), new AtomicBoolean() );
        registry.put( registryKey, placeHolder );
        try
        {
            var future = executor.submit( registeredJob );
            registry.replace( registryKey, new RegisteredJob( jobId, future, jobMonitoringParams, submitted, running ) );
            return new PooledJobHandle<>( future, registryKey, registry );
        }
        catch ( Exception e )
        {
            registry.remove( registryKey );
            throw e;
        }
    }

    public JobHandle<?> submit( JobMonitoringParams jobMonitoringParams, Runnable job )
    {
        return submit( jobMonitoringParams, asCallable( job ) );
    }

    private static Callable<?> asCallable( Runnable job )
    {
        return () -> {
            job.run();
            return null;
        };
    }

    int activeJobCount()
    {
        return registry.size();
    }

    int activeThreadCount()
    {
        return threadFactory.getThreadGroup().activeCount();
    }

    Stream<Thread> activeThreads()
    {
        ThreadGroup threadGroup = threadFactory.getThreadGroup();
        int activeCountEstimate = threadGroup.activeCount();
        int activeCountFudge = Math.max( (int) Math.sqrt( activeCountEstimate ), 10 );
        Thread[] snapshot = new Thread[activeCountEstimate + activeCountFudge];
        threadGroup.enumerate( snapshot );
        return Arrays.stream( snapshot ).filter( Objects::nonNull );
    }

    void cancelAllJobs()
    {
        registry.values().removeIf( registeredJob ->
        {
            registeredJob.future.cancel( true );
            return true;
        } );
    }

    void shutDown()
    {
        executor.shutdown();
        try
        {
            executor.awaitTermination( SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS );
        }
        catch ( InterruptedException e )
        {
            shutdownInterrupted = e;
        }
    }

    List<MonitoredJobInfo> getMonitoredJobs()
    {
        return registry.values().stream()
                       .filter( registeredJob -> registeredJob.monitoredJobParams != NOT_MONITORED )
                       .map( monitoredJob ->
                               new MonitoredJobInfo(
                                       monitoredJob.jobId,
                                       group,
                                       monitoredJob.submitted,
                                       monitoredJob.monitoredJobParams.getSubmitter(),
                                       monitoredJob.monitoredJobParams.getTargetDatabaseName(),
                                       monitoredJob.monitoredJobParams.getDescription(),
                                       null,
                                       null,
                                       monitoredJob.running.get() ? MonitoredJobInfo.State.EXECUTING : MonitoredJobInfo.State.SCHEDULED,
                                       JobType.IMMEDIATE,
                                       monitoredJob.monitoredJobParams.getCurrentStateDescription() )
                       )
                       .collect( Collectors.toList() );
    }

    InterruptedException getShutdownException()
    {
        return shutdownInterrupted;
    }

    private void recordFailedRun( long jobId, JobMonitoringParams jobMonitoringParams, Instant submitted, Instant executionStart, Throwable t )
    {
        if ( jobMonitoringParams == NOT_MONITORED )
        {
            return;
        }

        FailedJobRun failedJobRun = new FailedJobRun( jobId,
                group,
                jobMonitoringParams.getSubmitter(),
                jobMonitoringParams.getTargetDatabaseName(),
                jobMonitoringParams.getDescription(),
                JobType.IMMEDIATE,
                submitted,
                executionStart,
                clock.instant(),
                t );
        failedJobRunsStore.add( failedJobRun );
    }

    private static class RegisteredJob
    {
        private final long jobId;
        private final Future<?> future;
        private final JobMonitoringParams monitoredJobParams;
        private final Instant submitted;
        private final AtomicBoolean running;

        RegisteredJob( long jobId, Future<?> future, JobMonitoringParams monitoredJobParams, Instant submitted, AtomicBoolean running )
        {
            this.jobId = jobId;
            this.future = requireNonNull( future );
            this.monitoredJobParams = requireNonNull( monitoredJobParams );
            this.submitted = requireNonNull( submitted );
            this.running = requireNonNull( running );
        }
    }
}
