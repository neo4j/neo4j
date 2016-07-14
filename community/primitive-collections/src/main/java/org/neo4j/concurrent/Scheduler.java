/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.concurrent;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MutableCallSite;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static java.lang.invoke.MethodType.methodType;
import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.getInteger;

public final class Scheduler
{
    public enum OnRejection
    {
        SPAWN,
        THROW,
        CALLER_RUNS,
        DROP
    }

    public interface ThreadPoolFactory
    {
        ForkJoinPool buildPool(
                int parallelismHint,
                ForkJoinPool.ForkJoinWorkerThreadFactory factory,
                Thread.UncaughtExceptionHandler handler,
                boolean asyncModeHint );
    }

    public static class SchedulerSettings
    {
        public int parallelism;
        public ForkJoinPool.ForkJoinWorkerThreadFactory threadFactory;
        public ThreadPoolFactory poolFactory;
    }

    private static final String cpuThreadNamePrefix = "neo4j.compute";
    private static final String ioThreadNamePrefix = "neo4j.io";
    private static final String schedNamePrefix = "neo4j.sched";
    private static final String extraNamePrefix = "neo4j.extra";

    private static int defaultCpuBoundPoolParallelism = getInteger(
            Scheduler.class, "computeBoundPoolParallelism", Runtime.getRuntime().availableProcessors() );
    private static int defaultIoBoundPoolParallelism = getInteger(
            Scheduler.class, "ioBoundPoolParallelism", 2 );
    private static final ForkJoinPool.ForkJoinWorkerThreadFactory defaultCpuBoundThreadFactory =
            buildThreadFactory( cpuThreadNamePrefix );
    private static final ForkJoinPool.ForkJoinWorkerThreadFactory defaultIoBoundThreadFactory =
            buildThreadFactory( ioThreadNamePrefix );
    private static final ThreadPoolFactory defaultThreadPoolFactory = ForkJoinPool::new;
    private static final ThreadFactory schedulerThreadFactory = buildOrdinaryThreadFactory( schedNamePrefix );
    private static final ThreadFactory extraThreadFactory = buildOrdinaryThreadFactory( extraNamePrefix );
    private static final AtomicLong spawnedExtraThreads = new AtomicLong();

    private static final ScheduledExecutorService schedulingService =
            Executors.newSingleThreadScheduledExecutor( schedulerThreadFactory );
    private static final Future<?> cancelledFuture = new Future<Object>()
    {
        @Override
        public boolean cancel( boolean mayInterruptIfRunning )
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return true;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException
        {
            throw new CancellationException();
        }

        @Override
        public Object get( long timeout, TimeUnit unit )
                throws InterruptedException, ExecutionException, TimeoutException
        {
            throw new CancellationException();
        }
    };

    private static ForkJoinPool.ForkJoinWorkerThreadFactory cpuBoundThreadFactory = defaultCpuBoundThreadFactory;
    private static ForkJoinPool.ForkJoinWorkerThreadFactory ioBoundThreadFactory = defaultIoBoundThreadFactory;
    private static ThreadPoolFactory cpuBoundPoolFactory = defaultThreadPoolFactory;
    private static ThreadPoolFactory ioBoundPoolFactory = defaultThreadPoolFactory;
    private static int currentCpuParallelism = defaultCpuBoundPoolParallelism;
    private static int currentIoParallelism = defaultIoBoundPoolParallelism;
    private static boolean cpuInitialised;
    private static boolean ioInitialised;

    private static final MutableCallSite cpuPoolGetterCallSite;
    private static final MethodHandle initCpuMethodHandle;
    private static final MethodHandle cpuBoundPoolGetter;
    private static final MutableCallSite ioPoolGetterCallSite;
    private static final MethodHandle initIoMethodHandle;
    private static final MethodHandle ioBoundPoolGetter;

    static
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try
        {
            initCpuMethodHandle = lookup.findStatic( Scheduler.class, "initCpu", methodType( ForkJoinPool.class ) );
        }
        catch ( Exception e )
        {
            throw new LinkageError( "initCpu", e );
        }
        cpuPoolGetterCallSite = new MutableCallSite( initCpuMethodHandle );
        cpuBoundPoolGetter = cpuPoolGetterCallSite.dynamicInvoker();

        try
        {
            initIoMethodHandle = lookup.findStatic( Scheduler.class, "initIo", methodType( ForkJoinPool.class ) );
        }
        catch ( Exception e )
        {
            throw new LinkageError( "initIo", e );
        }
        ioPoolGetterCallSite = new MutableCallSite( initIoMethodHandle );
        ioBoundPoolGetter = ioPoolGetterCallSite.dynamicInvoker();
    }

    @SuppressWarnings( "unused" ) // Called through MethodHandles
    private static synchronized ForkJoinPool initCpu()
    {
        if ( !cpuInitialised )
        {
            ForkJoinPool pool = cpuBoundPoolFactory.buildPool(
                    currentCpuParallelism, cpuBoundThreadFactory, null, false );
            MethodHandle getter = MethodHandles.constant( ForkJoinPool.class, pool );
            cpuPoolGetterCallSite.setTarget( getter );
            cpuInitialised = true;
            syncMutableCallSites();
        }

        return getCpuBoundPool();
    }

    @SuppressWarnings( "unused" ) // Called through MethodHandles
    private static synchronized ForkJoinPool initIo()
    {
        if ( !ioInitialised )
        {
            ForkJoinPool pool = ioBoundPoolFactory.buildPool(
                    currentIoParallelism, ioBoundThreadFactory, null, true );
            MethodHandle getter = MethodHandles.constant( ForkJoinPool.class, pool );
            ioPoolGetterCallSite.setTarget( getter );
            ioInitialised = true;
            syncMutableCallSites();
        }

        return getIoBoundPool();
    }

    private static void syncMutableCallSites()
    {
        MutableCallSite.syncAll( new MutableCallSite[]{cpuPoolGetterCallSite, ioPoolGetterCallSite} );
    }

    private static ForkJoinPool.ForkJoinWorkerThreadFactory buildThreadFactory( String namePrefix )
    {
        AtomicInteger threadCounter = new AtomicInteger();
        ForkJoinPool.ForkJoinWorkerThreadFactory motherFactory = ForkJoinPool.commonPool().getFactory();
        return pool ->
        {
            ForkJoinWorkerThread thread = motherFactory.newThread( pool );
            configureThread( thread, namePrefix, threadCounter );
            return thread;
        };
    }

    private static ThreadFactory buildOrdinaryThreadFactory( String namePrefix )
    {
        AtomicInteger threadCounter = new AtomicInteger();
        return runnable -> configureThread( new Thread( runnable ), namePrefix, threadCounter );
    }

    private static <T extends Thread> T configureThread( T thread, String namePrefix, AtomicInteger threadCounter )
    {
        thread.setDaemon( true );
        thread.setName( namePrefix + "." + threadCounter.incrementAndGet() );
        return thread;
    }

    private static ForkJoinPool getCpuBoundPool()
    {
        try
        {
            return (ForkJoinPool) cpuBoundPoolGetter.invokeExact();
        }
        catch ( Throwable throwable )
        {
            throw new LinkageError( "cpuBoundPoolGetter", throwable );
        }
    }

    private static ForkJoinPool getIoBoundPool()
    {
        try
        {
            return (ForkJoinPool) ioBoundPoolGetter.invokeExact();
        }
        catch ( Throwable throwable )
        {
            throw new LinkageError( "ioBoundPoolGetter", throwable );
        }
    }

    public static void resetSchedulerSettings()
    {
        modifySchedulerSettings( (cpu, io) ->
        {
            cpu.parallelism = defaultCpuBoundPoolParallelism;
            cpu.threadFactory = defaultCpuBoundThreadFactory;
            cpu.poolFactory = defaultThreadPoolFactory;

            io.parallelism = defaultIoBoundPoolParallelism;
            io.threadFactory = defaultIoBoundThreadFactory;
            io.poolFactory = defaultThreadPoolFactory;
        });
    }

    public static void modifySchedulerSettings(
            BiConsumer<SchedulerSettings, SchedulerSettings> modifier )
    {
        List<ForkJoinPool> oldPools = updateAndInstallNewPools(modifier);
        for ( ForkJoinPool oldPool : oldPools )
        {
            oldPool.awaitQuiescence( 5, TimeUnit.SECONDS );
            oldPool.shutdown();
        }
    }

    private static synchronized List<ForkJoinPool> updateAndInstallNewPools(
            BiConsumer<SchedulerSettings, SchedulerSettings> modifier )
    {
        SchedulerSettings cpu = new SchedulerSettings();
        cpu.parallelism = currentCpuParallelism;
        cpu.poolFactory = cpuBoundPoolFactory;
        cpu.threadFactory = cpuBoundThreadFactory;
        SchedulerSettings io = new SchedulerSettings();
        io.parallelism = currentIoParallelism;
        io.poolFactory = ioBoundPoolFactory;
        io.threadFactory = ioBoundThreadFactory;

        modifier.accept( cpu, io );

        boolean modifiedCpu = false;
        boolean modifiedIo = false;

        if ( cpu.parallelism > 0 && cpu.parallelism != currentCpuParallelism )
        {
            currentCpuParallelism = cpu.parallelism;
            modifiedCpu = true;
        }

        if ( cpu.threadFactory != null && cpu.threadFactory != cpuBoundThreadFactory )
        {
            cpuBoundThreadFactory = cpu.threadFactory;
            modifiedCpu = true;
        }

        if ( cpu.poolFactory != null && cpu.poolFactory != cpuBoundPoolFactory )
        {
            cpuBoundPoolFactory = cpu.poolFactory;
            modifiedCpu = true;
        }

        if ( io.parallelism > 0 && io.parallelism != currentIoParallelism )
        {
            currentIoParallelism = io.parallelism;
            modifiedIo = true;
        }

        if ( io.threadFactory != null && io.threadFactory != ioBoundThreadFactory )
        {
            ioBoundThreadFactory = io.threadFactory;
            modifiedIo = true;
        }

        if ( io.poolFactory != null && io.poolFactory != ioBoundPoolFactory )
        {
            ioBoundPoolFactory = io.poolFactory;
            modifiedIo = true;
        }

        List<ForkJoinPool> oldPools = new LinkedList<>();
        if ( modifiedCpu & cpuInitialised )
        {
            oldPools.add( getCpuBoundPool() );
            cpuInitialised = false;
            cpuPoolGetterCallSite.setTarget( initCpuMethodHandle );
        }
        if ( modifiedIo & ioInitialised )
        {
            oldPools.add( getIoBoundPool() );
            ioInitialised = false;
            ioPoolGetterCallSite.setTarget( initIoMethodHandle );
        }
        if ( !oldPools.isEmpty() )
        {
            syncMutableCallSites();
        }
        return oldPools;
    }

    public static <T> Future<T> executeComputeBound( Callable<T> callable, OnRejection onRejection )
    {
        checkRejectionForNull( onRejection );
        try
        {
            return getCpuBoundPool().submit( callable );
        }
        catch ( Throwable th )
        {
            return handleRejection( callable, th, onRejection );
        }
    }

    public static <T> Future<T> executeIOBound( Callable<T> callable, OnRejection onRejection )
    {
        checkRejectionForNull( onRejection );
        try
        {
            return getIoBoundPool().submit( callable );
        }
        catch ( Throwable th )
        {
            return handleRejection( callable, th, onRejection );
        }
    }

    private static void checkRejectionForNull( OnRejection onRejection )
    {
        if ( onRejection == null )
        {
            throw new IllegalArgumentException( "The OnRejection cannot be null" );
        }
    }

    private static <T> Future<T> handleRejection(
            Callable<T> callable, Throwable th, OnRejection onRejection )
    {
        FutureTask<T> task;
        switch ( onRejection )
        {
        case CALLER_RUNS:
            task = new FutureTask<>( callable );
            task.run();
            return task;
        case DROP:
            //noinspection unchecked
            return (Future<T>) cancelledFuture;
        case SPAWN:
            task = new FutureTask<>( callable );
            Thread thread = extraThreadFactory.newThread( task );
            thread.start();
            spawnedExtraThreads.getAndIncrement();
            return task;
        case THROW:
            if ( th instanceof RejectedExecutionException )
            {
                throw (RejectedExecutionException) th;
            }
            throw new RejectedExecutionException( th );
        }
        throw new AssertionError( "Missing rejection case: " + onRejection );
    }

    public static Future<?> executeRecurring( Runnable action, long initialDelay, long period, TimeUnit unit )
    {
        return schedulingService.scheduleAtFixedRate( action, initialDelay, period, unit );
    }

    static boolean awaitQuiesce( long timeout, TimeUnit unit )
    {
        long halfTimeout = timeout / 2;
        return getCpuBoundPool().awaitQuiescence( halfTimeout, unit ) &
               getIoBoundPool().awaitQuiescence( halfTimeout, unit );
    }

    static long countSpawnedExtraThreads()
    {
        return spawnedExtraThreads.get();
    }

    private Scheduler()
    {
        // Disallow instantiation.
        // All public methods and internal states are static, because we are dealing with system-wide resources.
    }
}
