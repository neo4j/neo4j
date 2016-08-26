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

import org.junit.Test;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class WorkSyncTest
{
    private static ExecutorService executor;

    @BeforeClass
    public static void startExecutor()
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void stopExecutor()
    {
        executor.shutdown();
    }

    private static void usleep( long micros )
    {
        long deadline = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos( micros );
        long now;
        do
        {
            now = System.nanoTime();
        }
        while ( now < deadline );
    }

    private static class AddWork implements Work<Adder,AddWork>
    {
        private int delta;

        private AddWork( int delta )
        {
            this.delta = delta;
        }

        @Override
        public AddWork combine( AddWork work )
        {
            delta += work.delta;
            return this;
        }

        @Override
        public void apply( Adder adder )
        {
            adder.add( delta );
        }
    }

    private class Adder
    {
        private volatile long sum;
        private volatile long count;

        public void add( int delta )
        {
            long s = sum;
            long c = count;

            // Make sure other threads have a chance to run and race with our update
            Thread.yield();
            // Allow an up to ~50 micro-second window for racing and losing updates
            usleep( ThreadLocalRandom.current().nextInt( 50 ) );
            // Finally check if we need to do any actual blocking to order test operations
            // (using uninterruptible acquire so we don't mess with interruption tests)
            semaphore.acquireUninterruptibly();

            sum = s + delta;
            count = c + 1;
        }

        long sum()
        {
            return sum;
        }

        long count()
        {
            return count;
        }
    }

    private class RunnableWork implements Callable<Void>
    {
        private final AddWork addWork;

        RunnableWork( AddWork addWork )
        {
            this.addWork = addWork;
        }

        @Override
        public Void call() throws Exception
        {
            sync.apply( addWork );
            return null;
        }
    }

    private Adder adder = new Adder();
    private WorkSync<Adder,AddWork> sync = new WorkSync<>( adder );
    private Semaphore semaphore = new Semaphore( Integer.MAX_VALUE );

    @After
    public void refillSemaphore()
    {
        // This ensures that no threads end up stuck
        semaphore.drainPermits();
        semaphore.release( Integer.MAX_VALUE );
    }

    @Test
    public void mustApplyWork() throws Exception
    {
        sync.apply( new AddWork( 10 ) );
        assertThat( adder.sum(), is( 10L ) );

        sync.apply( new AddWork( 20 ) );
        assertThat( adder.sum(), is( 30L ) );
    }

    @Test
    public void mustApplyWorkAsync() throws Exception
    {
        Future<?> a = sync.applyAsync( new AddWork( 10 ) );
        a.get();
        assertThat( adder.sum(), is( 10L ) );

        Future<?> b = sync.applyAsync( new AddWork( 20 ) );
        Future<?> c = sync.applyAsync( new AddWork( 30 ) );
        b.get();
        c.get();
        assertThat( adder.sum(), is( 60L ) );
    }

    @Test
    public void mustCombineWork() throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 64 );
        for ( int i = 0; i < 1000; i++ )
        {
            executor.submit( new RunnableWork( new AddWork( 1 ) ) );
        }
        executor.shutdown();
        assertTrue( executor.awaitTermination( 2, TimeUnit.SECONDS ) );

        assertThat( adder.count(), lessThan( adder.sum() ) );
    }

    @Test
    public void mustCombineWorkAsync() throws Exception
    {
        makeWorkStuckAtSemaphore();

        Future<?> a = sync.applyAsync( new AddWork( 1 ) );
        Future<?> b = sync.applyAsync( new AddWork( 1 ) );
        Future<?> c = sync.applyAsync( new AddWork( 1 ) );
        semaphore.release( 2 );
        a.get();
        b.get();
        c.get();
        assertThat( adder.sum(), is( 4L ) );
        assertThat( adder.count(), is( 2L ) );
    }

    private void makeWorkStuckAtSemaphore() throws InterruptedException, ExecutionException
    {
        semaphore.drainPermits();
        Future<Void> concurrentWork = executor.submit( new RunnableWork( new AddWork( 1 ) ) );
        try
        {
            concurrentWork.get( 10, TimeUnit.MILLISECONDS );
            fail( "should have thrown a TimeoutException" );
        }
        catch ( TimeoutException ignore )
        {
            // good, the concurrent AddWork is now stuck on the semaphore
            assertTrue( semaphore.hasQueuedThreads() );
        }
    }

    @Test
    public void mustSkipFirstCancelledAsyncWork() throws Exception
    {
        makeWorkStuckAtSemaphore();

        Future<?> a = sync.applyAsync( new AddWork( 10 ) );
        Future<?> b = sync.applyAsync( new AddWork( 1 ) );
        Future<?> c = sync.applyAsync( new AddWork( 1 ) );
        assertTrue( a.cancel( true ) );
        semaphore.release( 2 );
        assertGetThrowsCancellationException( a );
        b.get();
        assertTrue( b.isDone() );
        c.get();
        assertTrue( c.isDone() );
        assertThat( adder.sum(), is( 3L ) );
        assertThat( adder.count(), is( 2L ) );
    }

    private void assertGetThrowsCancellationException( Future<?> workSyncFuture )
            throws InterruptedException, ExecutionException
    {
        try
        {
            workSyncFuture.get();
            fail( "should have thrown a CancellationException" );
        }
        catch ( CancellationException e )
        {
            // very good
            assertTrue( workSyncFuture.isCancelled() );
            assertTrue( workSyncFuture.isDone() );
        }
    }

    @Test
    public void mustSkipFirstCancelledAsyncWorkOnGetWithTimeout() throws Exception
    {
        makeWorkStuckAtSemaphore();

        Future<?> a = sync.applyAsync( new AddWork( 10 ) );
        Future<?> b = sync.applyAsync( new AddWork( 1 ) );
        Future<?> c = sync.applyAsync( new AddWork( 1 ) );
        assertTrue( a.cancel( true ) );
        semaphore.release( 2 );
        assertGetWithTimeoutThrowsCancellationException( a );
        b.get();
        assertTrue( b.isDone() );
        c.get();
        assertTrue( c.isDone() );
        assertThat( adder.sum(), is( 3L ) );
        assertThat( adder.count(), is( 2L ) );
    }

    private void assertGetWithTimeoutThrowsCancellationException( Future<?> workSyncFuture )
            throws InterruptedException, ExecutionException, TimeoutException
    {
        try
        {
            workSyncFuture.get( 100, TimeUnit.MILLISECONDS );
            fail( "should have thrown a CancellationException" );
        }
        catch ( CancellationException e )
        {
            // very good
            assertTrue( workSyncFuture.isCancelled() );
            assertTrue( workSyncFuture.isDone() );
        }
    }

    @Test
    public void mustSkipMiddleCancelledAsyncWork() throws Exception
    {
        makeWorkStuckAtSemaphore();

        Future<?> a = sync.applyAsync( new AddWork( 1 ) );
        Future<?> b = sync.applyAsync( new AddWork( 10 ) );
        Future<?> c = sync.applyAsync( new AddWork( 1 ) );
        assertTrue( b.cancel( true ) );
        semaphore.release( 2 );
        a.get();
        assertTrue( a.isDone() );
        assertGetThrowsCancellationException( b );
        c.get();
        assertTrue( c.isDone() );
        assertThat( adder.sum(), is( 3L ) );
        assertThat( adder.count(), is( 2L ) );
    }

    @Test
    public void mustSkipMiddleCancelledAsyncWorkOnGetWithTimeout() throws Exception
    {
        makeWorkStuckAtSemaphore();

        Future<?> a = sync.applyAsync( new AddWork( 1 ) );
        Future<?> b = sync.applyAsync( new AddWork( 10 ) );
        Future<?> c = sync.applyAsync( new AddWork( 1 ) );
        assertTrue( b.cancel( true ) );
        semaphore.release( 2 );
        a.get();
        assertTrue( a.isDone() );
        assertGetWithTimeoutThrowsCancellationException( b );
        c.get();
        assertTrue( c.isDone() );
        assertThat( adder.sum(), is( 3L ) );
        assertThat( adder.count(), is( 2L ) );
    }

    @Test
    public void mustSkipLastCancelledAsyncWork() throws Exception
    {
        makeWorkStuckAtSemaphore();

        Future<?> a = sync.applyAsync( new AddWork( 1 ) );
        Future<?> b = sync.applyAsync( new AddWork( 1 ) );
        Future<?> c = sync.applyAsync( new AddWork( 10 ) );
        assertTrue( c.cancel( true ) );
        semaphore.release( 2 );
        a.get();
        assertTrue( a.isDone() );
        b.get();
        assertTrue( b.isDone() );
        assertGetThrowsCancellationException( c );
        assertThat( adder.sum(), is( 3L ) );
        assertThat( adder.count(), is( 2L ) );
    }

    @Test
    public void mustSkipLastCancelledAsyncWorkOnGetWithTimeout() throws Exception
    {
        makeWorkStuckAtSemaphore();

        Future<?> a = sync.applyAsync( new AddWork( 1 ) );
        Future<?> b = sync.applyAsync( new AddWork( 1 ) );
        Future<?> c = sync.applyAsync( new AddWork( 10 ) );
        assertTrue( c.cancel( true ) );
        semaphore.release( 2 );
        a.get();
        assertTrue( a.isDone() );
        b.get();
        assertTrue( b.isDone() );
        assertGetWithTimeoutThrowsCancellationException( c );
        assertThat( adder.sum(), is( 3L ) );
        assertThat( adder.count(), is( 2L ) );
    }

    @Test
    public void applyAsyncGetMustThrowIfCancelledWhileBlocking() throws Exception
    {
        makeWorkStuckAtSemaphore();

        Future<?> future = sync.applyAsync( new AddWork( 1 ) );
        AtomicBoolean readyLatch = new AtomicBoolean();
        AtomicBoolean startLatch = new AtomicBoolean();
        executor.submit( () ->
        {
            readyLatch.set( true );
            spinwait( startLatch );
            usleep( 1000 );
            future.cancel( true );
            return null;
        } );
        spinwait( readyLatch );
        startLatch.set( true );
        assertGetThrowsCancellationException( future );
    }

    private void spinwait( AtomicBoolean latch )
    {
        boolean go;
        do
        {
            go = latch.get();
        }
        while ( !go );
    }

    @Test
    public void applyAsyncGetWithTimeoutMustThrowIfCancelledWhileBlocking() throws Exception
    {
        makeWorkStuckAtSemaphore();

        Future<?> future = sync.applyAsync( new AddWork( 1 ) );
        AtomicBoolean readyLatch = new AtomicBoolean();
        AtomicBoolean startLatch = new AtomicBoolean();
        executor.submit( () ->
        {
            readyLatch.set( true );
            spinwait( startLatch );
            usleep( 1000 );
            future.cancel( true );
            return null;
        } );
        spinwait( readyLatch );
        startLatch.set( true );
        assertGetWithTimeoutThrowsCancellationException( future );
    }

    @Test
    public void applyAsyncGetWithTimeoutMustThrowOnTimeout() throws Exception
    {
        makeWorkStuckAtSemaphore();
        Future<?> future = sync.applyAsync( new AddWork( 1 ) );

        try
        {
            future.get( 1, TimeUnit.NANOSECONDS );
            fail( "should have thrown a TimeoutException" );
        }
        catch ( TimeoutException e )
        {
            // very good
            assertFalse( future.isDone() );
            assertFalse( future.isCancelled() );
        }
    }

    @Test
    public void mustApplyWorkEvenWhenInterrupted() throws Exception
    {
        Thread.currentThread().interrupt();

        sync.apply( new AddWork( 10 ) );

        assertThat( adder.sum(), is( 10L ) );
        assertTrue( Thread.interrupted() );
    }

    @Test
    public void mustApplyWorkAsyncEvenWhenInterrupted() throws Exception
    {
        Thread.currentThread().interrupt();

        Future<?> future = sync.applyAsync( new AddWork( 10 ) );
        try
        {
            future.get();
        }
        catch ( InterruptedException e )
        {
            // if our get is interrupted, then we should be able to retry without any problems
            future.get();
            Thread.currentThread().interrupt();
        }

        assertThat( adder.sum(), is( 10L ) );
        assertTrue( Thread.interrupted() );
    }

    @Test( timeout = 1000 )
    public void mustRecoverFromExceptions() throws Exception
    {
        final AtomicBoolean broken = new AtomicBoolean( true );
        Adder adder = new Adder()
        {
            @Override
            public void add( int delta )
            {
                if ( broken.get() )
                {
                    throw new IllegalStateException( "boom!" );
                }
                super.add( delta );
            }
        };
        sync = new WorkSync<>( adder );

        try
        {
            // Run this in a different thread to account for reentrant locks.
            executor.submit( new RunnableWork( new AddWork( 10 ) ) ).get();
            fail( "Should have thrown" );
        }
        catch ( ExecutionException exception )
        {
            // Outermost ExecutionException from the ExecutorService
            assertThat( exception.getCause(), instanceOf( ExecutionException.class ) );

            // Inner ExecutionException from the WorkSync
            exception = (ExecutionException) exception.getCause();
            assertThat( exception.getCause(), instanceOf( IllegalStateException.class ) );
        }

        broken.set( false );
        sync.apply( new AddWork( 20 ) );

        assertThat( adder.sum(), is( 20L ) );
        assertThat( adder.count(), is( 1L ) );
    }

    @Test
    public void mustNotApplyWorkInParallelUnderStress() throws Exception
    {
        int workers = Runtime.getRuntime().availableProcessors() * 5;
        int iterations = 1_000;
        int incrementValue = 42;
        CountDownLatch startLatch = new CountDownLatch( workers );
        CountDownLatch endLatch = new CountDownLatch( workers );
        ExecutorService executor = Executors.newFixedThreadPool( workers );
        AtomicBoolean start = new AtomicBoolean();
        Callable<Void> work = () ->
        {
            startLatch.countDown();
            boolean spin;
            do
            {
                spin = !start.get();
            }
            while ( spin );

            for ( int i = 0; i < iterations; i++ )
            {
                sync.apply( new AddWork( incrementValue ) );
            }

            endLatch.countDown();
            return null;
        };

        List<Future<Void>> futureList = new ArrayList<>();
        for ( int i = 0; i < workers; i++ )
        {
            futureList.add( executor.submit( work ) );
        }
        startLatch.await();
        start.set( true );
        endLatch.await();

        for ( Future<Void> future : futureList )
        {
            future.get(); // check for any exceptions
        }

        assertThat( adder.count(), lessThan( (long) (workers * iterations) ) );
        assertThat( adder.sum(), is( (long) (incrementValue * workers * iterations) ) );
    }
}
