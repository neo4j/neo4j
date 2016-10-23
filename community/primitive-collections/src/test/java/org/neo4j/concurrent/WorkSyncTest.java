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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
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
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WorkSyncTest
{
    private static ExecutorService executor;

    @BeforeClass
    public static void startExecutor()
    {
        executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() * 5 );
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

    private class AddWork implements Work<Adder,AddWork>, Callable<Void>
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

        @Override
        public Void call() throws ExecutionException
        {
            sync.apply( this );
            return null;
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
        AsyncWork a = sync.applyAsync( new AddWork( 10 ) );
        assertTrue( a.tryComplete() );
        assertThat( adder.sum(), is( 10L ) );

        AsyncWork b = sync.applyAsync( new AddWork( 20 ) );
        AsyncWork c = sync.applyAsync( new AddWork( 30 ) );
        assertTrue( b.tryComplete() );
        assertTrue( c.tryComplete() );
        assertThat( adder.sum(), is( 60L ) );
    }

    @Test( timeout = 3000 )
    public void mustCombineWork() throws Exception
    {
        int workers = 1000;
        Future[] futures = new Future[workers];
        for ( int i = 0; i < workers; i++ )
        {
            futures[i] = executor.submit( new AddWork( 1 ) );
        }
        for ( Future future : futures )
        {
            future.get();
        }

        assertThat( adder.count(), lessThan( adder.sum() ) );
    }

    @Test
    public void mustCombineWorkAsync() throws Exception
    {
        Future<Void> stuckAtSemaphore = makeWorkStuckAtSemaphore();

        AsyncWork a = sync.applyAsync( new AddWork( 1 ) );
        AsyncWork b = sync.applyAsync( new AddWork( 1 ) );
        AsyncWork c = sync.applyAsync( new AddWork( 1 ) );
        semaphore.release( 1 ); // Unstuck lock-holding thread
        stuckAtSemaphore.get();
        semaphore.release( 1 ); // One permit for the combined application of all three works.
        assertTrue( a.tryComplete() );
        assertTrue( b.tryComplete() );
        assertTrue( c.tryComplete() );
        assertThat( adder.sum(), is( 4L ) );
        assertThat( adder.count(), is( 2L ) );
    }

    private Future<Void> makeWorkStuckAtSemaphore() throws InterruptedException, ExecutionException
    {
        semaphore.drainPermits();
        Future<Void> concurrentWork = executor.submit( new AddWork( 1 ) );
        try
        {
            concurrentWork.get( 10, TimeUnit.MILLISECONDS );
            fail( "should have thrown a TimeoutException" );
        }
        catch ( TimeoutException ignore )
        {
            while ( !semaphore.hasQueuedThreads() )
            {
                usleep( 1 );
            }
            // good, the concurrent AddWork is now stuck on the semaphore
        }
        return concurrentWork;
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

        AsyncWork asyncWork = sync.applyAsync( new AddWork( 10 ) );
        assertTrue( asyncWork.tryComplete() );

        assertThat( adder.sum(), is( 10L ) );
        assertTrue( Thread.interrupted() );
    }

    @Test
    public void applyMustThrowInWorkApplyingThreadIfTaskFails() throws Exception
    {
        ArithmeticException oops = new ArithmeticException( "Oops" );
        AddWork work = new AddWork( 10 )
        {
            @Override
            public void apply( Adder adder )
            {
                throw oops;
            }
        };
        try
        {
            sync.apply( work );
            fail( "apply should have failed" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), sameInstance( oops ) );
        }
    }

    @Test
    public void applyAsyncMustThrowInWorkApplyingThreadIfTaskFails() throws Exception
    {
        ArithmeticException oops = new ArithmeticException( "Oops" );
        AddWork work = new AddWork( 10 )
        {
            @Override
            public void apply( Adder adder )
            {
                throw oops;
            }
        };
        AsyncWork asyncWork = sync.applyAsync( work );
        try
        {
            asyncWork.tryComplete();
            fail( "asyncWork.tryComplete should have failed" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), sameInstance( oops ) );
        }
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
            executor.submit( new AddWork( 10 ) ).get();
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

    @Test
    public void mustNotApplyAsyncWorkInParallelUnderStress() throws Exception
    {
        int workers = Runtime.getRuntime().availableProcessors() * 5;
        int iterations = 200;
        int incrementValue = 512;
        CountDownLatch startLatch = new CountDownLatch( workers );
        CountDownLatch endLatch = new CountDownLatch( workers );
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
                AsyncWork asyncWork = sync.applyAsync( new AddWork( incrementValue ) );
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                boolean done;
                do
                {
                    usleep( rng.nextInt( 50 ) );
                    done = asyncWork.tryComplete();
                }
                while ( !done );
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

        long expectedCount = workers * iterations;
        long expectedSum = incrementValue * expectedCount;
        assertThat( adder.count(), lessThan( expectedCount ) );
        assertThat( adder.sum(), is( expectedSum ) );
    }
}
