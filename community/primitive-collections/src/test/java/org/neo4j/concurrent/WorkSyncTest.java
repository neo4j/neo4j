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
package org.neo4j.concurrent;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
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
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void stopExecutor()
    {
        executor.shutdownNow();
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
            usleep( 50 );
            adder.add( delta );
        }
    }

    private class Adder
    {
        public void add( int delta )
        {
            semaphore.acquireUninterruptibly();
            sum.add( delta );
            count.increment();
        }
    }

    private class CallableWork implements Callable<Void>
    {
        private final AddWork addWork;

        CallableWork( AddWork addWork )
        {
            this.addWork = addWork;
        }

        @Override
        public Void call() throws ExecutionException
        {
            sync.apply( addWork );
            return null;
        }
    }

    private static class UnsynchronisedAdder
    {
        // The volatile modifier prevents hoisting and reordering optimisations that could *hide* races
        private volatile long value;

        public void add( long delta )
        {
            long v = value;
            // Make sure other threads have a chance to run and race with our update
            Thread.yield();
            // Allow an up to ~50 micro-second window for racing and losing updates
            usleep( ThreadLocalRandom.current().nextInt( 50 ) );
            value = v + delta;
        }

        public void increment()
        {
            add( 1 );
        }

        public long sum()
        {
            return value;
        }
    }

    private UnsynchronisedAdder sum = new UnsynchronisedAdder();
    private UnsynchronisedAdder count = new UnsynchronisedAdder();
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

    private Future<Void> makeWorkStuckAtSemaphore( int delta ) throws InterruptedException, ExecutionException
    {
        semaphore.drainPermits();
        Future<Void> concurrentWork = executor.submit( new CallableWork( new AddWork( delta ) ) );
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
    public void mustApplyWork() throws Exception
    {
        sync.apply( new AddWork( 10 ) );
        assertThat( sum.sum(), is( 10L ) );

        sync.apply( new AddWork( 20 ) );
        assertThat( sum.sum(), is( 30L ) );
    }

    @Test ( timeout = 10000 )
    public void mustCombineWork() throws Exception
    {
        BinaryLatch startLatch = new BinaryLatch();
        BinaryLatch blockLatch = new BinaryLatch();
        FutureTask<Void> blocker = new FutureTask<>( new CallableWork( new AddWork( 1 )
        {
            @Override
            public void apply( Adder adder )
            {
                super.apply( adder );
                startLatch.release();
                blockLatch.await();
            }
        } ) );
        new Thread( blocker ).start();
        startLatch.await();
        Collection<FutureTask<Void>> tasks = new ArrayList<>();
        tasks.add( blocker );
        for ( int i = 0; i < 20; i++ )
        {

            CallableWork task = new CallableWork( new AddWork( 1 ) );
            FutureTask<Void> futureTask = new FutureTask<>( task );
            tasks.add( futureTask );
            Thread thread = new Thread( futureTask );
            thread.start();
            //wait for the thread to reach the lock
            while ( thread.getState() != Thread.State.TIMED_WAITING )
            {
            }
        }
        blockLatch.release();
        for ( FutureTask<Void> task : tasks )
        {
            task.get();
        }
        assertThat( count.sum(), lessThan( sum.sum() ) );
    }

    @Test
    public void mustApplyWorkEvenWhenInterrupted() throws Exception
    {
        Thread.currentThread().interrupt();

        sync.apply( new AddWork( 10 ) );

        assertThat( sum.sum(), is( 10L ) );
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
            executor.submit( new CallableWork( new AddWork( 10 ) ) ).get();
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

        assertThat( sum.sum(), is( 20L ) );
        assertThat( count.sum(), is( 1L ) );
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

        assertThat( count.sum(), lessThan( (long) (workers * iterations) ) );
        assertThat( sum.sum(), is( (long) (incrementValue * workers * iterations) ) );
    }

    @Test
    public void mustNotApplyAsyncWorkInParallelUnderStress() throws Exception
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

            ThreadLocalRandom rng = ThreadLocalRandom.current();
            List<AsyncApply> asyncs = new ArrayList<>();
            for ( int i = 0; i < iterations; i++ )
            {
                asyncs.add( sync.applyAsync( new AddWork( incrementValue ) ) );
                if ( rng.nextInt( 10 ) == 0 )
                {
                    for ( AsyncApply async : asyncs )
                    {
                        async.await();
                    }
                    asyncs.clear();
                }
            }

            for ( AsyncApply async : asyncs )
            {
                async.await();
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

        assertThat( count.sum(), lessThan( (long) (workers * iterations) ) );
        assertThat( sum.sum(), is( (long) (incrementValue * workers * iterations) ) );
    }

    @Test
    public void mustApplyWorkAsync() throws Exception
    {
        AsyncApply a = sync.applyAsync( new AddWork( 10 ) );
        a.await();
        assertThat( sum.sum(), is( 10L ) );

        AsyncApply b = sync.applyAsync( new AddWork( 20 ) );
        AsyncApply c = sync.applyAsync( new AddWork( 30 ) );
        b.await();
        c.await();
        assertThat( sum.sum(), is( 60L ) );
    }

    @Test
    public void mustCombineWorkAsync() throws Exception
    {
        makeWorkStuckAtSemaphore( 1 );

        AsyncApply a = sync.applyAsync( new AddWork( 1 ) );
        AsyncApply b = sync.applyAsync( new AddWork( 1 ) );
        AsyncApply c = sync.applyAsync( new AddWork( 1 ) );
        semaphore.release( 2 );
        a.await();
        b.await();
        c.await();
        assertThat( sum.sum(), is( 4L ) );
        assertThat( count.sum(), is( 2L ) );
    }

    @Test
    public void mustApplyWorkAsyncEvenWhenInterrupted() throws Exception
    {
        Thread.currentThread().interrupt();

        sync.applyAsync( new AddWork( 10 ) ).await();

        assertThat( sum.sum(), is( 10L ) );
        assertTrue( Thread.interrupted() );
    }

    @Test
    public void asyncWorkThatThrowsMustRememberException()
    {
        RuntimeException boo = new RuntimeException( "boo" );
        AsyncApply asyncApply = sync.applyAsync( new AddWork( 10 )
        {
            @Override
            public void apply( Adder adder )
            {
                super.apply( adder );
                throw boo;
            }
        } );

        try
        {
            asyncApply.await();
            fail( "Should have thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), sameInstance( boo ) );
        }

        assertThat( sum.sum(), is( 10L ) );
        assertThat( count.sum(), is( 1L ) );

        try
        {
            asyncApply.await();
            fail( "Should have thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), sameInstance( boo ) );
        }

        assertThat( sum.sum(), is( 10L ) );
        assertThat( count.sum(), is( 1L ) );
    }

    @Test
    public void asyncWorkThatThrowsInAwaitMustRememberException() throws Exception
    {
        Future<Void> stuckAtSemaphore = makeWorkStuckAtSemaphore( 1 );

        RuntimeException boo = new RuntimeException( "boo" );
        AsyncApply asyncApply = sync.applyAsync( new AddWork( 10 )
        {
            @Override
            public void apply( Adder adder )
            {
                super.apply( adder );
                throw boo;
            }
        } );

        refillSemaphore();
        stuckAtSemaphore.get();

        try
        {
            asyncApply.await();
            fail( "Should have thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), sameInstance( boo ) );
        }

        assertThat( sum.sum(), is( 11L ) );
        assertThat( count.sum(), is( 2L ) );

        try
        {
            asyncApply.await();
            fail( "Should have thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), sameInstance( boo ) );
        }

        assertThat( sum.sum(), is( 11L ) );
        assertThat( count.sum(), is( 2L ) );
    }
}
