/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.FakeClock;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ResourcePoolTest
{
    private static final int TIMEOUT_MILLIS = 100;
    private static final int TIMEOUT_EXCEED_MILLIS = TIMEOUT_MILLIS + 10;

    @Test
    public void shouldNotReuseBrokenInstances() throws Exception
    {
        ResourcePool<Something> pool = new ResourcePool<Something>( 5 )
        {
            @Override
            protected Something create()
            {
                return new Something();
            }

            @Override
            protected boolean isAlive( Something resource )
            {
                return !resource.closed;
            }
        };

        Something somethingFirst = pool.acquire();
        somethingFirst.doStuff();
        pool.release();

        Something something = pool.acquire();
        assertEquals( somethingFirst, something );
        something.doStuff();
        something.close();
        pool.release();

        Something somethingElse = pool.acquire();
        assertFalse( something == somethingElse );
        somethingElse.doStuff();
    }

    @Test
    public void shouldTimeoutGracefully() throws InterruptedException
    {
        FakeClock clock = new FakeClock();

        ResourcePool.CheckStrategy timeStrategy = new ResourcePool.CheckStrategy.TimeoutCheckStrategy( TIMEOUT_MILLIS, clock );

        while ( clock.currentTimeMillis() <= TIMEOUT_MILLIS )
        {
            assertFalse( timeStrategy.shouldCheck() );
            clock.forward( 10, TimeUnit.MILLISECONDS );
        }

        assertTrue( timeStrategy.shouldCheck() );

        clock.forward( 1, TimeUnit.MILLISECONDS );
        assertFalse( timeStrategy.shouldCheck() );
    }

    @Test
    public void shouldBuildUpGracefullyUntilReachedMinPoolSize() throws InterruptedException
    {
        // GIVEN
        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final ResourcePool<Something> pool = getResourcePool( stateMonitor, clock, 5 );

        // WHEN
        acquireFromPool( pool, 5 );

        // THEN
        assertEquals( -1, stateMonitor.currentPeakSize.get() );
        assertEquals( -1, stateMonitor.targetSize.get() ); // that means the target size was not updated
        assertEquals( 0, stateMonitor.disposed.get() ); // no disposed happened, since the count to update is 10
    }

    @Test
    public void shouldBuildUpGracefullyWhilePassingMinPoolSizeBeforeTimerRings() throws InterruptedException
    {
        // GIVEN
        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final ResourcePool<Something> pool = getResourcePool( stateMonitor, clock, 5 );

        // WHEN
        acquireFromPool( pool, 15 );

        // THEN
        assertEquals( -1, stateMonitor.currentPeakSize.get() );
        assertEquals( 15, stateMonitor.created.get() );
        assertEquals( -1, stateMonitor.targetSize.get() );
        assertEquals( 0, stateMonitor.disposed.get() );
    }

    @Test
    public void shouldUpdateTargetSizeWhenSpikesOccur() throws Exception
    {
        // given
        final int poolMinSize = 5;
        final int poolMaxSize = 10;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final ResourcePool<Something> pool = getResourcePool( stateMonitor, clock, poolMinSize );

        // when
        List<ResourceHolder> holders = acquireFromPool( pool, poolMaxSize );
        exceedTimeout( clock );
        holders.addAll( acquireFromPool( pool, 1 ) ); // Needed to trigger the alarm

        // then
        assertEquals( poolMaxSize + 1, stateMonitor.currentPeakSize.get() );
        // We have not released anything, so targetSize will not be reduced
        assertEquals( poolMaxSize + 1, stateMonitor.targetSize.get() ); // + 1 from the acquire

        for ( ResourceHolder holder : holders )
        {
            holder.end();
        }
    }

    @Test
    public void shouldKeepSmallPeakAndNeverDisposeIfAcquireAndReleaseContinuously() throws Exception
    {
        // given
        final int poolMinSize = 1;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final ResourcePool<Something> pool = getResourcePool( stateMonitor, clock, poolMinSize );

        // when
        for ( int i = 0; i < 200; i++ )
        {
            List<ResourceHolder> newOnes = acquireFromPool( pool, 1 );
            CountDownLatch release = new CountDownLatch( newOnes.size() );
            for ( ResourceHolder newOne : newOnes )
            {
                newOne.release( release );
            }
            release.await();
        }

        // then
        assertEquals( -1, stateMonitor.currentPeakSize.get() ); // no alarm has rung, -1 is the default
        assertEquals( 1, stateMonitor.created.get() );
        assertEquals( 0, stateMonitor.disposed.get() ); // we should always be below min size, so 0 dispose calls
    }

    @Test
    public void shouldSlowlyReduceTheNumberOfResourcesInThePoolWhenResourcesAreReleased() throws Exception
    {
        // given
        final int poolMinSize = 50;
        final int poolMaxSize = 200;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final SomethingResourcePool pool = getResourcePool( stateMonitor, clock, poolMinSize );

        acquireResourcesAndExceedTimeout( pool, clock, poolMaxSize );

        // when
        // After the peak, stay below MIN_SIZE concurrent usage, using up all already present resources.
        exceedTimeout( clock );
        for ( int i = 0; i < poolMaxSize; i++ )
        {
            acquireFromPool( pool, 1 ).get( 0 ).release();
        }

        // then
        // currentPeakSize must have reset from the latest check to minimum size.
        assertEquals( 1, stateMonitor.currentPeakSize.get() ); // because of timeout
        // targetSize must be set to MIN_SIZE since currentPeakSize was that 2 checks ago and didn't increase
        assertEquals( poolMinSize, stateMonitor.targetSize.get() );
        // Only pooled resources must be used, disposing what is in excess
        // +1 that was used to trigger exceed timeout check
        assertEquals( poolMinSize, pool.unusedSize() );
        assertEquals( poolMaxSize - poolMinSize + 1, stateMonitor.disposed.get() );
    }

    @Test
    public void shouldMaintainPoolHigherThenMinSizeWhenPeekUsagePasses() throws Exception
    {
        // given
        final int poolMinSize = 50;
        final int poolMaxSize = 200;
        final int afterPeekPoolSize = 90;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final SomethingResourcePool pool = getResourcePool( stateMonitor, clock, poolMinSize );

        acquireResourcesAndExceedTimeout( pool, clock, poolMaxSize );

        // when
        // After the peak, stay at afterPeekPoolSize concurrent usage, using up all already present resources in the process
        // but also keeping the high watermark above the minimum size
        exceedTimeout( clock );
        // Requires some rounds to happen, since there is constant racing between releasing and acquiring which does
        // not always result in reaping of resources, as there is reuse
        for ( int i = 0; i < 10; i++ )
        {
            // The latch is necessary to reduce races between batches
            CountDownLatch release = new CountDownLatch( afterPeekPoolSize );
            for ( ResourceHolder holder : acquireFromPool( pool, afterPeekPoolSize ) )
            {
                holder.release( release );
            }
            release.await();
            exceedTimeout( clock );
        }

        // then
        // currentPeakSize should be at afterPeekPoolSize
        assertEquals( afterPeekPoolSize, stateMonitor.currentPeakSize.get() );
        // target size too
        assertEquals( afterPeekPoolSize, stateMonitor.targetSize.get() );
        // only the excess from the maximum size down to after peek usage size must have been disposed
        // +1 that was used to trigger exceed timeout check
        assertEquals( afterPeekPoolSize, pool.unusedSize() );
        assertThat( stateMonitor.disposed.get(), greaterThanOrEqualTo( poolMaxSize - afterPeekPoolSize + 1 )  );
    }

    @Test
    public void shouldReclaimAndRecreateWhenUsageGoesDownBetweenSpikes() throws Exception
    {
        // given
        final int poolMinSize = 50;
        final int bellowPoolMinSize = poolMinSize / 5;
        final int poolMaxSize = 200;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final SomethingResourcePool pool = getResourcePool( stateMonitor, clock, poolMinSize );

        acquireResourcesAndExceedTimeout( pool, clock, poolMaxSize );

        // when
        // After the peak, stay well below concurrent usage, using up all already present resources in the process
        exceedTimeout( clock );
        // Requires some rounds to happen, since there is constant racing between releasing and acquiring which does
        // not always result in reaping of resources, as there is reuse
        for ( int i = 0; i < 30; i++ )
        {
            // The latch is necessary to reduce races between batches
            CountDownLatch release = new CountDownLatch( bellowPoolMinSize );
            for ( ResourceHolder holder : acquireFromPool( pool, bellowPoolMinSize ) )
            {
                holder.release( release );
            }
            release.await();
            exceedTimeout( clock );
        }

        // then
        // currentPeakSize should be at bellowPoolMinSize
        assertEquals( bellowPoolMinSize, stateMonitor.currentPeakSize.get() );
        // target size should remain at pool min size
        assertEquals( poolMinSize, stateMonitor.targetSize.get() );
        assertEquals( poolMinSize, pool.unusedSize() );
        // only the excess from the pool max size down to min size must have been disposed
        // +1 that was used to trigger initial exceed timeout check
        assertEquals( poolMaxSize - poolMinSize + 1, stateMonitor.disposed.get() );

        stateMonitor.created.set( 0 );
        stateMonitor.disposed.set( 0 );

        // when
        // After the lull, recreate a peak
        acquireResourcesAndExceedTimeout( pool, clock, poolMaxSize );

        // then
        assertEquals( poolMaxSize - poolMinSize + 1, stateMonitor.created.get() );
        assertEquals( 0, stateMonitor.disposed.get() );
    }

    private void exceedTimeout( FakeClock clock )
    {
        clock.forward( TIMEOUT_EXCEED_MILLIS, TimeUnit.MILLISECONDS );
    }

    private void acquireResourcesAndExceedTimeout( ResourcePool<Something> pool,
            FakeClock clock, int resourcesToAcquire ) throws InterruptedException
    {
        List<ResourceHolder> holders = new LinkedList<>();
        holders.addAll( acquireFromPool( pool, resourcesToAcquire ) );

        exceedTimeout( clock );

        // "Ring the bell" only on acquisition, of course.
        holders.addAll( acquireFromPool( pool, 1 ) );

        for ( ResourceHolder holder : holders )
        {
            holder.release();
        }
    }

    private SomethingResourcePool getResourcePool( StatefulMonitor stateMonitor, FakeClock clock, int minSize )
    {
        ResourcePool.CheckStrategy.TimeoutCheckStrategy timeoutCheckStrategy =
                new ResourcePool.CheckStrategy.TimeoutCheckStrategy( TIMEOUT_MILLIS, clock );
        return new SomethingResourcePool( minSize, timeoutCheckStrategy, stateMonitor );
    }

    private List<ResourceHolder> acquireFromPool( ResourcePool pool, int resourcesToAcquire ) throws InterruptedException
    {
        List<ResourceHolder> acquirers = new LinkedList<>();
        final CountDownLatch latch = new CountDownLatch( resourcesToAcquire );
        for ( int i = 0; i < resourcesToAcquire; i++ )
        {
            ResourceHolder holder = new ResourceHolder( pool, latch );
            Thread t = new Thread( holder );
            acquirers.add( holder );
            t.start();
        }
        latch.await();
        return acquirers;
    }

    private static class SomethingResourcePool extends ResourcePool<Something>
    {
        public SomethingResourcePool( int minSize, CheckStrategy checkStrategy, StatefulMonitor stateMonitor )
        {
            super( minSize, checkStrategy, stateMonitor );
        }

        @Override
        protected Something create()
        {
            return new Something();
        }

        @Override
        protected boolean isAlive( Something resource )
        {
            return !resource.closed;
        }

        public int unusedSize()
        {
            return unused.size();
        }
    }

    private class ResourceHolder implements Runnable
    {
        private final Semaphore latch = new Semaphore( 0 );
        private final CountDownLatch released = new CountDownLatch( 1 );
        private final CountDownLatch onAcquire;
        private final ResourcePool pool;
        private final AtomicBoolean release = new AtomicBoolean();
        private volatile Thread runner;

        private ResourceHolder( ResourcePool pool, CountDownLatch onAcquire )
        {
            this.pool = pool;
            this.onAcquire = onAcquire;
        }

        @Override
        public void run()
        {
            runner = Thread.currentThread();
            try
            {
                pool.acquire();
                onAcquire.countDown();
                try
                {
                    latch.acquire();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                if ( release.get() )
                {
                    pool.release();
                    released.countDown();
                }
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
            }
        }

        public void release()
        {
            this.release.set( true);
            latch.release();
            try
            {
                released.await();

                Thread runner;
                do
                {
                    // Wait to observe thread running this ResourceHolder.
                    // If we don't, then the thread can continue running for a little while after releasing, which can
                    // result in racy changes to the StatefulMonitor.
                    runner = this.runner;
                }
                while ( runner == null );
                runner.join();
            }
            catch ( InterruptedException e )
            {
                e.printStackTrace();
            }
        }

        public void release( CountDownLatch releaseLatch )
        {
            release();
            releaseLatch.countDown();
        }

        public void end()
        {
            this.release.set( false);
            latch.release();
        }
    }

    private class StatefulMonitor implements ResourcePool.Monitor<Something>
    {
        public AtomicInteger currentPeakSize = new AtomicInteger(-1);
        public AtomicInteger targetSize = new AtomicInteger( -1 );
        public AtomicInteger created = new AtomicInteger( 0 );
        public AtomicInteger acquired = new AtomicInteger( 0 );
        public AtomicInteger disposed = new AtomicInteger( 0 );

        @Override
        public void updatedCurrentPeakSize( int currentPeakSize )
        {
            this.currentPeakSize.set( currentPeakSize );
        }

        @Override
        public void updatedTargetSize( int targetSize )
        {
            this.targetSize.set( targetSize );
        }

        @Override
        public void created( Something something )
        {
            this.created.incrementAndGet();
        }

        @Override
        public void acquired( Something something )
        {
            this.acquired.incrementAndGet();
        }

        @Override
        public void disposed( Something something )
        {
            this.disposed.incrementAndGet();
        }
    }

    private static class Something
    {
        private boolean closed;

        public void doStuff() throws Exception
        {
            if ( closed )
            {
                throw new Exception( "Closed" );
            }
        }

        public void close()
        {
            this.closed = true;
        }
    }

}
