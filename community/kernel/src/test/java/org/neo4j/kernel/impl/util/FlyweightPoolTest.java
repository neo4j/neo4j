/**
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
package org.neo4j.kernel.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.neo4j.helpers.FakeClock;

public class FlyweightPoolTest
{
    @Test
    public void shouldTimeoutGracefully() throws InterruptedException
    {
        FakeClock clock = new FakeClock();

        FlyweightPool.CheckStrategy timeStrategy = new FlyweightPool.CheckStrategy.TimeoutCheckStrategy( 100, clock );

        while ( clock.currentTimeMillis() <= 100 )
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
        final FlyweightPool<Object> pool = getFlyweightPool( stateMonitor, clock, 5 );

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
        final FlyweightPool<Object> pool = getFlyweightPool( stateMonitor, clock, 5 );

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
        final int MIN_SIZE = 5;
        final int MAX_SIZE = 10;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final FlyweightPool<Object> pool = getFlyweightPool( stateMonitor, clock, MIN_SIZE );

        // when
        List<FlyweightHolder<Object>> holders = acquireFromPool( pool, MAX_SIZE );
        clock.forward( 110, TimeUnit.MILLISECONDS );
        holders.addAll( acquireFromPool( pool, 1 ) ); // Needed to trigger the alarm

        // then
        assertEquals( MAX_SIZE + 1, stateMonitor.currentPeakSize.get() );
        // We have not released anything, so targetSize will not be reduced
        assertEquals( MAX_SIZE + 1, stateMonitor.targetSize.get() ); // + 1 from the acquire

        for ( FlyweightHolder holder : holders )
        {
            holder.end();
        }
    }

    @Test
    public void shouldKeepSmallPeakAndNeverDisposeIfAcquireAndReleaseContinuously() throws Exception
    {
        // given
        final int MIN_SIZE = 1;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final FlyweightPool<Object> pool = getFlyweightPool( stateMonitor, clock, MIN_SIZE );

        // when
        for ( int i = 0; i < 200; i++ )
        {
            List<FlyweightHolder<Object>> newOnes = acquireFromPool( pool, 1 );
            CountDownLatch release = new CountDownLatch( newOnes.size() );
            for ( FlyweightHolder newOne : newOnes )
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
    public void shouldSlowlyReduceTheNumberOfFlyweightsInThePoolWhenFlyweightsAreReleased() throws Exception
    {
        // given
        final int MIN_SIZE = 50;
        final int MAX_SIZE = 200;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final FlyweightPool<Object> pool = getFlyweightPool( stateMonitor, clock, MIN_SIZE );
        List<FlyweightHolder<Object>> holders = new LinkedList<>();

        buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects( MAX_SIZE, clock, pool, holders );

        // when
        // After the peak, stay below MIN_SIZE concurrent usage, using up all already present Flyweights.
        clock.forward( 110, TimeUnit.MILLISECONDS );
        for ( int i = 0; i < MAX_SIZE; i++ )
        {
            acquireFromPool( pool, 1 ).get( 0 ).release();
        }

        // then

        // currentPeakSize must have reset from the latest alarm to MIN_SIZE.
        assertEquals( 1, stateMonitor.currentPeakSize.get() ); // Alarm
        // targetSize must be set to MIN_SIZE since currentPeakSize was that 2 alarms ago and didn't increase
        assertEquals( MIN_SIZE, stateMonitor.targetSize.get() );
        // Only pooled Flyweights must be used, disposing what is in excess
        // +1 for the alarm from buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects
        assertEquals( MAX_SIZE - MIN_SIZE + 1, stateMonitor.disposed.get() );
    }

    @Test
    public void shouldMaintainPoolAtHighWatermarkWhenConcurrentUsagePassesMinSize() throws Exception
    {
        // given
        final int MIN_SIZE = 50;
        final int MAX_SIZE = 200;
        final int MID_SIZE = 90;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final FlyweightPool<Object> pool = getFlyweightPool( stateMonitor, clock, MIN_SIZE );
        List<FlyweightHolder<Object>> holders = new LinkedList<>();

        buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects( MAX_SIZE, clock, pool, holders );

        // when
        // After the peak, stay at MID_SIZE concurrent usage, using up all already present Flyweights in the process
        // but also keeping the high watermark above the MIN_SIZE
        clock.forward( 110, TimeUnit.MILLISECONDS );
        // Requires some rounds to happen, since there is constant racing between releasing and acquiring which does
        // not always result in reaping of Flyweights, as there is reuse
        for ( int i = 0; i < 10; i++ )
        {
            // The latch is necessary to reduce races between batches
            CountDownLatch release = new CountDownLatch( MID_SIZE );
            for ( FlyweightHolder holder : acquireFromPool( pool, MID_SIZE ) )
            {
                holder.release( release );
            }
            release.await();
            clock.forward( 110, TimeUnit.MILLISECONDS );
        }

        // then
        // currentPeakSize should be at MID_SIZE
        assertEquals( MID_SIZE, stateMonitor.currentPeakSize.get() );
        // target size too
        assertEquals( MID_SIZE, stateMonitor.targetSize.get() );
        // only the excess from the MAX_SIZE down to mid size must have been disposed
        // +1 for the alarm from buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects
        assertEquals( MAX_SIZE - MID_SIZE + 1, stateMonitor.disposed.get() );
    }

    @Test
    public void shouldReclaimAndRecreateWhenLullBetweenSpikesOccurs() throws Exception
    {
        // given
        final int MIN_SIZE = 50;
        final int BELOW_MIN_SIZE = MIN_SIZE / 5;
        final int MAX_SIZE = 200;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final FlyweightPool<Object> pool = getFlyweightPool( stateMonitor, clock, MIN_SIZE );
        List<FlyweightHolder<Object>> holders = new LinkedList<>();

        buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects( MAX_SIZE, clock, pool, holders );

        // when
        // After the peak, stay well below concurrent usage, using up all already present Flyweights in the process
        clock.forward( 110, TimeUnit.MILLISECONDS );
        // Requires some rounds to happen, since there is constant racing between releasing and acquiring which does
        // not always result in reaping of Flyweights, as there is reuse
        for ( int i = 0; i < 30; i++ )
        {
            // The latch is necessary to reduce races between batches
            CountDownLatch release = new CountDownLatch( BELOW_MIN_SIZE );
            for ( FlyweightHolder holder : acquireFromPool( pool, BELOW_MIN_SIZE ) )
            {
                holder.release( release );
            }
            release.await();
            clock.forward( 110, TimeUnit.MILLISECONDS );
        }

        // then
        // currentPeakSize should be at MIN_SIZE / 5
        assertEquals( BELOW_MIN_SIZE, stateMonitor.currentPeakSize.get() );
        // target size should remain at MIN_SIZE
        assertEquals( MIN_SIZE, stateMonitor.targetSize.get() );
        // only the excess from the MAX_SIZE down to min size must have been disposed
        // +1 for the alarm from buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects
        assertEquals( MAX_SIZE - MIN_SIZE + 1, stateMonitor.disposed.get() );

        stateMonitor.created.set( 0 );
        stateMonitor.disposed.set( 0 );

        // when
        // After the lull, recreate a peak
        buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects( MAX_SIZE, clock, pool, holders );

        // then
        assertEquals( MAX_SIZE - MIN_SIZE + 1, stateMonitor.created.get() );
        assertEquals( 0, stateMonitor.disposed.get() );

    }

    private void buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects( int MAX_SIZE, FakeClock clock,
                                                                              FlyweightPool<Object>
                                                                                      pool,
                                                                              List<FlyweightHolder<Object>> holders ) throws
            InterruptedException
    {
        holders.addAll( acquireFromPool( pool, MAX_SIZE ) );

        clock.forward( 110, TimeUnit.MILLISECONDS );

        // "Ring the bell" only on acquisition, of course.
        holders.addAll( acquireFromPool( pool, 1 ) );

        for ( FlyweightHolder holder : holders )
        {
            holder.release();
        }
    }

    private FlyweightPool<Object> getFlyweightPool( StatefulMonitor stateMonitor,
                                                     FakeClock clock,
                                                     int minSize )
    {
        return new FlyweightPool<Object>( minSize,
                new FlyweightPool.CheckStrategy.TimeoutCheckStrategy( 100, clock ), stateMonitor )
        {
            @Override
            protected Object create()
            {
                return new Object();
            }
        };
    }

    private <R> List<FlyweightHolder<R>>  acquireFromPool( final FlyweightPool<R> pool, int times ) throws InterruptedException
    {
        List<FlyweightHolder<R>> acquirers = new LinkedList<>();
        final CountDownLatch latch = new CountDownLatch( times );
        for ( int i = 0; i < times; i++ )
        {
            FlyweightHolder<R> holder = new FlyweightHolder<R>( pool, latch );
            Thread t = new Thread( holder );
            acquirers.add( holder );
            t.start();
        }
        latch.await();
        return acquirers;
    }

    private class FlyweightHolder<R> implements Runnable
    {
        private final Semaphore latch = new Semaphore( 0 );
        private final CountDownLatch released = new CountDownLatch( 1 );
        private final CountDownLatch onAcquire;
        private final FlyweightPool<R> pool;
        private final AtomicBoolean release = new AtomicBoolean(  );

        private FlyweightHolder( FlyweightPool<R> pool, CountDownLatch onAcquire )
        {
            this.pool = pool;
            this.onAcquire = onAcquire;
        }

        @Override
        public void run()
        {
            try
            {
                R acquired = pool.acquire();
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
                    pool.release( acquired );
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
            this.release.set( true );
            latch.release();
            try
            {
                released.await();
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
            this.release.set( false );
            latch.release();
        }
    }

    private class StatefulMonitor implements FlyweightPool.Monitor<Object>
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
        public void created( Object Object )
        {
            this.created.incrementAndGet();
        }

        @Override
        public void acquired( Object Object )
        {
            this.acquired.incrementAndGet();
        }

        @Override
        public void disposed( Object Object )
        {
            this.disposed.incrementAndGet();
        }
    }
}
