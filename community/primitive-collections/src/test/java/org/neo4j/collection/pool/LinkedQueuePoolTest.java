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
package org.neo4j.collection.pool;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.neo4j.function.Factory;
import org.neo4j.function.LongSupplier;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LinkedQueuePoolTest
{
    @Test
    public void shouldTimeoutGracefully() throws InterruptedException
    {
        FakeClock clock = new FakeClock();

        LinkedQueuePool.CheckStrategy timeStrategy = new LinkedQueuePool.CheckStrategy.TimeoutCheckStrategy( 100, clock );

        while ( clock.getAsLong() <= 100 )
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
        final LinkedQueuePool<Object> pool = getLinkedQueuePool( stateMonitor, clock, 5 );

        // WHEN
        List<FlyweightHolder<Object>> flyweightHolders = acquireFromPool( pool, 5 );
        for ( FlyweightHolder<Object> flyweightHolder : flyweightHolders )
        {
            flyweightHolder.release();
        }

        // THEN
        // clock didn't tick, these two are not set
        assertEquals( -1, stateMonitor.currentPeakSize.get() );
        assertEquals( -1, stateMonitor.targetSize.get() );
        // no disposed happened, since the count to update is 5
        assertEquals( 0, stateMonitor.disposed.get() );
    }

    @Test
    public void shouldBuildUpGracefullyWhilePassingMinPoolSizeBeforeTimerRings() throws InterruptedException
    {
        // GIVEN
        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final LinkedQueuePool<Object> pool = getLinkedQueuePool( stateMonitor, clock, 5 );

        // WHEN
        List<FlyweightHolder<Object>> flyweightHolders = acquireFromPool( pool, 15 );
        for ( FlyweightHolder<Object> flyweightHolder : flyweightHolders )
        {
            flyweightHolder.release();
        }

        // THEN
        // The clock hasn't ticked, so these two should be unset
        assertEquals( -1, stateMonitor.currentPeakSize.get() );
        assertEquals( -1, stateMonitor.targetSize.get() );
        // We obviously created 15 threads
        assertEquals( 15, stateMonitor.created.get() );
        // And of those 10 are not needed and therefore disposed on release (min size is 5)
        assertEquals( 10, stateMonitor.disposed.get() );
    }

    @Test
    public void shouldUpdateTargetSizeWhenSpikesOccur() throws Exception
    {
        // given
        final int MIN_SIZE = 5;
        final int MAX_SIZE = 10;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final LinkedQueuePool<Object> pool = getLinkedQueuePool( stateMonitor, clock, MIN_SIZE );
        // when
        List<FlyweightHolder<Object>> holders = acquireFromPool( pool, MAX_SIZE );
        clock.forward( 110, TimeUnit.MILLISECONDS );
        holders.addAll( acquireFromPool( pool, 1 ) ); // Needed to trigger the alarm

        // then
        assertEquals( MAX_SIZE + 1, stateMonitor.currentPeakSize.get() );
        // We have not released anything, so targetSize will not be reduced
        assertEquals( MAX_SIZE + 1, stateMonitor.targetSize.get() ); // + 1 from the acquire
    }

    @Test
    public void shouldKeepSmallPeakAndNeverDisposeIfAcquireAndReleaseContinuously() throws Exception
    {
        // given
        final int MIN_SIZE = 1;

        StatefulMonitor stateMonitor = new StatefulMonitor();
        FakeClock clock = new FakeClock();
        final LinkedQueuePool<Object> pool = getLinkedQueuePool( stateMonitor, clock, MIN_SIZE );
        // when
        for ( int i = 0; i < 200; i++ )
        {
            List<FlyweightHolder<Object>> newOnes = acquireFromPool( pool, 1 );
            for ( FlyweightHolder newOne : newOnes )
            {
                newOne.release();
            }
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
        final LinkedQueuePool<Object> pool = getLinkedQueuePool( stateMonitor, clock, MIN_SIZE );
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
        final LinkedQueuePool<Object> pool = getLinkedQueuePool( stateMonitor, clock, MIN_SIZE );
        List<FlyweightHolder<Object>> holders = new LinkedList<>();

        // when
        buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects( MAX_SIZE, clock, pool, holders );

        // then
        assertEquals( MAX_SIZE + 1, stateMonitor.currentPeakSize.get() ); // the peak method above does +1 on the peak

        // when
        /* After the peak, stay at MID_SIZE concurrent usage, using up all already present Flyweights in the process
         * but also keeping the high watermark above the MIN_SIZE
         * We must do this at least twice, since the counter for disposed is incremented once per release, if appropriate,
         * and only after the clock has ticked. Essentially this is one loop for reducing the watermark down to
         * mid size and one more loop to dispose of all excess resources. That does indeed mean that there is a lag
         * of one clock tick before resources are disposed. If this is a bug or not remains to be seen.
         */
        for ( int i = 0; i < 2; i++ )
        {
            clock.forward( 110, TimeUnit.MILLISECONDS );
            for ( FlyweightHolder holder : acquireFromPool( pool, MID_SIZE ) )
            {
                holder.release();
            }
            clock.forward( 110, TimeUnit.MILLISECONDS );
            for ( FlyweightHolder holder : acquireFromPool( pool, MID_SIZE ) )
            {
                holder.release();
            }
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
        final LinkedQueuePool<Object> pool = getLinkedQueuePool( stateMonitor, clock, MIN_SIZE );
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
            for ( FlyweightHolder holder : acquireFromPool( pool, BELOW_MIN_SIZE ) )
            {
                holder.release( );
            }
            clock.forward( 110, TimeUnit.MILLISECONDS );
        }

        // then
        // currentPeakSize should be at MIN_SIZE / 5
        assertTrue( "Expected " + stateMonitor.currentPeakSize.get() + " <= " + BELOW_MIN_SIZE,
                stateMonitor.currentPeakSize.get() <= BELOW_MIN_SIZE );
        // target size should remain at MIN_SIZE
        assertEquals( MIN_SIZE, stateMonitor.targetSize.get() );
        // only the excess from the MAX_SIZE down to min size must have been disposed
        // +1 for the alarm from buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects
        assertEquals( MAX_SIZE - MIN_SIZE + 1, stateMonitor.disposed.get() );

        stateMonitor.created.set( 0 );
        stateMonitor.disposed.set( 0 );

        // when
        // After the lull, recreate a peak
        holders.addAll( acquireFromPool( pool, MAX_SIZE ) );

        // then
        assertEquals( MAX_SIZE - MIN_SIZE , stateMonitor.created.get() );
        assertEquals( 0, stateMonitor.disposed.get() );
    }

    private void buildAPeakOfAcquiredFlyweightsAndTriggerAlarmWithSideEffects( int MAX_SIZE, FakeClock clock,
                                                                              LinkedQueuePool<Object> pool,
                                                                              List<FlyweightHolder<Object>> holders )
            throws InterruptedException
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

    private LinkedQueuePool<Object> getLinkedQueuePool( StatefulMonitor stateMonitor, FakeClock clock, int minSize )
    {
        return new LinkedQueuePool<>( minSize, new Factory<Object>()
            {
                @Override
                public Object newInstance()
                {
                    return new Object();
                }
            },
            new LinkedQueuePool.CheckStrategy.TimeoutCheckStrategy( 100, clock ), stateMonitor );
    }

    private <R> List<FlyweightHolder<R>>  acquireFromPool( final LinkedQueuePool<R> pool, int times )
            throws InterruptedException
    {
        List<FlyweightHolder<R>> acquirers = new LinkedList<>();
        for ( int i = 0; i < times; i++ )
        {
            FlyweightHolder<R> holder = new FlyweightHolder<R>( pool );
            acquirers.add( holder );
            holder.run();
        }
        return acquirers;
    }

    private class FlyweightHolder<R> implements Runnable
    {
        private final LinkedQueuePool<R> pool;
        private R resource;

        private FlyweightHolder( LinkedQueuePool<R> pool )
        {
            this.pool = pool;
        }

        @Override
        public void run()
        {
            resource = pool.acquire();
        }

        public void release()
        {
            pool.release( resource );
        }
    }

    private class StatefulMonitor implements LinkedQueuePool.Monitor<Object>
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

    private static class FakeClock implements LongSupplier
    {
        @Override
        public long getAsLong()
        {
            return time;
        }

        private long time = 0;

        public void forward( long amount, TimeUnit timeUnit)
        {
            time = time + timeUnit.toMillis( amount );
        }
    }
}
