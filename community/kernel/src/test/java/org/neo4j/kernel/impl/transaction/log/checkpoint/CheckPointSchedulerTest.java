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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.scheduler.JobScheduler.Groups.checkPoint;

public class CheckPointSchedulerTest
{
    private final IOLimiter ioLimiter = mock( IOLimiter.class );
    private final CheckPointer checkPointer = mock( CheckPointer.class );
    private final OnDemandJobScheduler jobScheduler = spy( new OnDemandJobScheduler() );
    private final DatabaseHealth health = mock( DatabaseHealth.class );

    private static ExecutorService executor;

    @BeforeClass
    public static void setUpExecutor()
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void tearDownExecutor() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination( 30, TimeUnit.SECONDS );
    }

    @Test
    public void shouldScheduleTheCheckPointerJobOnStart()
    {
        // given
        CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, ioLimiter, jobScheduler, 20L, health );

        assertNull( jobScheduler.getJob() );

        // when
        scheduler.start();

        // then
        assertNotNull( jobScheduler.getJob() );
        verify( jobScheduler, times( 1 ) ).schedule( eq( checkPoint ), any( Runnable.class ),
                eq( 20L ), eq( TimeUnit.MILLISECONDS ) );
    }

    @Test
    public void shouldRescheduleTheJobAfterARun() throws Throwable
    {
        // given
        CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, ioLimiter, jobScheduler, 20L, health );

        assertNull( jobScheduler.getJob() );

        scheduler.start();

        Runnable scheduledJob = jobScheduler.getJob();
        assertNotNull( scheduledJob );

        // when
        jobScheduler.runJob();

        // then
        verify( jobScheduler, times( 2 ) ).schedule( eq( checkPoint ), any( Runnable.class ),
                eq( 20L ), eq( TimeUnit.MILLISECONDS ) );
        verify( checkPointer, times( 1 ) ).checkPointIfNeeded( any( TriggerInfo.class ) );
        assertEquals( scheduledJob, jobScheduler.getJob() );
    }

    @Test
    public void shouldNotRescheduleAJobWhenStopped()
    {
        // given
        CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, ioLimiter, jobScheduler, 20L, health );

        assertNull( jobScheduler.getJob() );

        scheduler.start();

        assertNotNull( jobScheduler.getJob() );

        // when
        scheduler.stop();

        // then
        assertNull( jobScheduler.getJob() );
    }

    @Test
    public void stoppedJobCantBeInvoked() throws Throwable
    {
        CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, ioLimiter, jobScheduler, 10L, health );
        scheduler.start();
        jobScheduler.runJob();

        // verify checkpoint was triggered
        verify( checkPointer ).checkPointIfNeeded( any( TriggerInfo.class ) );

        // simulate scheduled run that was triggered just before stop
        scheduler.stop();
        scheduler.start();
        jobScheduler.runJob();

        // checkpointer should not be invoked now because job stopped
        verifyNoMoreInteractions( checkPointer );
    }

    // Timeout as fallback safety if test deadlocks
    @Test( timeout = 60_000 )
    public void shouldWaitOnStopUntilTheRunningCheckpointIsDone() throws Throwable
    {
        // given
        final AtomicReference<Throwable> ex = new AtomicReference<>();
        final AtomicBoolean stoppedCompleted = new AtomicBoolean();
        final DoubleLatch checkPointerLatch = new DoubleLatch( 1 );
        OtherThreadExecutor<Void> otherThreadExecutor = new OtherThreadExecutor<>( "scheduler stopper", null );
        CheckPointer checkPointer = new CheckPointer()
        {
            @Override
            public long checkPointIfNeeded( TriggerInfo triggerInfo )
            {
                checkPointerLatch.startAndWaitForAllToStart();
                checkPointerLatch.waitForAllToFinish();
                return 42;
            }

            @Override
            public long tryCheckPoint( TriggerInfo triggerInfo )
            {
                throw new RuntimeException( "this should have not been called" );
            }

            @Override
            public long forceCheckPoint( TriggerInfo triggerInfo )
            {
                throw new RuntimeException( "this should have not been called" );
            }

            @Override
            public long lastCheckPointedTransactionId()
            {
                return 42;
            }
        };

        final CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, ioLimiter, jobScheduler, 20L, health );

        // when
        scheduler.start();

        Thread runCheckPointer = new Thread( jobScheduler::runJob );
        runCheckPointer.start();

        checkPointerLatch.waitForAllToStart();

        otherThreadExecutor.executeDontWait( (WorkerCommand<Void,Void>) state ->
        {
            try
            {
                scheduler.stop();
                stoppedCompleted.set( true );
            }
            catch ( Throwable throwable )
            {
                ex.set( throwable );
            }
            return null;
        } );
        otherThreadExecutor.waitUntilWaiting( details -> details.isAt( CheckPointScheduler.class, "waitOngoingCheckpointCompletion" ) );

        // then
        assertFalse( stoppedCompleted.get() );

        checkPointerLatch.finish();
        runCheckPointer.join();

        while ( !stoppedCompleted.get() )
        {
            Thread.sleep( 1 );
        }
        otherThreadExecutor.close();

        assertNull( ex.get() );
    }

    @Test
    public void shouldContinueThroughSporadicFailures()
    {
        // GIVEN
        ControlledCheckPointer checkPointer = new ControlledCheckPointer();
        CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, ioLimiter, jobScheduler, 1, health );
        scheduler.start();

        // WHEN/THEN
        for ( int i = 0; i < CheckPointScheduler.MAX_CONSECUTIVE_FAILURES_TOLERANCE * 2; i++ )
        {
            // Fail
            checkPointer.fail = true;
            jobScheduler.runJob();
            verifyZeroInteractions( health );

            // Succeed
            checkPointer.fail = false;
            jobScheduler.runJob();
            verifyZeroInteractions( health );
        }
    }

    @Test( timeout = 10_000 )
    public void checkpointOnStopShouldFlushAsFastAsPossible() throws Throwable
    {
        CheckableIOLimiter ioLimiter = new CheckableIOLimiter();
        CountDownLatch checkPointerLatch = new CountDownLatch( 1 );
        WaitUnlimitedCheckPointer checkPointer = new WaitUnlimitedCheckPointer( ioLimiter, checkPointerLatch );
        CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, ioLimiter, jobScheduler, 0L, health );
        scheduler.start();

        Future<?> checkpointerStarter = executor.submit( jobScheduler::runJob );

        checkPointerLatch.await();
        scheduler.stop();
        checkpointerStarter.get();

        assertTrue( "Checkpointer should be created.", checkPointer.isCheckpointCreated() );
        assertTrue( "Limiter should be enabled in the end.", ioLimiter.isLimitEnabled() );
    }

    @Test
    public void shouldCausePanicAfterSomeFailures() throws Throwable
    {
        // GIVEN
        RuntimeException[] failures = new RuntimeException[] {
                new RuntimeException( "First" ),
                new RuntimeException( "Second" ),
                new RuntimeException( "Third" ) };
        when( checkPointer.checkPointIfNeeded( any( TriggerInfo.class ) ) ).thenThrow( failures );
        CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, ioLimiter, jobScheduler, 1, health );
        scheduler.start();

        // WHEN
        for ( int i = 0; i < CheckPointScheduler.MAX_CONSECUTIVE_FAILURES_TOLERANCE - 1; i++ )
        {
            jobScheduler.runJob();
            verifyZeroInteractions( health );
        }

        try
        {
            jobScheduler.runJob();
            fail( "Should have failed" );
        }
        catch ( UnderlyingStorageException e )
        {
            // THEN
            assertEquals( Iterators.asSet( failures ), Iterators.asSet( e.getSuppressed() ) );
            verify( health ).panic( e );
        }
    }

    private static class ControlledCheckPointer implements CheckPointer
    {
        volatile boolean fail;

        @Override
        public long checkPointIfNeeded( TriggerInfo triggerInfo ) throws IOException
        {
            if ( fail )
            {
                throw new IOException( "Just failing" );
            }
            return 1;
        }

        @Override
        public long tryCheckPoint( TriggerInfo triggerInfo )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long forceCheckPoint( TriggerInfo triggerInfo )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long lastCheckPointedTransactionId()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class CheckableIOLimiter implements IOLimiter
    {
        private volatile boolean limitEnabled;

        @Override
        public long maybeLimitIO( long previousStamp, int recentlyCompletedIOs, Flushable flushable )
        {
            return 0;
        }

        @Override
        public void disableLimit()
        {
            limitEnabled = false;
        }

        @Override
        public void enableLimit()
        {
            limitEnabled = true;
        }

        boolean isLimitEnabled()
        {
            return limitEnabled;
        }
    }

    private static class WaitUnlimitedCheckPointer implements CheckPointer
    {
        private final CheckableIOLimiter ioLimiter;
        private final CountDownLatch latch;
        private volatile boolean checkpointCreated;

        WaitUnlimitedCheckPointer( CheckableIOLimiter ioLimiter, CountDownLatch latch )
        {
            this.ioLimiter = ioLimiter;
            this.latch = latch;
            checkpointCreated = false;
        }

        @Override
        public long checkPointIfNeeded( TriggerInfo triggerInfo )
        {
            latch.countDown();
            while ( ioLimiter.isLimitEnabled() )
            {
                //spin while limiter enabled
            }
            checkpointCreated = true;
            return 42;
        }

        @Override
        public long tryCheckPoint( TriggerInfo triggerInfo )
        {
            throw new UnsupportedOperationException( "This should have not been called" );
        }

        @Override
        public long forceCheckPoint( TriggerInfo triggerInfo )
        {
            throw new UnsupportedOperationException( "This should have not been called" );
        }

        @Override
        public long lastCheckPointedTransactionId()
        {
            return 0;
        }

        boolean isCheckpointCreated()
        {
            return checkpointCreated;
        }
    }
}
