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
package org.neo4j.causalclustering.core.consensus.schedule;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.FakeClockJobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static org.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.SYNC_WAIT;
import static org.neo4j.causalclustering.core.consensus.schedule.TimerServiceTest.Timers.TIMER_A;
import static org.neo4j.causalclustering.core.consensus.schedule.TimerServiceTest.Timers.TIMER_B;

public class TimerServiceTest
{
    private final JobScheduler.Group group = new JobScheduler.Group( "Test" );

    private final TimeoutHandler handlerA = mock( TimeoutHandler.class );
    private final TimeoutHandler handlerB = mock( TimeoutHandler.class );

    private final FakeClockJobScheduler scheduler = new FakeClockJobScheduler();
    private final TimerService timerService = new TimerService( scheduler, NullLogProvider.getInstance() );

    private final Timer timerA = timerService.create( TIMER_A, group, handlerA );
    private final Timer timerB = timerService.create( TIMER_B, group, handlerB );

    @Test
    public void shouldNotInvokeHandlerBeforeTimeout() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1000, MILLISECONDS ) );

        // when
        scheduler.forward( 999, MILLISECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
    }

    @Test
    public void shouldInvokeHandlerOnTimeout() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1000, MILLISECONDS ) );

        // when
        scheduler.forward( 1000, MILLISECONDS );

        // then
        verify( handlerA, times( 1 ) ).onTimeout( any() );
    }

    @Test
    public void shouldInvokeHandlerAfterTimeout() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );

        // when
        scheduler.forward( 1001, MILLISECONDS );

        // then
        verify( handlerA, times( 1 ) ).onTimeout( any() );
    }

    @Test
    public void shouldInvokeMultipleHandlersOnDifferentTimeouts() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );
        timerB.set( fixedTimeout( 2, SECONDS ) );

        // when
        scheduler.forward( 1, SECONDS );

        // then
        verify( handlerA, times( 1 ) ).onTimeout( timerA );
        verify( handlerB, never() ).onTimeout( any() );

        // given
        reset( handlerA );
        reset( handlerB );

        // when
        scheduler.forward( 1, SECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
        verify( handlerB, times( 1 ) ).onTimeout( timerB );

        // given
        reset( handlerA );
        reset( handlerB );

        // when
        scheduler.forward( 1, SECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
        verify( handlerB, never() ).onTimeout( any() );
    }

    @Test
    public void shouldInvokeMultipleHandlersOnSameTimeout() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );
        timerB.set( fixedTimeout( 1, SECONDS ) );

        // when
        scheduler.forward( 1, SECONDS );

        // then
        verify( handlerA, times( 1 ) ).onTimeout( timerA );
        verify( handlerB, times( 1 ) ).onTimeout( timerB );
    }

    @Test
    public void shouldInvokeTimersOnExplicitInvocation() throws Exception
    {
        // when
        timerService.invoke( TIMER_A );

        // then
        verify( handlerA, times( 1 ) ).onTimeout( timerA );
        verify( handlerB, never() ).onTimeout( any() );

        // given
        reset( handlerA );
        reset( handlerB );

        // when
        timerService.invoke( TIMER_B );

        // then
        verify( handlerA, never() ).onTimeout( any() );
        verify( handlerB, times( 1 ) ).onTimeout( timerB );
    }

    @Test
    public void shouldTimeoutAfterReset() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );

        // when
        scheduler.forward( 900, MILLISECONDS );
        timerA.reset();
        scheduler.forward( 900, MILLISECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );

        // then
        scheduler.forward( 100, MILLISECONDS );

        // when
        verify( handlerA, times( 1 ) ).onTimeout( any() );
    }

    @Test
    public void shouldTimeoutSingleTimeAfterMultipleResets() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );

        // when
        scheduler.forward( 900, MILLISECONDS );
        timerA.reset();
        scheduler.forward( 900, MILLISECONDS );
        timerA.reset();
        scheduler.forward( 900, MILLISECONDS );
        timerA.reset();
        scheduler.forward( 1000, MILLISECONDS );

        // then
        verify( handlerA, times( 1 ) ).onTimeout( any() );

        // when
        reset( handlerA );
        scheduler.forward( 5000, MILLISECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
    }

    @Test
    public void shouldNotInvokeCancelledTimer() throws Exception
    {
        // given
        timerA.set( fixedTimeout( 1, SECONDS ) );
        scheduler.forward( 900, MILLISECONDS );

        // when
        timerA.cancel( SYNC_WAIT );
        scheduler.forward( 100, MILLISECONDS );

        // then
        verify( handlerA, never() ).onTimeout( any() );
    }

    @Test
    public void shouldAwaitCancellationUnderRealScheduler() throws Throwable
    {
        // given
        Neo4jJobScheduler scheduler = new Neo4jJobScheduler();
        scheduler.init();
        scheduler.start();

        TimerService timerService = new TimerService( scheduler, FormattedLogProvider.toOutputStream( System.out ) );

        CountDownLatch started = new CountDownLatch( 1 );
        CountDownLatch finished = new CountDownLatch( 1 );

        TimeoutHandler handlerA = timer ->
        {
            started.countDown();
            finished.await();
        };

        TimeoutHandler handlerB = timer -> finished.countDown();

        Timer timerA = timerService.create( Timers.TIMER_A, group, handlerA );
        timerA.set( fixedTimeout( 0, SECONDS ) );
        started.await();

        Timer timerB = timerService.create( Timers.TIMER_B, group, handlerB );
        timerB.set( fixedTimeout( 2, SECONDS ) );

        // when
        timerA.cancel( SYNC_WAIT );

        // then
        assertEquals( 0, finished.getCount() );

        // cleanup
        scheduler.stop();
        scheduler.shutdown();
    }

    @Test
    public void shouldBeAbleToCancelBeforeHandlingWithRealScheduler() throws Throwable
    {
        // given
        Neo4jJobScheduler scheduler = new Neo4jJobScheduler();
        scheduler.init();
        scheduler.start();

        TimerService timerService = new TimerService( scheduler, FormattedLogProvider.toOutputStream( System.out ) );

        TimeoutHandler handlerA = timer -> {};

        Timer timer = timerService.create( Timers.TIMER_A, group, handlerA );
        timer.set( fixedTimeout( 2, SECONDS ) );

        // when
        timer.cancel( SYNC_WAIT );

        // then: should not deadlock

        // cleanup
        scheduler.stop();
        scheduler.shutdown();
    }

    enum Timers implements TimerService.TimerName
    {
        TIMER_A,
        TIMER_B
    }
}
