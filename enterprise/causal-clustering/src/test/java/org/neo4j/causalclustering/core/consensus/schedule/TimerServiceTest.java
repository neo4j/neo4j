/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.schedule;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.FakeClockJobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
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
        CentralJobScheduler scheduler = new CentralJobScheduler();
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
        CentralJobScheduler scheduler = new CentralJobScheduler();
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
