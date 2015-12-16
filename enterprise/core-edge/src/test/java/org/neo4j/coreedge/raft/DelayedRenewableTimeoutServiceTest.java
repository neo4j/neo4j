/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class DelayedRenewableTimeoutServiceTest
{
    private final LifeSupport life = new LifeSupport();

    @Before
    public void startupLife()
    {
        life.init();
        life.start();
    }

    @After
    public void shutdownLife()
    {
        life.stop();
        life.shutdown();
    }

    enum Timeouts implements RenewableTimeoutService.TimeoutName
    {
        FOOBAR
    }

    @Test
    public void shouldTimeOutAfterTimeoutPeriod() throws Throwable
    {
        // given
        final AtomicLong timeoutCount = new AtomicLong();

        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService( Clock.SYSTEM_CLOCK, NullLogProvider.getInstance() );

        timeoutService.create( Timeouts.FOOBAR, 1000, 0, new RenewableTimeoutService.TimeoutHandler()
        {
            @Override
            public void onTimeout( RenewableTimeoutService.RenewableTimeout timeout )
            {
                timeoutCount.incrementAndGet();
            }
        } );

        life.add( timeoutService );

        // when
        Thread.sleep( 500 );
        //then
        assertThat( timeoutCount.get(), equalTo( 0L ) );
        //when
        Thread.sleep( 750 );
        // then
        assertThat( timeoutCount.get(), equalTo( 1L ) );
    }

    @Test
    public void shouldNotTimeOutWhenRenewedWithinTimeoutPeriod() throws Throwable
    {
        // given
        final AtomicLong timeoutCount = new AtomicLong();

        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService( Clock.SYSTEM_CLOCK, NullLogProvider.getInstance() );

        RenewableTimeoutService.RenewableTimeout timeout = timeoutService.create( Timeouts.FOOBAR, 1000, 0, new RenewableTimeoutService.TimeoutHandler()
        {
            @Override
            public void onTimeout( RenewableTimeoutService.RenewableTimeout timeout )
            {
                timeoutCount.incrementAndGet();
            }
        } );

        life.add( timeoutService );

        // when
        Thread.sleep( 700 );
        timeout.renew();
        Thread.sleep( 500 );

        // then
        assertThat( timeoutCount.get(), equalTo( 0L ) );
    }

    @Test
    public void shouldNotTimeOutWhenStopped() throws Throwable
    {
        // given
        final AtomicLong timeoutCount = new AtomicLong();

        FakeClock clock = new FakeClock();

        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService( clock, NullLogProvider.getInstance() );

        RenewableTimeoutService.RenewableTimeout timeout = timeoutService.create( Timeouts.FOOBAR, 1000, 0, t -> timeoutCount
                .incrementAndGet() );

        life.add( timeoutService );

        clock.forward( 1000, MILLISECONDS );
        Thread.sleep( 5 ); // to make sure the scheduled thread has checked time elapsed

        assertThat( timeoutCount.get(), equalTo( 1L ) );

        timeoutService.stop();
        timeoutService.shutdown();

        timeout.renew();
        Thread.sleep( 5 );
        clock.forward( 1000, MILLISECONDS );
        Thread.sleep( 5 );

        assertThat( timeoutCount.get(), equalTo( 1L ) );
    }

    @Test
    public void shouldNotDeadLockWhenCancellingDuringExpiryHandling() throws Throwable
    {
        // given: a timeout handler that blocks on a latch
        final CountDownLatch latch = new CountDownLatch( 1 );

        FakeClock clock = new FakeClock();
        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService( clock, NullLogProvider.getInstance() );

        int TIMEOUT_MS = 1000;
        RenewableTimeoutService.RenewableTimeout timeout = timeoutService.create( Timeouts.FOOBAR, TIMEOUT_MS, 0, handler -> {
            try
            {
                latch.await( 10_000, TimeUnit.MILLISECONDS );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        } );

        life.add( timeoutService );

        clock.forward( TIMEOUT_MS, MILLISECONDS );
        Thread.sleep( 5 ); // to make sure the scheduled thread has checked time elapsed

        // given: another thread that wants to cancel the timeout while the handler is in progress
        Thread cancelThread = new Thread()
        {
            @Override
            public void run()
            {
                timeout.cancel(); // this would previously deadlock, because the timeout service was stuck handling the handler callback
            }
        };

        // when: we cancel the timeout, then it should not deadlock, and the latch be immediately released
        cancelThread.start();
        // so the following join should finish immediately and the cancelThread should be dead
        cancelThread.join( 5_000 );
        assertFalse( cancelThread.isAlive() );

        // cleanup
        latch.countDown();
        cancelThread.interrupt();
        timeoutService.stop();
        timeoutService.shutdown();
    }
}
