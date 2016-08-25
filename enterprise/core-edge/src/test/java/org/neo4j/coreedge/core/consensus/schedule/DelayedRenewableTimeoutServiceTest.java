/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.core.consensus.schedule;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.function.Predicates;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class DelayedRenewableTimeoutServiceTest
{
    // The base timeout for timers in tests, should be short.
    private final long TIMEOUT_MS = 100;
    // The maximum time for awaiting conditions in tests.. used for failing tests.
    private final long LONG_TIME_MS = 30000;
    // Used for allowing for example a timer to fire slightly earlier then asked for
    // since in general tests can assert on tardiness but not on promptness.
    // The error could be due to the timer service itself or the test.
    private final long ERROR_MS = 1;

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

        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService( Clocks.systemClock(),
                getInstance() );

        long startTime = System.currentTimeMillis();
        timeoutService.create( Timeouts.FOOBAR, TIMEOUT_MS, 0, timeout -> timeoutCount.incrementAndGet() );
        life.add( timeoutService );

        Predicates.await( timeoutCount::get, count -> count == 1, LONG_TIME_MS, MILLISECONDS, 1, MILLISECONDS );

        long runTime = System.currentTimeMillis() - startTime;
        assertThat( runTime, greaterThanOrEqualTo( TIMEOUT_MS - ERROR_MS ) );
    }

    @Test
    public void shouldNotTimeOutWhenRenewedWithinTimeoutPeriod() throws Throwable
    {
        // given
        final AtomicLong timeoutCount = new AtomicLong();

        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService( Clocks.systemClock(),
                getInstance() );

        long startTime = System.currentTimeMillis();
        RenewableTimeoutService.RenewableTimeout timeout =
                timeoutService.create( Timeouts.FOOBAR, TIMEOUT_MS, 0, timeout1 -> timeoutCount.incrementAndGet() );

        life.add( timeoutService );

        // when
        Thread.sleep( TIMEOUT_MS/2 );
        long timeoutCountSample = timeoutCount.get();
        long sampleTime = System.currentTimeMillis();

        if( sampleTime < startTime + TIMEOUT_MS )
        {
            assertThat( timeoutCountSample, is( 0L ) );
        }

        long renewTime = System.currentTimeMillis();
        timeout.renew();

        if( System.currentTimeMillis() < startTime + TIMEOUT_MS )
        {
            // we managed to renew before it expired
            Predicates.await( timeoutCount::get, count -> count == 1, LONG_TIME_MS, MILLISECONDS, 1, MILLISECONDS );
            assertThat( System.currentTimeMillis(), greaterThanOrEqualTo( renewTime + TIMEOUT_MS - ERROR_MS ) );
        }
    }

    @Test
    public void shouldNotTimeOutWhenStopped() throws Throwable
    {
        // given
        final AtomicLong timeoutCount = new AtomicLong();

        FakeClock clock = Clocks.fakeClock();

        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService( clock,
                NullLogProvider.getInstance() );

        RenewableTimeoutService.RenewableTimeout timeout = timeoutService.create( Timeouts.FOOBAR, TIMEOUT_MS, 0, t -> timeoutCount
                .incrementAndGet() );

        life.add( timeoutService );

        clock.forward( TIMEOUT_MS, MILLISECONDS );
        Predicates.await( timeoutCount::get, count -> count == 1, LONG_TIME_MS, MILLISECONDS, 1, MILLISECONDS );

        // when
        timeoutService.stop();
        timeoutService.shutdown();

        timeout.renew();
        Thread.sleep( TIMEOUT_MS/2 );
        clock.forward( TIMEOUT_MS, MILLISECONDS );
        Thread.sleep( TIMEOUT_MS/2 );

        // then
        assertThat( timeoutCount.get(), equalTo( 1L ) );
    }

    @Test
    public void shouldNotDeadLockWhenCancellingDuringExpiryHandling() throws Throwable
    {
        // given: a timeout handler that blocks on a latch
        final CountDownLatch latch = new CountDownLatch( 1 );

        FakeClock clock = Clocks.fakeClock();
        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService( clock,
                NullLogProvider.getInstance() );

        RenewableTimeoutService.RenewableTimeout timeout = timeoutService.create( Timeouts.FOOBAR, TIMEOUT_MS, 0,
                handler -> {
                    try
                    {
                        latch.await( LONG_TIME_MS, MILLISECONDS );
                    }
                    catch ( InterruptedException ignored )
                    {
                    }
                } );

        life.add( timeoutService );

        clock.forward( TIMEOUT_MS, MILLISECONDS );
        Thread.sleep( TIMEOUT_MS/2 ); // to allow the scheduled timeout to fire and get stuck in the latch

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
        // so the following join should finish quicker than the latch expiry, and the cancelThread should be dead
        cancelThread.join( LONG_TIME_MS/2 );
        assertFalse( cancelThread.isAlive() );

        // cleanup
        latch.countDown();
        cancelThread.interrupt();
    }
}
