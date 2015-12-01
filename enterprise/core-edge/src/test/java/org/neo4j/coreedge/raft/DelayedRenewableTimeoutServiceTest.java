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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.ArtificialClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.coreedge.raft.DelayedRenewableTimeoutServiceTest.Timeouts.FOOBAR;

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

        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService();

        timeoutService.create( FOOBAR, 1000, 0, new RenewableTimeoutService.TimeoutHandler()
        {
            @Override
            public void onTimeout( RenewableTimeoutService.Timeout timeout )
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

        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService();

        RenewableTimeoutService.Timeout timeout = timeoutService.create( FOOBAR, 1000, 0, new RenewableTimeoutService.TimeoutHandler()
        {
            @Override
            public void onTimeout( RenewableTimeoutService.Timeout timeout )
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

        ArtificialClock clock = new ArtificialClock();

        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService( clock );

        RenewableTimeoutService.Timeout timeout1 = timeoutService.create( FOOBAR, 1000, 0, timeout -> timeoutCount
                .incrementAndGet() );

        life.add( timeoutService );

        clock.progress( 1000, MILLISECONDS );
        Thread.sleep( 5 ); // to make sure the scheduled thread has checked time elapsed

        assertThat( timeoutCount.get(), equalTo( 1L ) );

        timeoutService.stop();
        timeoutService.shutdown();

        timeout1.renew();
        Thread.sleep( 5 );
        clock.progress( 1000, MILLISECONDS );
        Thread.sleep( 5 );

        assertThat( timeoutCount.get(), equalTo( 1L ) );
    }
}
