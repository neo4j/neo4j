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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.coreedge.raft.ScheduledTimeoutServiceTest.Timeouts.FOOBAR;

public class ScheduledTimeoutServiceTest
{
    enum Timeouts implements TimeoutService.TimeoutName
    {
        FOOBAR
    }

    @Test
    public void shouldTimeOutAfterTimeoutPeriod() throws Throwable
    {
        // given
        final AtomicLong timeoutCount = new AtomicLong();

        ScheduledTimeoutService timeoutService = new ScheduledTimeoutService();

        timeoutService.create( FOOBAR, 1000, 0, new TimeoutService.TimeoutHandler()
        {
            @Override
            public void onTimeout( TimeoutService.Timeout timeout )
            {
                timeoutCount.incrementAndGet();
            }
        } );

        timeoutService.init();
        timeoutService.start();

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

        ScheduledTimeoutService timeoutService = new ScheduledTimeoutService();

        TimeoutService.Timeout timeout = timeoutService.create( FOOBAR, 1000, 0, new TimeoutService.TimeoutHandler()
        {
            @Override
            public void onTimeout( TimeoutService.Timeout timeout )
            {
                timeoutCount.incrementAndGet();
            }
        } );

        timeoutService.init();
        timeoutService.start();

        // when
        Thread.sleep( 700 );
        timeout.renew();
        Thread.sleep( 500 );

        // then
        assertThat( timeoutCount.get(), equalTo( 0L ) );
    }
}
