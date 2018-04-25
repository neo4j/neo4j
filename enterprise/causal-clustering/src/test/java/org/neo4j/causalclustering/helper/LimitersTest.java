/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.helper;

import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.helper.Limiters.rateLimiter;

public class LimitersTest
{

    private final Duration ETERNITY = Duration.ofDays( 1000 );

    @Test
    public void shouldRateLimitCalls()
    {
        // given
        int intervalMillis = 10;
        FakeClock clock = Clocks.fakeClock();

        Consumer<Runnable> cap = rateLimiter( Duration.ofMillis( intervalMillis ), clock );
        AtomicInteger cnt = new AtomicInteger();
        Runnable op = cnt::incrementAndGet;

        // when
        cap.accept( op );
        cap.accept( op );
        cap.accept( op );

        // then
        assertThat( cnt.get(), equalTo( 1 ) );

        // when
        clock.forward( intervalMillis, MILLISECONDS );
        cap.accept( op );
        cap.accept( op );
        cap.accept( op );

        // then
        assertThat( cnt.get(), equalTo( 2 ) );

        // when
        clock.forward( 1000 * intervalMillis, MILLISECONDS );
        cap.accept( op );
        cap.accept( op );
        cap.accept( op );

        // then
        assertThat( cnt.get(), equalTo( 3 ) );
    }

    @Test
    public void shouldOnlyAllowOneThreadPerInterval() throws Exception
    {
        // given
        int intervalMillis = 10;
        int nThreads = 10;
        int iterations = 100;

        FakeClock clock = Clocks.fakeClock();
        Consumer<Runnable> cap = rateLimiter( Duration.ofMillis( intervalMillis ), clock );
        AtomicInteger cnt = new AtomicInteger();
        Runnable op = cnt::incrementAndGet;

        for ( int iteration = 1; iteration <= iterations; iteration++ )
        {
            // given
            clock.forward( intervalMillis, MILLISECONDS );
            CountDownLatch latch = new CountDownLatch( 1 );

            ExecutorService es = Executors.newCachedThreadPool();
            for ( int j = 0; j < nThreads; j++ )
            {
                es.submit( () -> {
                    try
                    {
                        latch.await();
                    }
                    catch ( InterruptedException e )
                    {
                        e.printStackTrace();
                    }
                    cap.accept( op );
                } );
            }

            // when
            latch.countDown();
            es.shutdown();
            es.awaitTermination( 10, SECONDS );

            // then
            assertThat( cnt.get(), equalTo( iteration ) );
        }
    }

    @Test
    public void distinctRateLimitersOperateIndependently() throws Exception
    {
        // given
        Limiters limiters = new Limiters( Clocks.fakeClock() );
        AtomicInteger cnt = new AtomicInteger();

        Consumer<Runnable> rateLimiterA = limiters.rateLimiter( "A", ETERNITY );
        Consumer<Runnable> rateLimiterB = limiters.rateLimiter( "B", ETERNITY );

        // when
        rateLimiterA.accept( cnt::incrementAndGet );
        rateLimiterA.accept( cnt::incrementAndGet );
        rateLimiterA.accept( cnt::incrementAndGet );

        rateLimiterB.accept( cnt::incrementAndGet );
        rateLimiterB.accept( cnt::incrementAndGet );
        rateLimiterB.accept( cnt::incrementAndGet );

        // then
        assertEquals( 2, cnt.get() );
    }

    @Test
    public void shouldReturnSameRateLimiterForSameHandle() throws Exception
    {
        // given
        Limiters limiters = new Limiters( Clocks.fakeClock() );
        AtomicInteger cnt = new AtomicInteger();

        Consumer<Runnable> rateLimiterA = limiters.rateLimiter( "SAME", ETERNITY );
        Consumer<Runnable> rateLimiterB = limiters.rateLimiter( "SAME", ETERNITY );

        // when
        rateLimiterA.accept( cnt::incrementAndGet );
        rateLimiterA.accept( cnt::incrementAndGet );

        rateLimiterB.accept( cnt::incrementAndGet );
        rateLimiterB.accept( cnt::incrementAndGet );

        // then
        assertEquals( 1, cnt.get() );
    }
}
