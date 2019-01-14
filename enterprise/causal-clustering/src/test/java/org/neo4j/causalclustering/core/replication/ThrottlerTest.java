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
package org.neo4j.causalclustering.core.replication;

import org.junit.After;
import org.junit.Test;

import java.util.HashSet;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.ThrowingSupplier;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ThrottlerTest
{
    private ExecutorService es = Executors.newCachedThreadPool();
    private ExecutorCompletionService<Integer> ecs = new ExecutorCompletionService<>( es );

    @After
    public void after() throws InterruptedException
    {
        es.shutdown();
        es.awaitTermination( 1, MINUTES );
    }

    @Test
    public void shouldAllowInvocationWhenCreditsAvailable() throws Exception
    {
        // given
        Throttler throttler = new Throttler( 1000 );
        Counter counter = new Counter();

        // when
        int count = ecs.submit( () -> throttler.invoke( counter, 1000 ) ).get( 1, MINUTES );

        // then
        assertEquals( 1, count );
    }

    @Test
    public void shouldAllowSequentialInvocations() throws Exception
    {
        // given
        Throttler throttler = new Throttler( 1000 );
        Counter counter = new Counter();

        // when
        HashSet<Integer> set = new HashSet<>();
        set.add( ecs.submit( () -> throttler.invoke( counter, 1000 ) ).get( 1, MINUTES ) );
        set.add( ecs.submit( () -> throttler.invoke( counter, 1000 ) ).get( 1, MINUTES ) );
        set.add( ecs.submit( () -> throttler.invoke( counter, 1000 ) ).get( 1, MINUTES ) );

        // then
        assertThat( set, hasItems( 1, 2, 3 ) );
    }

    @Test
    public void shouldAllowOneInvocationOversteppingTheLimit() throws Exception
    {
        // given
        Throttler throttler = new Throttler( 1000 );
        Counter counter = new Counter();
        ecs.submit( () -> throttler.invoke( counter, 500 ) ).get( 1, MINUTES );
        assertEventually( counter::count, equalTo( 1 ), 1, MINUTES );

        // when
        int count = ecs.submit( () -> throttler.invoke( counter, 800 ) ).get( 1, MINUTES );

        // then
        assertEquals( 2, count );
    }

    @Test
    public void shouldBlockInvocationWhenCreditsNotAvailable() throws Exception
    {
        // given
        Throttler throttler = new Throttler( 1000 );
        Blocker blocker = new Blocker();
        Future<Integer> call1 = ecs.submit( () -> throttler.invoke( blocker, 1200 ) );
        assertEventually( blocker::count, equalTo( 1 ), 1, MINUTES );

        // when
        Future<Integer> call2 = ecs.submit( () -> throttler.invoke( blocker, 800 ) );
        Thread.sleep( 10 );

        // then
        assertEquals( 1, blocker.count() );
        assertFalse( call1.isDone() );
        assertFalse( call2.isDone() );

        // cleanup
        blocker.release( 2 );
        call1.get( 1, MINUTES );
        call2.get( 1, MINUTES );
    }

    @Test
    public void shouldInvokeWhenCreditsBecomeAvailable() throws Exception
    {
        // given
        Throttler throttler = new Throttler( 1000 );
        Blocker blocker = new Blocker();

        // when
        Future<Integer> call1 = ecs.submit( () -> throttler.invoke( blocker, 1200 ) );

        // then
        assertEventually( blocker::count, equalTo( 1 ), 1, MINUTES );

        // when
        blocker.release( 1 );
        Future<Integer> call2 = ecs.submit( () -> throttler.invoke( blocker, 800 ) );

        // then
        call1.get( 1, MINUTES );
        assertEventually( blocker::count, equalTo( 2 ), 1, MINUTES );
        assertFalse( call2.isDone() );

        // cleanup
        blocker.release( 1 );
        call2.get( 1, MINUTES );
    }

    @Test
    public void shouldInvokeMultipleWhenCreditsBecomeAvailable() throws Exception
    {
        // given
        Throttler throttler = new Throttler( 1000 );
        Blocker blocker = new Blocker();

        // when
        Future<Integer> call1 = ecs.submit( () -> throttler.invoke( blocker, 2000 ) );

        // then
        assertEventually( blocker::count, equalTo( 1 ), 1, MINUTES );

        // when
        Future<Integer> call2 = ecs.submit( () -> throttler.invoke( blocker, 400 ) );
        Future<Integer> call3 = ecs.submit( () -> throttler.invoke( blocker, 400 ) );
        Thread.sleep( 10 );

        // then
        assertEquals( 1, blocker.count() );

        // when
        blocker.release( 1 );

        // then
        call1.get( 1, MINUTES );
        assertEventually( blocker::count, equalTo( 3 ), 1, MINUTES );

        // cleanup
        blocker.release( 2 );
        call2.get( 1, MINUTES );
        call3.get( 1, MINUTES );
    }

    static class Counter implements ThrowingSupplier<Integer,Exception>
    {
        private final AtomicInteger count = new AtomicInteger();

        @Override
        public Integer get()
        {
            return count.incrementAndGet();
        }

        public int count()
        {
            return count.get();
        }
    }

    static class Blocker implements ThrowingSupplier<Integer,Exception>
    {
        private final Semaphore semaphore = new Semaphore( 0 );
        private final AtomicInteger count = new AtomicInteger();

        @Override
        public Integer get() throws Exception
        {
            count.incrementAndGet();
            semaphore.acquire();
            return semaphore.availablePermits();
        }

        void release( int permits )
        {
            semaphore.release( permits );
        }

        int count()
        {
            return count.get();
        }
    }
}
