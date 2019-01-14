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
package org.neo4j.util.concurrent;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;

class BinaryLatchTest
{
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterAll
    static void shutDownExecutor()
    {
        executor.shutdown();
    }

    @Test
    void releaseThenAwaitDoesNotBlock()
    {
        assertTimeout( ofSeconds( 3 ), () ->
        {
            BinaryLatch latch = new BinaryLatch();
            latch.release();
            latch.await();
        } );
    }

    @Test
    void releaseMustUnblockAwaiters()
    {
        assertTimeout( ofSeconds( 10 ), () ->
        {
            final BinaryLatch latch = new BinaryLatch();
            Runnable awaiter = latch::await;
            int awaiters = 24;
            Future<?>[] futures = new Future<?>[awaiters];
            for ( int i = 0; i < awaiters; i++ )
            {
                futures[i] = executor.submit( awaiter );
            }

            assertThrows( TimeoutException.class, () -> futures[0].get( 10, TimeUnit.MILLISECONDS ) );

            latch.release();

            for ( Future<?> future : futures )
            {
                future.get();
            }
        } );
    }

    @Test
    void stressLatch()
    {
        assertTimeout( ofSeconds( 60 ), () ->
        {
            final AtomicReference<BinaryLatch> latchRef = new AtomicReference<>( new BinaryLatch() );
            Runnable awaiter = () ->
            {
                BinaryLatch latch;
                while ( (latch = latchRef.get()) != null )
                {
                    latch.await();
                }
            };

            int awaiters = 6;
            Future<?>[] futures = new Future<?>[awaiters];
            for ( int i = 0; i < awaiters; i++ )
            {
                futures[i] = executor.submit( awaiter );
            }

            ThreadLocalRandom rng = ThreadLocalRandom.current();
            for ( int i = 0; i < 500000; i++ )
            {
                latchRef.getAndSet( new BinaryLatch() ).release();
                spin( rng.nextLong( 0, 10 ) );
            }

            latchRef.getAndSet( null ).release();

            // None of the tasks we started should get stuck, e.g. miss a release signal:
            for ( Future<?> future : futures )
            {
                future.get();
            }
        } );
    }

    private static void spin( long micros )
    {
        if ( micros == 0 )
        {
            return;
        }

        long now;
        long deadline = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos( micros );
        do
        {
            now = System.nanoTime();
        }
        while ( now < deadline );
    }
}
