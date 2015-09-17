/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.concurrent;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CounterTest
{
    @Test
    public void incrementsMustAddUp() throws Exception
    {
        Counter counter = Counter.create();
        assertThat( counter.sum(), is( 0L ) );
        counter.increment();
        assertThat( counter.sum(), is( 1L ) );
        counter.increment();
        assertThat( counter.sum(), is( 2L ) );
    }

    @Test
    public void additionsMustAddUp() throws Exception
    {
        Counter counter = Counter.create();
        counter.add( 2 );
        assertThat( counter.sum(), is( 2L ) );
        counter.increment();
        assertThat( counter.sum(), is( 3L ) );
        counter.add( 4 );
        assertThat( counter.sum(), is( 7L ) );
    }

    @Test
    public void concurrentIncrementsAndAdditionsMustAddUp() throws Exception
    {
        int threads = 16;
        ExecutorService executor = Executors.newFixedThreadPool( threads );
        final int iterations = 20_000;
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        final CountDownLatch endLatch = new CountDownLatch( threads );
        final Counter counter = Counter.create();
        Callable<Void> worker = new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                startLatch.await();
                for ( int i = 0; i < iterations; i++ )
                {
                    counter.increment();
                }
                endLatch.countDown();
                return null;
            }
        };
        for ( int i = 0; i < threads; i++ )
        {
            executor.submit( worker );
        }

        startLatch.countDown();
        endLatch.await();
        executor.shutdown();

        assertThat( counter.sum(), is( (long) threads * iterations ) );
    }

    @Test
    public void overflowMustWrapAround() throws Exception
    {
        Counter counter = Counter.create();
        counter.add( Long.MAX_VALUE );
        assertThat( counter.sum(), is( Long.MAX_VALUE ) );
        counter.increment();
        assertThat( counter.sum(), is( Long.MIN_VALUE ) );
    }
}
