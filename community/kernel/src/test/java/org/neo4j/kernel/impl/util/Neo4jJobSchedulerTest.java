/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.lifecycle.LifeSupport;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.util.JobScheduler.Group.THREAD_ID;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.indexPopulation;
import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.NEW_THREAD;
import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

public class Neo4jJobSchedulerTest
{
    private final AtomicInteger invocations = new AtomicInteger();
    private final LifeSupport life = new LifeSupport();
    private Neo4jJobScheduler scheduler = life.add( new Neo4jJobScheduler() );

    private Runnable countInvocationsJob = new Runnable()
    {
        @Override
        public void run()
        {
            try
            {
                invocations.incrementAndGet();
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
                throw launderedException( e );
            }
        }
    };

    @After
    public void stopScheduler() throws Throwable
    {
        scheduler.shutdown();
    }

    @Test
    public void shouldRunRecurringJob() throws Throwable
    {
        // Given
        long period = 1_000;
        int count = 2;
        life.start();

        // When
        scheduler.scheduleRecurring( indexPopulation, countInvocationsJob, period, MILLISECONDS );
        awaitFirstInvocation();
        sleep( period * count - period / 2 );
        scheduler.shutdown();

        // Then
        int actualInvocations = invocations.get();
        assertEquals( count, actualInvocations );

        sleep( period );
        assertThat( invocations.get(), equalTo( actualInvocations ) );
    }

    @Test
    public void shouldCancelRecurringJob() throws Exception
    {
        // Given
        long period = 2;
        life.start();
        JobScheduler.JobHandle jobHandle = scheduler.scheduleRecurring(
                indexPopulation,
                countInvocationsJob,
                period,
                MILLISECONDS );
        awaitFirstInvocation();

        // When
        jobHandle.cancel( false );

        // Then
        int recorded = invocations.get();
        sleep( period * 100 );
        assertThat( invocations.get(), equalTo( recorded ) );
    }

    @Test
    public void shouldRunJobInNewThread() throws Throwable
    {
        // Given
        life.start();

        // We start a thread that will signal when it's running, and remain running until we tell it to stop.
        // This way we can check and make sure a thread with the name we expect is live and well
        final CountDownLatch threadStarted = new CountDownLatch( 1 );
        final CountDownLatch unblockThread = new CountDownLatch( 1 );

        // When
        scheduler.schedule( new JobScheduler.Group( "MyGroup", NEW_THREAD ),
                waitForLatch( threadStarted, unblockThread ),
                stringMap( THREAD_ID, "MyTestThread" ) );
        threadStarted.await();

        // Then
        try
        {
            String threadName = "neo4j.MyGroup-MyTestThread";
            for ( String name : threadNames() )
            {
                if ( name.equals( threadName ) )
                {
                    return;
                }
            }
            fail( "Expected a thread named '" + threadName + "' in " + threadNames() );

        }
        finally
        {
            unblockThread.countDown();
        }
    }

    @Test
    public void shouldRunWithDelay() throws Throwable
    {
        // Given
        life.start();

        final AtomicLong runTime = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch( 1 );

        long time = System.nanoTime();

        scheduler.schedule( new JobScheduler.Group( "group", POOLED ), new Runnable()
        {
            @Override
            public void run()
            {
                runTime.set( System.nanoTime() );
                latch.countDown();
            }
        }, 100, TimeUnit.MILLISECONDS );

        latch.await();

        assertTrue( time + TimeUnit.MILLISECONDS.toNanos( 100 ) <= runTime.get() );
    }

    private List<String> threadNames()
    {
        List<String> names = new ArrayList<>();
        for ( Thread thread : Thread.getAllStackTraces().keySet() )
        {
            names.add( thread.getName() );
        }
        return names;
    }

    private Runnable waitForLatch( final CountDownLatch threadStarted, final CountDownLatch runUntil )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    threadStarted.countDown();
                    runUntil.await();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }

    private void awaitFirstInvocation()
    {
        while ( invocations.get() == 0 )
        {   // Wait for the job to start running
            Thread.yield();
        }
    }
}
