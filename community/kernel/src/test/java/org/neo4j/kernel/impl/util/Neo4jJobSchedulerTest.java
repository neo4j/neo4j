/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobScheduler.JobHandle;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.scheduler.JobScheduler.Groups.indexPopulation;
import static org.neo4j.test.ReflectionUtil.replaceValueInPrivateField;

public class Neo4jJobSchedulerTest
{
    private final AtomicInteger invocations = new AtomicInteger();
    private final LifeSupport life = new LifeSupport();
    private final Neo4jJobScheduler scheduler = life.add( new Neo4jJobScheduler() );

    private final Runnable countInvocationsJob = () ->
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
    };

    @After
    public void stopScheduler() throws Throwable
    {
        life.shutdown();
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
        JobScheduler.JobHandle jobHandle =
                scheduler.scheduleRecurring( indexPopulation, countInvocationsJob, period, MILLISECONDS );
        awaitFirstInvocation();

        // When
        jobHandle.cancel( false );

        // Then
        int recorded = invocations.get();
        sleep( period * 100 );
        assertThat( invocations.get(), equalTo( recorded ) );
    }

    @Test
    public void shouldRunWithDelay() throws Throwable
    {
        // Given
        life.start();

        final AtomicLong runTime = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch( 1 );

        long time = System.nanoTime();

        scheduler.schedule( new JobScheduler.Group( "group" ), () ->
        {
            runTime.set( System.nanoTime() );
            latch.countDown();
        }, 100, TimeUnit.MILLISECONDS );

        latch.await();

        assertTrue( time + TimeUnit.MILLISECONDS.toNanos( 100 ) <= runTime.get() );
    }

    @Test
    public void shouldNotSwallowExceptions() throws Exception
    {
        // given
        Neo4jJobScheduler neo4jJobScheduler = new Neo4jJobScheduler();
        neo4jJobScheduler.init();

        // mock stuff
        ExecutorService es = mock( ExecutorService.class );
        doThrow( new RuntimeException( "ES" ) ).when( es ).shutdown();
        replaceValueInPrivateField( neo4jJobScheduler, "globalPool", ExecutorService.class, es );
        ScheduledThreadPoolExecutor stpe = mock( ScheduledThreadPoolExecutor.class );
        doThrow( new RuntimeException( "STPE" ) ).when( stpe ).shutdown();
        replaceValueInPrivateField( neo4jJobScheduler, "scheduledExecutor", ScheduledThreadPoolExecutor.class, stpe );

        // when
        try
        {
            neo4jJobScheduler.shutdown();
        }
        catch ( RuntimeException t )
        {
            // then
            assertEquals( "Unable to shut down job scheduler properly.", t.getMessage() );
            Throwable inner = t.getCause();
            assertEquals( "ES", inner.getMessage() );
            assertEquals( 1, inner.getSuppressed().length );
            assertEquals( "STPE", inner.getSuppressed()[0].getMessage() );
        }
    }

    @Test
    public void shouldNotifyCancelListeners() throws Exception
    {
        // GIVEN
        Neo4jJobScheduler neo4jJobScheduler = new Neo4jJobScheduler();
        neo4jJobScheduler.init();

        // WHEN
        AtomicBoolean halted = new AtomicBoolean();
        Runnable job = () ->
        {
            while ( !halted.get() )
            {
                LockSupport.parkNanos( MILLISECONDS.toNanos( 10 ) );
            }
        };
        JobHandle handle = neo4jJobScheduler.schedule( indexPopulation, job );
        handle.registerCancelListener( mayBeInterrupted -> halted.set( true ) );
        handle.cancel( false );

        // THEN
        assertTrue( halted.get() );
        neo4jJobScheduler.shutdown();
    }

    private void awaitFirstInvocation()
    {
        while ( invocations.get() == 0 )
        {   // Wait for the job to start running
            Thread.yield();
        }
    }
}
