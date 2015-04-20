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
package org.neo4j.kernel.impl.util;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.kernel.impl.util.JobScheduler.Group.indexPopulation;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Neo4jJobSchedulerTest
{
    private Neo4jJobScheduler scheduler;
    private AtomicInteger invocations;

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

    @Before
    public void initInvocation()
    {
        invocations = new AtomicInteger( 0 );
    }

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
        scheduler = new Neo4jJobScheduler();

        // When
        scheduler.init();
        scheduler.scheduleRecurring( indexPopulation, countInvocationsJob, period, MILLISECONDS );
        awaitFirstInvocation();
        sleep( period * count - period / 2 );
        scheduler.shutdown();

        // Then
        int actualInvocations = invocations.get();
        assertEquals( count, actualInvocations );

        sleep( period );
        assertThat( invocations.get(), equalTo(actualInvocations) );
    }

    @Test
    public void shouldCancelRecurringJob() throws Exception
    {
        // Given
        long period = 2;
        scheduler = new Neo4jJobScheduler();

        scheduler.init();
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
        sleep( period*100 );
        assertThat( invocations.get(), equalTo(recorded) );
    }

    private void awaitFirstInvocation()
    {
        while ( invocations.get() == 0 )
        {   // Wait for the job to start running
            Thread.yield();
        }
    }
}
