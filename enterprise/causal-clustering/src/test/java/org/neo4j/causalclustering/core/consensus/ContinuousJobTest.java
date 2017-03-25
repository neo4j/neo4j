/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.kernel.impl.util.JobScheduler.Group;
import org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ContinuousJobTest
{
    private static final long DEFAULT_TIMEOUT_MS = 15_000;
    private Group jobGroup = new Group( "test", SchedulingStrategy.NEW_THREAD );

    @Test
    public void shouldRunJobContinuously() throws Throwable
    {
        // given
        CountDownLatch latch = new CountDownLatch( 10 );
        Runnable task = latch::countDown;

        Neo4jJobScheduler scheduler = new Neo4jJobScheduler();
        ContinuousJob continuousJob = new ContinuousJob( scheduler, jobGroup, task, NullLogProvider.getInstance() );

        // when
        try ( Lifespan ignored = new Lifespan( scheduler, continuousJob ) )
        {
            //then
            assertTrue( latch.await( DEFAULT_TIMEOUT_MS, MILLISECONDS ) );
        }
    }

    @Test
    public void shouldTerminateOnStop() throws Exception
    {
        // given: this task is gonna take >20 ms total
        Semaphore semaphore = new Semaphore( -20 );

        Runnable task = () ->
        {
            LockSupport.parkNanos( 1_000_000 ); // 1 ms
            semaphore.release();
        };

        Neo4jJobScheduler scheduler = new Neo4jJobScheduler();
        ContinuousJob continuousJob = new ContinuousJob( scheduler, jobGroup, task, NullLogProvider.getInstance() );

        // when
        long startTime = System.currentTimeMillis();
        try ( Lifespan ignored = new Lifespan( scheduler, continuousJob ) )
        {
            semaphore.acquireUninterruptibly();
        }
        long runningTime = System.currentTimeMillis() - startTime;

        // then
        assertThat( runningTime, lessThan( DEFAULT_TIMEOUT_MS ) );

        //noinspection StatementWithEmptyBody
        while ( semaphore.tryAcquire() )
        {
            // consume all outstanding permits
        }

        // no more permits should be granted
        semaphore.tryAcquire( 10, MILLISECONDS );
    }
}
