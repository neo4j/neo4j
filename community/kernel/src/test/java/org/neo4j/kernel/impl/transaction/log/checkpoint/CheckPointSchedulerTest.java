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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.test.OnDemandJobScheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.checkPoint;

public class CheckPointSchedulerTest
{
    private final CheckPointer checkPointer = mock( CheckPointer.class );
    private final OnDemandJobScheduler jobScheduler = spy( new OnDemandJobScheduler() );

    @Test
    public void shouldScheduleTheCheckPointerJobJobOnStart() throws Throwable
    {
        // given
        CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, jobScheduler, 20l );

        assertNull( jobScheduler.getJob() );

        // when
        scheduler.start();

        // then
        assertNotNull( jobScheduler.getJob() );
        verify( jobScheduler, times( 1 ) ).schedule( eq( checkPoint ), any( Runnable.class ),
                eq( 20l ), eq( TimeUnit.MILLISECONDS ) );
    }

    @Test
    public void shouldRescheduleTheJobAfterARun() throws Throwable
    {
        // given
        CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, jobScheduler, 20l );

        assertNull( jobScheduler.getJob() );

        scheduler.start();

        Runnable scheduledJob = jobScheduler.getJob();
        assertNotNull( scheduledJob );

        // when
        jobScheduler.runJob();

        // then
        verify( jobScheduler, times( 2 ) ).schedule( eq( checkPoint ), any( Runnable.class ),
                eq( 20l ), eq( TimeUnit.MILLISECONDS ) );
        verify( checkPointer, times( 1 )).checkPointIfNeeded();
        assertEquals( scheduledJob, jobScheduler.getJob() );
    }

    @Test
    public void shouldNotRescheduleAJobWhenStopped() throws Throwable
    {
        // given
        CheckPointScheduler scheduler = new CheckPointScheduler( checkPointer, jobScheduler, 20l );

        assertNull( jobScheduler.getJob() );

        scheduler.start();

        assertNotNull( jobScheduler.getJob() );

        // when
        scheduler.stop();

        // then
        assertNull( jobScheduler.getJob() );
    }
}
