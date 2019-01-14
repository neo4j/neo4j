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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.transaction.log.checkpoint.DefaultCheckPointerTracer.Monitor;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class DefaultCheckPointerTracerTest
{
    private final FakeClock clock = Clocks.fakeClock();
    private final Monitor monitor = mock( Monitor.class );
    private final OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();

    @Test
    public void shouldCountEventsAnAccumulatedTotalTime()
    {
        DefaultCheckPointerTracer tracer = new DefaultCheckPointerTracer( clock, monitor, jobScheduler );

        triggerEvent( tracer, 20 );

        assertEquals( 1, tracer.numberOfCheckPointEvents() );
        assertEquals( 20, tracer.checkPointAccumulatedTotalTimeMillis() );
        verify( monitor, times( 1 ) ).lastCheckPointEventDuration( 20L );

        triggerEvent( tracer, 30 );

        assertEquals( 2, tracer.numberOfCheckPointEvents() );
        assertEquals( 50, tracer.checkPointAccumulatedTotalTimeMillis() );
        verify( monitor, times( 1 ) ).lastCheckPointEventDuration( 30L );
    }

    @Test
    public void shouldReturnZeroIfNoDataIsAvailable()
    {
        DefaultCheckPointerTracer tracer = new DefaultCheckPointerTracer( clock, monitor, jobScheduler );

        jobScheduler.runJob();

        assertEquals( 0, tracer.numberOfCheckPointEvents() );
        assertEquals( 0, tracer.checkPointAccumulatedTotalTimeMillis() );
        verifyZeroInteractions( monitor );
    }

    private void triggerEvent( DefaultCheckPointerTracer tracer, int eventDuration )
    {
        clock.forward( ThreadLocalRandom.current().nextLong( 200 ), TimeUnit.MILLISECONDS );
        try ( LogCheckPointEvent event = tracer.beginCheckPoint() )
        {
            clock.forward( eventDuration, TimeUnit.MILLISECONDS );
        }

        jobScheduler.runJob();
    }
}
