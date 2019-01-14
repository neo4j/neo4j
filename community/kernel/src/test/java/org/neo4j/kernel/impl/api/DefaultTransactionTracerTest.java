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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.api.DefaultTransactionTracer.Monitor;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class DefaultTransactionTracerTest
{
    private final FakeClock clock = Clocks.fakeClock();
    private final OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
    private final Monitor monitor = mock( Monitor.class );

    @Test
    public void shouldComputeStartEndAndTotalTimeForLogRotation()
    {
        DefaultTransactionTracer tracer = new DefaultTransactionTracer( clock, monitor, jobScheduler );

        triggerEvent( tracer, 20 );

        assertEquals( 1, tracer.numberOfLogRotationEvents() );
        assertEquals( 20, tracer.logRotationAccumulatedTotalTimeMillis() );
        verify( monitor, times( 1 ) ).lastLogRotationEventDuration( 20L );

        triggerEvent( tracer, 30 );

        // should reset the total time value whenever read
        assertEquals( 2, tracer.numberOfLogRotationEvents() );
        assertEquals( 50, tracer.logRotationAccumulatedTotalTimeMillis() );
        verify( monitor, times( 1 ) ).lastLogRotationEventDuration( 30L );
    }

    @Test
    public void shouldReturnMinusOneIfNoDataIsAvailableForLogRotation()
    {
        DefaultTransactionTracer tracer = new DefaultTransactionTracer( clock, monitor, jobScheduler );

        jobScheduler.runJob();

        assertEquals( 0, tracer.numberOfLogRotationEvents() );
        assertEquals( 0, tracer.logRotationAccumulatedTotalTimeMillis() );
        verifyZeroInteractions( monitor );
    }

    private void triggerEvent( DefaultTransactionTracer tracer, int eventDuration )
    {
        try ( TransactionEvent txEvent = tracer.beginTransaction() )
        {
            try ( CommitEvent commitEvent = txEvent.beginCommitEvent() )
            {
                try ( LogAppendEvent logAppendEvent = commitEvent.beginLogAppend() )
                {
                    clock.forward( ThreadLocalRandom.current().nextLong( 200 ), TimeUnit.MILLISECONDS );
                    try ( LogRotateEvent event = logAppendEvent.beginLogRotate() )
                    {
                        clock.forward( eventDuration, TimeUnit.MILLISECONDS );
                    }
                }
            }
        }

        jobScheduler.runJob();
    }
}
