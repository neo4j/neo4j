/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.transaction.log.rotation.monitor.DefaultLogRotationMonitor;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultTransactionTracerTest
{
    private final FakeClock clock = Clocks.fakeClock();
    private final LogRotationMonitor monitor = new DefaultLogRotationMonitor();

    @Test
    void shouldComputeStartEndAndTotalTimeForLogRotation()
    {
        DefaultTransactionTracer tracer = new DefaultTransactionTracer( monitor, clock );

        triggerEvent( tracer, 20 );

        assertEquals( 1, monitor.numberOfLogRotations() );
        assertEquals( 20, monitor.logRotationAccumulatedTotalTimeMillis() );

        triggerEvent( tracer, 30 );

        // should reset the total time value whenever read
        assertEquals( 2, monitor.numberOfLogRotations() );
        assertEquals( 50, monitor.logRotationAccumulatedTotalTimeMillis() );
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
    }
}
