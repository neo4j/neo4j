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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;

import static org.junit.Assert.assertEquals;

public class DefaultTransactionTracerTest
{
    private final FakeClock clock = new FakeClock();

    @Test
    public void shouldComputeStartEndAndTotalTimeForLogRotation() throws Throwable
    {
        DefaultTransactionTracer tracer = new DefaultTransactionTracer( clock );

        try ( TransactionEvent txEvent = tracer.beginTransaction() )
        {
            try ( CommitEvent commitEvent = txEvent.beginCommitEvent() )
            {
                try ( LogAppendEvent logAppendEvent = commitEvent.beginLogAppend() )
                {
                    clock.forward( 10, TimeUnit.MILLISECONDS );
                    try ( LogRotateEvent event = logAppendEvent.beginLogRotate() )
                    {
                        clock.forward( 20, TimeUnit.MILLISECONDS );
                    }
                }
            }
        }

        assertEquals( 10, tracer.lastLogRotationStartTimeMillis() );
        assertEquals( 30, tracer.lastLogRotationEndTimeMillis() );
        assertEquals( 20, tracer.lastLogRotationTotalTimeMillis() );
    }

    @Test
    public void shouldReturnMinusOneIfNoDataIsAvailableForLogRotation() throws Throwable
    {
        DefaultTransactionTracer tracer = new DefaultTransactionTracer( clock );
        assertEquals( 0, tracer.lastLogRotationStartTimeMillis() );
        assertEquals( 0, tracer.lastLogRotationEndTimeMillis() );
        assertEquals( 0, tracer.lastLogRotationTotalTimeMillis() );
    }
}
