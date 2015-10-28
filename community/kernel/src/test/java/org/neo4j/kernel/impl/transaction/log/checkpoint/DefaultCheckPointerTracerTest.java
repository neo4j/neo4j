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

import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;

import static org.junit.Assert.assertEquals;

public class DefaultCheckPointerTracerTest
{
    private final FakeClock clock = new FakeClock();

    @Test
    public void shouldComputeStartEndAndTotalTime() throws Throwable
    {
        DefaultCheckPointerTracer tracer = new DefaultCheckPointerTracer( clock );
        clock.forward( 10, TimeUnit.MILLISECONDS );

        try ( LogCheckPointEvent event = tracer.beginCheckPoint() )
        {
            clock.forward( 20, TimeUnit.MILLISECONDS );
        }

        assertEquals( 10, tracer.lastCheckPointStartTime() );
        assertEquals( 30, tracer.lastCheckPointEndTime() );
        assertEquals( 20, tracer.lastCheckPointTotalTime() );
    }

    @Test
    public void shouldReturnMinusOneIfNoDataIsAvailable() throws Throwable
    {
        DefaultCheckPointerTracer tracer = new DefaultCheckPointerTracer( clock );
        assertEquals( -1, tracer.lastCheckPointStartTime() );
        assertEquals( -1, tracer.lastCheckPointEndTime() );
        assertEquals( -1, tracer.lastCheckPointTotalTime() );
    }
}
