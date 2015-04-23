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

package org.neo4j.cypher.internal.compiler.v2_3.birk.profiling;

import org.junit.Test;

import org.neo4j.cypher.internal.compiler.v2_3.birk.QueryExecutionEvent;
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Id;

import static org.junit.Assert.assertEquals;

public class ProfilingTracerTest
{
    @Test
    public void shouldReportExecutionTimeOfQueryExecution() throws Exception
    {
        // given
        Clock clock = new Clock();
        Id query = new Id();
        ProfilingTracer tracer = new ProfilingTracer( clock );

        // when
        try ( QueryExecutionEvent event = tracer.executeQuery( query ) )
        {
            clock.progress( 516 );
        }

        // then
        assertEquals( 516, tracer.timeOf( query ) );
    }

    @Test
    public void shouldReportDbHitsOfQueryExecution() throws Exception
    {
        // given
        Id query = new Id();
        ProfilingTracer tracer = new ProfilingTracer();

        // when
        try ( QueryExecutionEvent event = tracer.executeQuery( query ) )
        {
            for ( int i = 0; i < 516; i++ )
            {
                event.dbHit();
            }
        }

        // then
        assertEquals( 516, tracer.dbHitsOf( query ) );
    }

    @Test
    public void shouldReportRowsOfQueryExecution() throws Exception
    {
        // given
        Id query = new Id();
        ProfilingTracer tracer = new ProfilingTracer();

        // when
        try ( QueryExecutionEvent event = tracer.executeQuery( query ) )
        {
            for ( int i = 0; i < 11; i++ )
            {
                event.row();
            }
        }

        // then
        assertEquals( 11, tracer.rowsOf( query ) );
    }

    static class Clock implements ProfilingTracer.Clock
    {
        private long time;

        @Override
        public long nanoTime()
        {
            return time;
        }

        public void progress( long nanos )
        {
            assert nanos > 0 : "time must move forwards";
            time += nanos;
        }
    }
}