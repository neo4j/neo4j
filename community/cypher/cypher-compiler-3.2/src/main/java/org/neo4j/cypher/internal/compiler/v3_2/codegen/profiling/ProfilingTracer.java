/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.profiling;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.internal.compiler.v3_2.codegen.QueryExecutionEvent;
import org.neo4j.cypher.internal.compiler.v3_2.codegen.QueryExecutionTracer;
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id;

public class ProfilingTracer implements QueryExecutionTracer
{
    public interface ProfilingInformation
    {
        long time();
        long dbHits();
        long rows();
    }

    public interface Clock
    {
        long nanoTime();

        Clock SYSTEM_TIMER = System::nanoTime;
    }

    private static final Data ZERO = new Data();

    private final Clock clock;
    private final Map<Id, Data> data = new HashMap<>();

    public ProfilingTracer()
    {
        this( Clock.SYSTEM_TIMER );
    }

    ProfilingTracer( Clock clock )
    {
        this.clock = clock;
    }

    public ProfilingInformation get( Id query )
    {
        Data value = data.get( query );
        return value == null ? ZERO : value;
    }

    public long timeOf( Id query )
    {
        return get( query ).time();
    }

    public long dbHitsOf( Id query )
    {
        return get( query ).dbHits();
    }

    public long rowsOf( Id query )
    {
        return get( query ).rows();
    }

    @Override
    public QueryExecutionEvent executeOperator( Id queryId )
    {
        Data data = this.data.get( queryId );
        if ( data == null && queryId != null )
        {
            this.data.put( queryId, data = new Data() );
        }
        return new ExecutionEvent( clock, data );
    }

    private static class ExecutionEvent implements QueryExecutionEvent
    {
        private final long start;
        private final Clock clock;
        private final Data data;
        private long hitCount;
        private long rowCount;

        public ExecutionEvent( Clock clock, Data data )
        {
            this.clock = clock;
            this.data = data;
            this.start = clock.nanoTime();
        }

        @Override
        public void close()
        {
            long executionTime = clock.nanoTime() - start;
            if ( data != null )
            {
                data.update( executionTime, hitCount, rowCount );
            }
        }

        @Override
        public void dbHit()
        {
            hitCount++;
        }

        @Override
        public void row()
        {
            rowCount++;
        }
    }

    private static class Data implements ProfilingInformation
    {
        private long time, hits, rows;

        public void update( long time, long hits, long rows )
        {
            this.time += time;
            this.hits += hits;
            this.rows += rows;
        }

        @Override
        public long time()
        {
            return time;
        }

        @Override
        public long dbHits()
        {
            return hits;
        }

        @Override
        public long rows()
        {
            return rows;
        }
    }
}
