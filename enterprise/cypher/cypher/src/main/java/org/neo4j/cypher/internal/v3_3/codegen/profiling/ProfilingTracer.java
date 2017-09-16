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
package org.neo4j.cypher.internal.v3_3.codegen.profiling;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.QueryExecutionEvent;
import org.neo4j.cypher.internal.compiler.v3_3.spi.KernelStatisticProvider;
import org.neo4j.cypher.internal.v3_3.codegen.QueryExecutionTracer;
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId;
import org.neo4j.helpers.MathUtil;

public class ProfilingTracer implements QueryExecutionTracer
{
    public interface ProfilingInformation
    {
        long time();
        long dbHits();
        long rows();
        long pageCacheHits();
        long pageCacheMisses();
        default double pageCacheHitRatio()
        {
            return MathUtil.portion( pageCacheHits(), pageCacheMisses() );
        }
    }

    public interface Clock
    {
        long nanoTime();

        Clock SYSTEM_TIMER = System::nanoTime;
    }

    private static final Data ZERO = new Data();

    private final Clock clock;
    private final KernelStatisticProvider statisticProvider;
    private final Map<LogicalPlanId, Data> data = new HashMap<>();

    public ProfilingTracer( KernelStatisticProvider statisticProvider )
    {
        this( Clock.SYSTEM_TIMER, statisticProvider );
    }

    ProfilingTracer( Clock clock, KernelStatisticProvider statisticProvider )
    {
        this.clock = clock;
        this.statisticProvider = statisticProvider;
    }

    public ProfilingInformation get( LogicalPlanId query )
    {
        Data value = data.get( query );
        return value == null ? ZERO : value;
    }

    public long timeOf( LogicalPlanId query )
    {
        return get( query ).time();
    }

    public long dbHitsOf( LogicalPlanId query )
    {
        return get( query ).dbHits();
    }

    public long rowsOf( LogicalPlanId query )
    {
        return get( query ).rows();
    }

    @Override
    public QueryExecutionEvent executeOperator( LogicalPlanId queryId )
    {
        Data data = this.data.get( queryId );
        if ( data == null && queryId != null )
        {
            this.data.put( queryId, data = new Data() );
        }
        return new ExecutionEvent( clock, statisticProvider, data );
    }

    private static class ExecutionEvent implements QueryExecutionEvent
    {
        private final long start;
        private final Clock clock;
        private final KernelStatisticProvider statisticProvider;
        private final Data data;
        private long hitCount;
        private long rowCount;

        ExecutionEvent( Clock clock, KernelStatisticProvider statisticProvider, Data data )
        {
            this.clock = clock;
            this.statisticProvider = statisticProvider;
            this.data = data;
            this.start = clock.nanoTime();
        }

        @Override
        public void close()
        {
            long executionTime = clock.nanoTime() - start;
            long pageCacheHits = statisticProvider.getPageCacheHits();
            long pageCacheFaults = statisticProvider.getPageCacheMisses();
            if ( data != null )
            {
                data.update( executionTime, hitCount, rowCount, pageCacheHits, pageCacheFaults );
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
        private long time;
        private long hits;
        private long rows;
        private long pageCacheHits;
        private long pageCacheMisses;

        public void update( long time, long hits, long rows, long pageCacheHits, long pageCacheMisses )
        {
            this.time += time;
            this.hits += hits;
            this.rows += rows;
            this.pageCacheHits += pageCacheHits;
            this.pageCacheMisses += pageCacheMisses;
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

        @Override
        public long pageCacheHits()
        {
            return pageCacheHits;
        }

        @Override
        public long pageCacheMisses()
        {
            return pageCacheMisses;
        }
    }
}
