/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.v3_4.codegen.profiling;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.QueryExecutionEvent;
import org.neo4j.cypher.internal.planner.v3_4.spi.KernelStatisticProvider;
import org.neo4j.cypher.internal.util.v3_4.attribution.Id;
import org.neo4j.cypher.internal.v3_4.codegen.QueryExecutionTracer;
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
    private final Map<Id, Data> data = new HashMap<>();

    public ProfilingTracer( KernelStatisticProvider statisticProvider )
    {
        this( Clock.SYSTEM_TIMER, statisticProvider );
    }

    ProfilingTracer( Clock clock, KernelStatisticProvider statisticProvider )
    {
        this.clock = clock;
        this.statisticProvider = statisticProvider;
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
        Data queryData = this.data.get( queryId );
        if ( queryData == null && queryId != null )
        {
            queryData = new Data();
            this.data.put( queryId, queryData );
        }
        return new ExecutionEvent( clock, statisticProvider, queryData );
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
