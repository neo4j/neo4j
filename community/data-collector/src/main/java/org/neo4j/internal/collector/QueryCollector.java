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
package org.neo4j.internal.collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

/**
 * Simple Thread-safe query collector.
 *
 * Note that is has several potentially not-so-nice properties:
 *
 *  - It buffers all query data until collection is done. On high-workload systems
 *    this could use substantial memory
 *
 *  - All threads that report queries on {@link QueryCollector#endSuccess(org.neo4j.kernel.api.query.ExecutingQuery)}
 *    contend for writing to the queue, which might cause delays before the first result on highly concurrent systems
 */
class QueryCollector extends CollectorStateMachine<Iterator<QuerySnapshot>> implements QueryExecutionMonitor
{
    private volatile boolean isCollecting;
    private final RingRecentBuffer<QuerySnapshot> queries = new RingRecentBuffer<>( QUERY_BUFFER_SIZE_IN_BITS );
    private final JobScheduler jobScheduler;
    /**
     * We retain at max 2^13 = 8192 queries in memory at any given time. This number
     * was chosen as a trade-off between getting a useful amount of queries, and not
     * wasting too much heap. Even with a buffer full of unique queries, the estimated
     * footprint lies in tens of MBs. If the buffer is full of cached queries, the
     * retained size was measured to 265 kB.
     */
    private static final int QUERY_BUFFER_SIZE_IN_BITS = 13;

    QueryCollector( JobScheduler jobScheduler )
    {
        super( true );
        this.jobScheduler = jobScheduler;
        isCollecting = false;
    }

    long numSilentQueryDrops()
    {
        return queries.numSilentQueryDrops();
    }

    @Override
    protected Result doCollect( Map<String,Object> config, long collectionId ) throws InvalidArgumentsException
    {
        int collectSeconds = QueryCollectorConfig.of( config ).collectSeconds;
        if ( collectSeconds > 0 )
        {
            jobScheduler.schedule( Group.DATA_COLLECTOR, () -> QueryCollector.this.stop( collectionId ), collectSeconds, TimeUnit.SECONDS );
        }
        isCollecting = true;
        return success( "Collection started." );
    }

    @Override
    protected Result doStop()
    {
        isCollecting = false;
        return success( "Collection stopped." );
    }

    @Override
    protected Result doClear()
    {
        queries.clear();
        return success( "Data cleared." );
    }

    @Override
    protected Iterator<QuerySnapshot> doGetData()
    {
        List<QuerySnapshot> querySnapshots = new ArrayList<>();
        queries.foreach( querySnapshots::add );
        return querySnapshots.iterator();
    }

    // QueryExecutionMonitor

    @Override
    public void endFailure( ExecutingQuery query, Throwable failure )
    {
    }

    @Override
    public void endSuccess( ExecutingQuery query )
    {
        if ( isCollecting )
        {
            queries.produce( query.snapshot() );
        }
    }
}
