/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
 * Thread-safe query collector.
 *
 * Delegates to RingRecentBuffer to hard limit the number of collected queries at any point in time.
 */
class QueryCollector extends CollectorStateMachine<Iterator<TruncatedQuerySnapshot>> implements QueryExecutionMonitor
{
    private volatile boolean isCollecting;
    private final RingRecentBuffer<TruncatedQuerySnapshot> queries;
    private final JobScheduler jobScheduler;
    private final int maxQueryTextSize;

    QueryCollector( JobScheduler jobScheduler,
                    int maxRecentQueryCount,
                    int maxQueryTextSize )
    {
        super( true );
        this.jobScheduler = jobScheduler;
        this.maxQueryTextSize = maxQueryTextSize;
        isCollecting = false;

        // Round down to the nearest power of 2
        int queryBufferSize = Integer.highestOneBit( maxRecentQueryCount );
        queries = new RingRecentBuffer<>( queryBufferSize );
    }

    long numSilentQueryDrops()
    {
        return queries.numSilentQueryDrops();
    }

    // CollectorStateMachine

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
    protected Iterator<TruncatedQuerySnapshot> doGetData()
    {
        List<TruncatedQuerySnapshot> querySnapshots = new ArrayList<>();
        queries.foreach( querySnapshots::add );
        return querySnapshots.iterator();
    }

    // QueryExecutionMonitor

    @Override
    public void endFailure( ExecutingQuery query, Throwable failure )
    {
    }

    @Override
    public void endFailure( ExecutingQuery query, String reason )
    {
    }

    @Override
    public void endSuccess( ExecutingQuery query )
    {
        if ( isCollecting )
        {
            QuerySnapshot snapshot = query.snapshot();
            queries.produce(
                    new TruncatedQuerySnapshot( snapshot.queryText(),
                                                snapshot.queryPlanSupplier(),
                                                snapshot.queryParameters(),
                                                snapshot.elapsedTimeMicros(),
                                                snapshot.compilationTimeMicros(),
                                                snapshot.startTimestampMillis(),
                                                maxQueryTextSize ) );
        }
    }
}
