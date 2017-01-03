/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.commons.lang3.builder.ToStringBuilder;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.query.QuerySource;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.time.CpuClock;
import org.neo4j.time.SystemNanoClock;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * Represents a currently running query.
 */
public class ExecutingQuery
{
    private static final AtomicLongFieldUpdater<ExecutingQuery> WAIT_TIME =
            newUpdater( ExecutingQuery.class, "waitTimeNanos" );
    private final long queryId;
    private final Locks.Tracer lockTracer = ExecutingQuery.this::waitForLock;

    private final String username;
    private final QuerySource querySource;
    private final String queryText;
    private final Map<String, Object> queryParameters;
    private final long startTime; // timestamp in milliseconds
    private final Thread threadExecutingTheQuery;
    private final SystemNanoClock clock;
    private final CpuClock cpuClock;
    private final long cpuTimeNanosWhenQueryStarted;
    private Map<String,Object> metaData;
    private volatile ExecutingQueryStatus status = ExecutingQueryStatus.RUNNING;
    /** Updated through {@link #WAIT_TIME} */
    @SuppressWarnings( "unused" )
    private volatile long waitTimeNanos;

    public ExecutingQuery(
            long queryId,
            QuerySource querySource,
            String username,
            String queryText,
            Map<String,Object> queryParameters,
            Map<String,Object> metaData,
            Thread threadExecutingTheQuery,
            SystemNanoClock clock,
            CpuClock cpuClock
    ) {
        this.queryId = queryId;
        this.querySource = querySource;
        this.username = username;
        this.queryText = queryText;
        this.queryParameters = queryParameters;
        this.clock = clock;
        this.startTime = clock.millis();
        this.metaData = metaData;
        this.threadExecutingTheQuery = threadExecutingTheQuery;
        this.cpuClock = cpuClock;
        this.cpuTimeNanosWhenQueryStarted = cpuClock.cpuTimeNanos( threadExecutingTheQuery );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ExecutingQuery that = (ExecutingQuery) o;

        return queryId == that.queryId;

    }

    @Override
    public int hashCode()
    {
        return (int) (queryId ^ (queryId >>> 32));
    }

    public long internalQueryId()
    {
        return queryId;
    }

    public String username()
    {
        return username;
    }

    public QuerySource querySource()
    {
        return querySource;
    }

    public String queryText()
    {
        return queryText;
    }

    public Map<String,Object> queryParameters()
    {
        return queryParameters;
    }

    public long startTime()
    {
        return startTime;
    }

    public long elapsedTime()
    {
        return clock.millis() - startTime;
    }

    /**
     * @return the CPU time used by the query, in nanoseconds.
     */
    public long cpuTime()
    {
        return cpuClock.cpuTimeNanos( threadExecutingTheQuery ) - cpuTimeNanosWhenQueryStarted;
    }

    public long waitTime()
    {
        return TimeUnit.NANOSECONDS.toMillis( waitTimeNanos + status.waitTimeNanos( clock ) );
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this );
    }

    public Map<String,Object> metaData()
    {
        return metaData;
    }

    public Locks.Tracer lockTracer()
    {
        return lockTracer;
    }

    public Map<String,Object> status()
    {
        return status.toMap( clock );
    }

    private Locks.WaitEvent waitForLock( ResourceType resourceType, long[] resourceIds )
    {
        ExecutingQueryStatus previous = status;
        long startTimeNanos = clock.nanos();
        status = new ExecutingQueryStatus.WaitingOnLock( resourceType, resourceIds, startTimeNanos );
        return () ->
        {
            WAIT_TIME.addAndGet( ExecutingQuery.this, clock.nanos() - startTimeNanos );
            status = previous;
        };
    }
}
