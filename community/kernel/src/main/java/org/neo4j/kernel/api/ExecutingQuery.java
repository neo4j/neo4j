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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.LongSupplier;

import org.apache.commons.lang3.builder.ToStringBuilder;

import org.neo4j.kernel.api.query.*;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.LockWaitEvent;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.time.CpuClock;
import org.neo4j.time.SystemNanoClock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * Represents a currently running query.
 */
public class ExecutingQuery
{

    private static final AtomicLongFieldUpdater<ExecutingQuery> WAIT_TIME =
            newUpdater( ExecutingQuery.class, "waitTimeNanos" );
    private final long queryId;
    private final LockTracer lockTracer = this::waitForLock;
    private final String username;
    private final ClientConnectionInfo clientConnection;
    private final String queryText;
    private final Map<String, Object> queryParameters;
    private final long startTime; // timestamp in milliseconds
    /** Uses write barrier of {@link #status}. */
    private long planningDone;
    private final Thread threadExecutingTheQuery;
    private final LongSupplier activeLockCount;
    private final SystemNanoClock clock;
    private final CpuClock cpuClock;
    private final long cpuTimeNanosWhenQueryStarted;
    private final Map<String,Object> metaData;
    /** Uses write barrier of {@link #status}. */
    private PlannerInfo plannerInfo;
    private volatile ExecutingQueryStatus status = SimpleState.planning();
    /** Updated through {@link #WAIT_TIME} */
    @SuppressWarnings( "unused" )
    private volatile long waitTimeNanos;

    public ExecutingQuery(
            long queryId,
            ClientConnectionInfo clientConnection,
            String username,
            String queryText,
            Map<String,Object> queryParameters,
            Map<String,Object> metaData,
            LongSupplier activeLockCount,
            Thread threadExecutingTheQuery,
            SystemNanoClock clock,
            CpuClock cpuClock )
    {
        this.queryId = queryId;
        this.clientConnection = clientConnection;
        this.username = username;
        this.queryText = queryText;
        this.queryParameters = queryParameters;
        this.activeLockCount = activeLockCount;
        this.clock = clock;
        this.startTime = clock.millis();
        this.metaData = metaData;
        this.threadExecutingTheQuery = threadExecutingTheQuery;
        this.cpuClock = cpuClock;
        this.cpuTimeNanosWhenQueryStarted = cpuClock.cpuTimeNanos( threadExecutingTheQuery );
    }

    public void planningCompleted( PlannerInfo plannerInfo )
    {
        this.plannerInfo = plannerInfo;
        this.planningDone = clock.millis();
        this.status = SimpleState.running(); // write barrier - must be last
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

    public ClientConnectionInfo clientConnection()
    {
        return clientConnection;
    }

    public QueryInfo query()
    {
        return status.isPlanning() // read barrier - must be first
                ? new QueryInfo( queryText, queryParameters, null )
                : new QueryInfo( queryText, queryParameters, plannerInfo );
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

    public long planningTimeMillis()
    {
        if ( status.isPlanning() ) // read barrier - must be first
        {
            return elapsedTimeMillis();
        }
        else
        {
            return planningDone - startTime;
        }
    }

    public long elapsedTimeMillis()
    {
        return clock.millis() - startTime;
    }

    /**
     * @return the CPU time used by the query, in nanoseconds.
     */
    public long cpuTimeMillis()
    {
        return NANOSECONDS.toMillis( cpuClock.cpuTimeNanos( threadExecutingTheQuery ) - cpuTimeNanosWhenQueryStarted );
    }

    public long waitTimeMillis()
    {
        return NANOSECONDS.toMillis( waitTimeNanos + status.waitTimeNanos( clock ) );
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

    public LockTracer lockTracer()
    {
        return lockTracer;
    }

    public long activeLockCount()
    {
        return activeLockCount.getAsLong();
    }

    public Map<String,Object> status()
    {
        return status.toMap( clock );
    }

    public String connectionDetailsForLogging()
    {
        return clientConnection.asConnectionDetails();
    }

    private LockWaitEvent waitForLock( boolean exclusive, ResourceType resourceType, long[] resourceIds )
    {
        WaitingOnLockEvent event = new WaitingOnLockEvent(
                exclusive ? ActiveLock.EXCLUSIVE_MODE : ActiveLock.SHARED_MODE,
                resourceType,
                resourceIds );
        status = event;
        return event;
    }

    private class WaitingOnLockEvent extends WaitingOnLock implements LockWaitEvent
    {
        private final ExecutingQueryStatus previous = status;

        WaitingOnLockEvent( String mode, ResourceType resourceType, long[] resourceIds )
        {
            super( mode, resourceType, resourceIds, clock.nanos() );
        }

        @Override
        public void close()
        {
            if ( status != this )
            {
                return; // already closed
            }
            WAIT_TIME.addAndGet( ExecutingQuery.this, waitTimeNanos( clock ) );
            status = previous;
        }

        @Override
        public boolean isPlanning()
        {
            return previous.isPlanning();
        }
    }
}
