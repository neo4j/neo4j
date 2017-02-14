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
package org.neo4j.kernel.api.query;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.LongSupplier;

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
    private final Map<String,Object> queryParameters;
    private final long startTimeNanos, startTimestampMillis;
    /** Uses write barrier of {@link #status}. */
    private long planningDoneNanos;
    private final Thread threadExecutingTheQuery;
    private final LongSupplier activeLockCount;
    private final SystemNanoClock clock;
    private final CpuClock cpuClock;
    private final long cpuTimeNanosWhenQueryStarted;
    private final Map<String,Object> transactionAnnotationData;
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
            Map<String,Object> transactionAnnotationData,
            LongSupplier activeLockCount,
            Thread threadExecutingTheQuery,
            SystemNanoClock clock,
            CpuClock cpuClock )
    {
        // Capture timestamps first
        this.cpuTimeNanosWhenQueryStarted = cpuClock.cpuTimeNanos( threadExecutingTheQuery );
        this.startTimeNanos = clock.nanos();
        this.startTimestampMillis = clock.millis();
        // then continue with assigning fields
        this.queryId = queryId;
        this.clientConnection = clientConnection;
        this.username = username;
        this.queryText = queryText;
        this.queryParameters = queryParameters;
        this.transactionAnnotationData = transactionAnnotationData;
        this.activeLockCount = activeLockCount;
        this.threadExecutingTheQuery = threadExecutingTheQuery;
        this.cpuClock = cpuClock;
        this.clock = clock;
    }

    // update state

    public void planningCompleted( PlannerInfo plannerInfo )
    {
        this.plannerInfo = plannerInfo;
        this.planningDoneNanos = clock.nanos();
        this.status = SimpleState.running(); // write barrier - must be last
    }

    public LockTracer lockTracer()
    {
        return lockTracer;
    }

    // snapshot state

    public QuerySnapshot snapshot()
    {
        // capture a consistent snapshot of the "live" state
        ExecutingQueryStatus status;
        long waitTimeNanos, currentTimeNanos, cpuTimeNanos;
        do
        {
            status = this.status; // read barrier, must be first
            waitTimeNanos = this.waitTimeNanos; // the reason for the retry loop: don't count the wait time twice
            cpuTimeNanos = cpuClock.cpuTimeNanos( threadExecutingTheQuery );
            currentTimeNanos = clock.nanos(); // capture the time as close to the snapshot as possible
        }
        while ( this.status != status );
        // guarded by barrier - unused if status is planning, stable otherwise
        long planningDoneNanos = this.planningDoneNanos;
        // guarded by barrier - like planningDoneNanos
        PlannerInfo planner = status.isPlanning() ? null : this.plannerInfo;
        // just needs to be captured at some point...
        long activeLockCount = this.activeLockCount.getAsLong();

        // - at this point we are done capturing the "live" state, and can start computing the snapshot -
        long planningTimeNanos = (status.isPlanning() ? currentTimeNanos : planningDoneNanos) - startTimeNanos;
        long elapsedTimeNanos = currentTimeNanos - startTimeNanos;
        cpuTimeNanos -= cpuTimeNanosWhenQueryStarted;
        waitTimeNanos += status.waitTimeNanos( currentTimeNanos );

        return new QuerySnapshot(
                this,
                planner,
                NANOSECONDS.toMillis( planningTimeNanos ),
                NANOSECONDS.toMillis( elapsedTimeNanos ),
                NANOSECONDS.toMillis( cpuTimeNanos ),
                NANOSECONDS.toMillis( waitTimeNanos ),
                status.toMap( currentTimeNanos ),
                activeLockCount
        );
    }

    // basic methods

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

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this );
    }

    // access stable state

    public long internalQueryId()
    {
        return queryId;
    }

    public String username()
    {
        return username;
    }

    public String queryText()
    {
        return queryText;
    }

    public Map<String,Object> queryParameters()
    {
        return queryParameters;
    }

    public long startTimestampMillis()
    {
        return startTimestampMillis;
    }

    public Map<String,Object> transactionAnnotationData()
    {
        return transactionAnnotationData;
    }

    ClientConnectionInfo clientConnection()
    {
        return clientConnection;
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
                resourceIds,
                this,
                clock.nanos(),
                status );
        status = event;
        return event;
    }

    void doneWaitingOnLock( WaitingOnLockEvent waiting )
    {
        if ( status != waiting )
        {
            return; // already closed
        }
        WAIT_TIME.addAndGet( this, waiting.waitTimeNanos( clock.nanos() ) );
        status = waiting.previousStatus();
    }
}
