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
package org.neo4j.kernel.api.query;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockWaitEvent;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.OptionalMemoryTracker;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

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
    private final String rawQueryText;
    private final MapValue rawQueryParameters;
    private final long startTimeNanos;
    private final long startTimestampMillis;
    private final NamedDatabaseId namedDatabaseId;
    private final LongSupplier hitsSupplier;
    private final LongSupplier faultsSupplier;
    /** Uses write barrier of {@link #status}. */
    private long compilationCompletedNanos;
    private String obfuscatedQueryText;
    private MapValue obfuscatedQueryParameters;
    private QueryExecutionType.QueryType queryType;
    private Supplier<ExecutionPlanDescription> planDescriptionSupplier;
    private final long threadExecutingTheQueryId;
    @SuppressWarnings( {"unused", "FieldCanBeLocal"} )
    private final String threadExecutingTheQueryName;
    private final LongSupplier activeLockCount;
    private final long initialActiveLocks;
    private final SystemNanoClock clock;
    private final CpuClock cpuClock;
    private final long cpuTimeNanosWhenQueryStarted;
    private final Map<String,Object> transactionAnnotationData;
    /** Uses write barrier of {@link #status}. */
    private CompilerInfo compilerInfo;
    private volatile ExecutingQueryStatus status = SimpleState.planning();
    /** Updated through {@link #WAIT_TIME} */
    @SuppressWarnings( "unused" )
    private volatile long waitTimeNanos;
    private OptionalMemoryTracker memoryTracker = OptionalMemoryTracker.NONE;

    public ExecutingQuery( long queryId, ClientConnectionInfo clientConnection, NamedDatabaseId namedDatabaseId, String username, String queryText,
            MapValue queryParameters, Map<String,Object> transactionAnnotationData, LongSupplier activeLockCount,
            LongSupplier hitsSupplier, LongSupplier faultsSupplier,
            long threadExecutingTheQueryId, String threadExecutingTheQueryName, SystemNanoClock clock, CpuClock cpuClock )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.hitsSupplier = hitsSupplier;
        this.faultsSupplier = faultsSupplier;
        // Capture timestamps first
        this.cpuTimeNanosWhenQueryStarted = cpuClock.cpuTimeNanos( threadExecutingTheQueryId );
        this.startTimeNanos = clock.nanos();
        this.startTimestampMillis = clock.millis();
        // then continue with assigning fields
        this.queryId = queryId;
        this.clientConnection = clientConnection;
        this.username = username;

        this.rawQueryText = queryText;
        this.rawQueryParameters = queryParameters;
        this.transactionAnnotationData = transactionAnnotationData;
        this.activeLockCount = activeLockCount;
        this.initialActiveLocks = activeLockCount.getAsLong();
        this.threadExecutingTheQueryId = threadExecutingTheQueryId;
        this.threadExecutingTheQueryName = threadExecutingTheQueryName;
        this.cpuClock = cpuClock;
        this.clock = clock;
    }

    // update state

    public void onCompilationCompleted( CompilerInfo compilerInfo,
                                        QueryExecutionType.QueryType queryType,
                                        Supplier<ExecutionPlanDescription> planDescriptionSupplier )
    {
        assertExpectedStatus( SimpleState.planning() );

        this.compilerInfo = compilerInfo;
        this.compilationCompletedNanos = clock.nanos();
        this.planDescriptionSupplier = planDescriptionSupplier;
        this.queryType = queryType;
        this.status = SimpleState.planned(); // write barrier - must be last
    }

    public void onExecutionStarted( OptionalMemoryTracker memoryTracker )
    {
        assertExpectedStatus( SimpleState.planned() );

        this.memoryTracker = memoryTracker;
        this.status = SimpleState.running(); // write barrier - must be last
    }

    public void onRetryAttempted()
    {
        assertExpectedStatus( SimpleState.running() );

        this.compilerInfo = null;
        this.compilationCompletedNanos = 0;
        this.planDescriptionSupplier = null;
        this.queryType = null;
        this.memoryTracker = OptionalMemoryTracker.NONE;
        this.status = SimpleState.planning();
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
        long waitTimeNanos;
        long currentTimeNanos;
        long cpuTimeNanos;
        do
        {
            status = this.status; // read barrier, must be first
            waitTimeNanos = this.waitTimeNanos; // the reason for the retry loop: don't count the wait time twice
            cpuTimeNanos = cpuClock.cpuTimeNanos( threadExecutingTheQueryId );
            currentTimeNanos = clock.nanos(); // capture the time as close to the snapshot as possible
        }
        while ( this.status != status );
        // guarded by barrier - unused if status is planning, stable otherwise
        long compilationCompletedNanos = this.compilationCompletedNanos;
        // guarded by barrier - like compilationCompletedNanos
        CompilerInfo planner = status.isPlanning() ? null : this.compilerInfo;
        List<ActiveLock> waitingOnLocks = status.isWaitingOnLocks() ? status.waitingOnLocks() : Collections.emptyList();
        // activeLockCount is not atomic to capture, so we capture it after the most sensitive part.
        long totalActiveLocks = this.activeLockCount.getAsLong();
        // just needs to be captured at some point...
        PageCounterValues pageCounters = new PageCounterValues( hitsSupplier, faultsSupplier );

        // - at this point we are done capturing the "live" state, and can start computing the snapshot -
        long compilationTimeNanos = (status.isPlanning() ? currentTimeNanos : compilationCompletedNanos) - startTimeNanos;
        long elapsedTimeNanos = currentTimeNanos - startTimeNanos;
        cpuTimeNanos -= cpuTimeNanosWhenQueryStarted;
        waitTimeNanos += status.waitTimeNanos( currentTimeNanos );

        return new QuerySnapshot(
                this,
                planner,
                pageCounters,
                NANOSECONDS.toMicros( compilationTimeNanos ),
                NANOSECONDS.toMicros( elapsedTimeNanos ),
                cpuTimeNanos == 0 && cpuTimeNanosWhenQueryStarted == -1 ? -1 : NANOSECONDS.toMicros( cpuTimeNanos ),
                NANOSECONDS.toMicros( waitTimeNanos ),
                status.name(),
                status.toMap( currentTimeNanos ),
                waitingOnLocks,
                totalActiveLocks - initialActiveLocks,
                memoryTracker.totalAllocatedMemory()
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
        if ( queryNeedsObfuscation() )
        {
            obfuscateQuery();
            return obfuscatedQueryText;
        }
        return rawQueryText;
    }

    Supplier<ExecutionPlanDescription> planDescriptionSupplier()
    {
        return planDescriptionSupplier;
    }

    public MapValue queryParameters()
    {
        if ( queryNeedsObfuscation() )
        {
            obfuscateQuery();
            return obfuscatedQueryParameters;
        }
        return rawQueryParameters;
    }

    public NamedDatabaseId databaseId()
    {
        return namedDatabaseId;
    }

    public long startTimestampMillis()
    {
        return startTimestampMillis;
    }

    public long elapsedNanos()
    {
        return clock.nanos() - startTimeNanos;
    }

    public Map<String,Object> transactionAnnotationData()
    {
        return transactionAnnotationData;
    }

    public long reportedWaitingTimeNanos()
    {
        return waitTimeNanos;
    }

    public long totalWaitingTimeNanos( long currentTimeNanos )
    {
        return waitTimeNanos + status.waitTimeNanos( currentTimeNanos );
    }

    ClientConnectionInfo clientConnection()
    {
        return clientConnection;
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

    private boolean queryNeedsObfuscation()
    {
        return queryType == QueryExecutionType.QueryType.DBMS || queryType == null || namedDatabaseId.equals( NAMED_SYSTEM_DATABASE_ID );
    }

    private void obfuscateQuery()
    {
        if ( obfuscatedQueryText == null )
        {
            Set<String> passwordParams = new HashSet<>();
            // security procedures can be run on both user and system database currently
            this.obfuscatedQueryText = QueryObfuscation.obfuscateText( rawQueryText, passwordParams );
            if ( namedDatabaseId.equals( NAMED_SYSTEM_DATABASE_ID ) )
            {
                // check for system commands
                this.obfuscatedQueryText = QueryObfuscation.obfuscateSystemCommand( obfuscatedQueryText, passwordParams );
            }
            this.obfuscatedQueryParameters = QueryObfuscation.obfuscateParams( rawQueryParameters, passwordParams );
        }
    }

    private void assertExpectedStatus( ExecutingQueryStatus expectedStatus )
    {
        if ( status != expectedStatus )
        {
            throw new IllegalStateException( String.format( "Expected query in '%s' state, actual state is '%s'.", expectedStatus.name(), status.name() ) );
        }
    }
}
