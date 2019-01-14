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
package org.neo4j.kernel.impl.api;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.api.operations.QueryRegistrationOperations;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.impl.util.MonotonicCounter;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class StackingQueryRegistrationOperations implements QueryRegistrationOperations
{
    private final MonotonicCounter lastQueryId = MonotonicCounter.newAtomicMonotonicCounter();
    private final SystemNanoClock clock;
    private final AtomicReference<CpuClock> cpuClockRef;
    private final AtomicReference<HeapAllocation> heapAllocationRef;

    public StackingQueryRegistrationOperations(
            SystemNanoClock clock,
            AtomicReference<CpuClock> cpuClockRef,
            AtomicReference<HeapAllocation> heapAllocationRef )
    {
        this.clock = clock;
        this.cpuClockRef = cpuClockRef;
        this.heapAllocationRef = heapAllocationRef;
    }

    @Override
    public Stream<ExecutingQuery> executingQueries( KernelStatement statement )
    {
        return statement.executingQueryList().queries();
    }

    @Override
    public void registerExecutingQuery( KernelStatement statement, ExecutingQuery executingQuery )
    {
        statement.startQueryExecution( executingQuery );
    }

    @Override
    public ExecutingQuery startQueryExecution(
        KernelStatement statement,
        ClientConnectionInfo clientConnection,
        String queryText,
        MapValue queryParameters
    )
    {
        long queryId = lastQueryId.incrementAndGet();
        Thread thread = Thread.currentThread();
        long threadId = thread.getId();
        String threadName = thread.getName();
        ExecutingQuery executingQuery =
                new ExecutingQuery( queryId, clientConnection, statement.username(), queryText, queryParameters,
                        statement.getTransaction().getMetaData(), () -> statement.locks().activeLockCount(),
                        statement.getPageCursorTracer(),
                        threadId, threadName, clock, cpuClockRef.get(), heapAllocationRef.get() );
        registerExecutingQuery( statement, executingQuery );
        return executingQuery;
    }

    @Override
    public void unregisterExecutingQuery( KernelStatement statement, ExecutingQuery executingQuery )
    {
        statement.stopQueryExecution( executingQuery );
    }
}

