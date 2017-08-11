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
package org.neo4j.kernel.impl.api;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.api.operations.QueryRegistrationOperations;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.impl.util.MonotonicCounter;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.time.SystemNanoClock;

public class StackingQueryRegistrationOperations implements QueryRegistrationOperations
{
    private final MonotonicCounter lastQueryId = MonotonicCounter.newAtomicMonotonicCounter();
    private final SystemNanoClock clock;
    private final CpuClock cpuClock;
    private final HeapAllocation heapAllocation;

    public StackingQueryRegistrationOperations(
            SystemNanoClock clock,
            CpuClock cpuClock,
            HeapAllocation heapAllocation )
    {
        this.clock = clock;
        this.cpuClock = cpuClock;
        this.heapAllocation = heapAllocation;
    }

    @Override
    public Stream<ExecutingQuery> executingQueries( KernelStatement statement)
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
        Map<String,Object> queryParameters
    )
    {
        long queryId = lastQueryId.incrementAndGet();
        Thread thread = Thread.currentThread();
        ExecutingQuery executingQuery =
                new ExecutingQuery( queryId, clientConnection, statement.username(), queryText, queryParameters,
                        statement.getTransaction().getMetaData(), statement.locks()::activeLockCount,
                        statement.getPageCursorTracer(),
                        thread, clock, cpuClock, heapAllocation );
        registerExecutingQuery( statement, executingQuery );
        return executingQuery;
    }

    @Override
    public void unregisterExecutingQuery( KernelStatement statement, ExecutingQuery executingQuery )
    {
        statement.stopQueryExecution( executingQuery );
    }
}

