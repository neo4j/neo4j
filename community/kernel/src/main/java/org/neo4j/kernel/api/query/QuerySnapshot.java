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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.values.virtual.MapValue;

public class QuerySnapshot
{
    private final ExecutingQuery query;
    private final CompilerInfo compilerInfo;
    private final long compilationTimeMicros;
    private final long elapsedTimeMicros;
    private final long cpuTimeMicros;
    private final long waitTimeMicros;
    private final String status;
    private final Map<String,Object> resourceInfo;
    private final List<ActiveLock> waitingLocks;
    private final long activeLockCount;
    private final long allocatedBytes;
    private final PageCounterValues page;

    QuerySnapshot( ExecutingQuery query, CompilerInfo compilerInfo, PageCounterValues page, long compilationTimeMicros,
                   long elapsedTimeMicros, long cpuTimeMicros, long waitTimeMicros, String status,
                   Map<String,Object> resourceInfo, List<ActiveLock> waitingLocks, long activeLockCount, long allocatedBytes )
    {
        this.query = query;
        this.compilerInfo = compilerInfo;
        this.page = page;
        this.compilationTimeMicros = compilationTimeMicros;
        this.elapsedTimeMicros = elapsedTimeMicros;
        this.cpuTimeMicros = cpuTimeMicros;
        this.waitTimeMicros = waitTimeMicros;
        this.status = status;
        this.resourceInfo = resourceInfo;
        this.waitingLocks = waitingLocks;
        this.activeLockCount = activeLockCount;
        this.allocatedBytes = allocatedBytes;
    }

    public long internalQueryId()
    {
        return query.internalQueryId();
    }

    public String queryText()
    {
        return query.queryText();
    }

    public ExecutionPlanDescription queryPlan()
    {
        return query.planDescription();
    }

    public MapValue queryParameters()
    {
        return query.queryParameters();
    }

    public String username()
    {
        return query.username();
    }

    public ClientConnectionInfo clientConnection()
    {
        return query.clientConnection();
    }

    public Map<String,Object> transactionAnnotationData()
    {
        return query.transactionAnnotationData();
    }

    public long activeLockCount()
    {
        return activeLockCount;
    }

    public String planner()
    {
        return compilerInfo == null ? null : compilerInfo.planner();
    }

    public String runtime()
    {
        return compilerInfo == null ? null : compilerInfo.runtime();
    }

    public List<Map<String,String>> indexes()
    {
        if ( compilerInfo == null )
        {
            return Collections.emptyList();
        }
        return compilerInfo.indexes().stream()
                           .map( IndexUsage::asMap )
                           .collect( Collectors.toList() );
    }

    public String status()
    {
        return status;
    }

    public Map<String,Object> resourceInformation()
    {
        return resourceInfo;
    }

    public long startTimestampMillis()
    {
        return query.startTimestampMillis();
    }

    /**
     * The time spent planning the query, before the query actually starts executing.
     *
     * @return the time in microseconds spent planning the query.
     */
    public long compilationTimeMicros()
    {
        return compilationTimeMicros;
    }

    /**
     * The time that has been spent waiting on locks or other queries, as opposed to actively executing this query.
     *
     * @return the time in microseconds spent waiting on locks.
     */
    public long waitTimeMicros()
    {
        return waitTimeMicros;
    }

    /**
     * The time (wall time) that has elapsed since the execution of this query started.
     *
     * @return the time in microseconds since execution of this query started.
     */
    public long elapsedTimeMicros()
    {
        return elapsedTimeMicros;
    }

    /**
     * Time that the CPU has actively spent working on things related to this query.
     *
     * @return the time in microseconds that the CPU has spent on this query, or {@code null} if the cpu time could not
     * be measured.
     */
    public Long cpuTimeMicros()
    {
        return cpuTimeMicros < 0 ? null : cpuTimeMicros;
    }

    /**
     * Time from the start of this query that the computer spent doing other things than working on this query, even
     * though the query was runnable.
     * <p>
     * In rare cases the idle time can be negative. This is due to the fact that the Thread does not go to sleep
     * immediately after we start measuring the wait-time, there is still some "lock bookkeeping time" that counts as
     * both cpu time (because the CPU is actually actively working on this thread) and wait time (because the query is
     * actually waiting on the lock rather than doing active work). In most cases such "lock bookkeeping time" is going
     * to be dwarfed by the idle time.
     *
     * @return the time in microseconds that this query was de-scheduled, or {@code null} if the cpu time could not be
     * measured.
     */
    public Long idleTimeMicros()
    {
        return cpuTimeMicros < 0 ? null : (elapsedTimeMicros - cpuTimeMicros - waitTimeMicros);
    }

    /**
     * The number of bytes allocated by the query.
     *
     * @return the number of bytes allocated by the execution of the query, or {@code null} if the memory allocation
     * could not be measured.
     */
    public Long allocatedBytes()
    {
        return allocatedBytes < 0 ? null : allocatedBytes;
    }

    public long pageHits()
    {
        return page.hits;
    }

    public long pageFaults()
    {
        return page.faults;
    }

    public List<ActiveLock> waitingLocks()
    {
        return waitingLocks;
    }
}
