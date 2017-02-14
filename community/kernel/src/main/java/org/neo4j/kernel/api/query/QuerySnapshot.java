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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;

public class QuerySnapshot
{
    private final ExecutingQuery query;
    private final PlannerInfo plannerInfo;
    private final long planningTimeMillis;
    private final long elapsedTimeMillis;
    private final long cpuTimeMillis;
    private final long waitTimeMillis;
    private final Map<String,Object> status;
    private final long activeLockCount;

    QuerySnapshot(
            ExecutingQuery query,
            PlannerInfo plannerInfo,
            long planningTimeMillis,
            long elapsedTimeMillis,
            long cpuTimeMillis,
            long waitTimeMillis,
            Map<String,Object> status,
            long activeLockCount )
    {
        this.query = query;
        this.plannerInfo = plannerInfo;
        this.planningTimeMillis = planningTimeMillis;
        this.elapsedTimeMillis = elapsedTimeMillis;
        this.cpuTimeMillis = cpuTimeMillis;
        this.waitTimeMillis = waitTimeMillis;
        this.status = status;
        this.activeLockCount = activeLockCount;
    }

    public long internalQueryId()
    {
        return query.internalQueryId();
    }

    public String queryText()
    {
        return query.queryText();
    }

    public Map<String,Object> queryParameters()
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
        return plannerInfo == null ? null : plannerInfo.planner();
    }

    public String runtime()
    {
        return plannerInfo == null ? null : plannerInfo.runtime();
    }

    public List<Map<String,String>> indexes()
    {
        if ( plannerInfo == null )
        {
            return Collections.emptyList();
        }
        return plannerInfo.indexes().stream()
                .map( IndexUsage::asMap )
                .collect( Collectors.toList() );
    }

    public Map<String,Object> status()
    {
        return status;
    }

    public long startTimestampMillis()
    {
        return query.startTimestampMillis();
    }

    public long planningTimeMillis()
    {
        return planningTimeMillis;
    }

    public long waitTimeMillis()
    {
        return waitTimeMillis;
    }

    public long elapsedTimeMillis()
    {
        return elapsedTimeMillis;
    }

    public long cpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    public long sleepTimeMillis()
    {
        return elapsedTimeMillis - cpuTimeMillis - waitTimeMillis;
    }
}
