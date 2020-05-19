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
package org.neo4j.kernel.impl.api;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.util.MonotonicCounter;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class ExecutingQueryFactory
{
    private static final MonotonicCounter lastQueryId = MonotonicCounter.newAtomicMonotonicCounter();
    private final SystemNanoClock clock;
    private final AtomicReference<CpuClock> cpuClockRef;
    private final AtomicBoolean trackQueryAllocations;

    public ExecutingQueryFactory( SystemNanoClock clock, AtomicReference<CpuClock> cpuClockRef, Config config )
    {
        this.clock = clock;
        this.cpuClockRef = cpuClockRef;
        this.trackQueryAllocations = new AtomicBoolean( config.get( GraphDatabaseSettings.track_query_allocation ) );
        config.addListener( GraphDatabaseSettings.track_query_allocation,
                            ( before, after ) -> trackQueryAllocations.set( after ) );

    }

    public ExecutingQuery createForStatement( KernelStatement statement, String queryText, MapValue queryParameters )
    {
        KernelTransactionImplementation transaction = statement.getTransaction();
        ExecutingQuery executingQuery = createUnbound( queryText, queryParameters, transaction.clientInfo(), statement.username(), transaction.getMetaData() );
        bindToStatement( executingQuery, statement );
        return executingQuery;
    }

    public ExecutingQuery createUnbound( String queryText, MapValue queryParameters,
                                         ClientConnectionInfo clientConnectionInfo, String username, Map<String,Object> transactionMetaData )
    {
        Thread thread = Thread.currentThread();
        return new ExecutingQuery(
                lastQueryId.incrementAndGet(),
                clientConnectionInfo,
                username,
                queryText,
                queryParameters,
                transactionMetaData,
                thread.getId(),
                thread.getName(),
                clock,
                cpuClockRef.get(),
                trackQueryAllocations.get() );
    }

    public void bindToStatement( ExecutingQuery executingQuery, KernelStatement statement )
    {
        executingQuery.onTransactionBound( new ExecutingQuery.TransactionBinding(
                statement.namedDatabaseId(),
                statement::getHits,
                statement::getFaults,
                () -> statement.locks().activeLockCount()
        ) );
    }
}

