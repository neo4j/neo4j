/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.lock.LockTracer;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class ExecutingQueryFactory {
    private static final AtomicLong lastQueryId = new AtomicLong();
    private final SystemNanoClock clock;
    private final AtomicReference<CpuClock> cpuClockRef;
    private final LockTracer systemLockTracer;

    public ExecutingQueryFactory(
            SystemNanoClock clock, AtomicReference<CpuClock> cpuClockRef, LockTracer systemLockTracer) {
        this.clock = clock;
        this.cpuClockRef = cpuClockRef;
        this.systemLockTracer = systemLockTracer;
    }

    public ExecutingQuery createForStatement(StatementInfo statement, String queryText, MapValue queryParameters) {
        ExecutingQuery executingQuery = createUnbound(
                queryText,
                queryParameters,
                statement.clientInfo(),
                statement.executingUser(),
                statement.authenticatedUser(),
                statement.getMetaData());
        bindToStatement(executingQuery, statement);
        return executingQuery;
    }

    public ExecutingQuery createUnbound(
            String queryText,
            MapValue queryParameters,
            ClientConnectionInfo clientConnectionInfo,
            String executingUser,
            String authenticatedUser,
            Map<String, Object> transactionMetaData) {
        Thread thread = Thread.currentThread();
        return new ExecutingQuery(
                lastQueryId.incrementAndGet(),
                clientConnectionInfo,
                executingUser,
                authenticatedUser,
                queryText,
                queryParameters,
                transactionMetaData,
                thread.getId(),
                thread.getName(),
                systemLockTracer,
                clock,
                cpuClockRef.get());
    }

    public static void bindToStatement(ExecutingQuery executingQuery, StatementInfo statement) {
        executingQuery.onTransactionBound(new ExecutingQuery.TransactionBinding(
                statement.namedDatabaseId(),
                statement::getHits,
                statement::getFaults,
                statement::activeLockCount,
                statement.getTransactionSequenceNumber()));
    }

    public static void unbindFromTransaction(ExecutingQuery executingQuery, long userTransactionId) {
        executingQuery.onTransactionUnbound(userTransactionId);
    }

    public static void beforeUnbindFromTransaction(ExecutingQuery executingQuery, long userTransactionId) {
        executingQuery.onPrepareTransactionOnbound(userTransactionId);
    }
}
