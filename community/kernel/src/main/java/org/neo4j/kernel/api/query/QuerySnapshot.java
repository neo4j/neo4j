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
package org.neo4j.kernel.api.query;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.lock.ActiveLock;
import org.neo4j.values.virtual.MapValue;

public class QuerySnapshot {
    private final ExecutingQuery query;
    private final CompilerInfo compilerInfo;
    private final long compilationTimeMicros;
    private final long elapsedTimeMicros;
    private final long cpuTimeMicros;
    private final long waitTimeMicros;
    private final String status;
    private final Map<String, Object> resourceInfo;
    private final List<ActiveLock> waitingLocks;
    private final long activeLockCount;
    private final long allocatedBytes;
    private final long pageHits;
    private final long pageFaults;
    private final Optional<String> obfuscatedQueryText;
    private final Optional<MapValue> obfuscatedQueryParameters;
    private final long transactionId;
    private final long parentTransactionId;
    private final String parentDbName;
    private final QueryCacheUsage executableQueryCacheUsage;
    private final QueryCacheUsage logicalPlanCacheUsage;

    QuerySnapshot(
            ExecutingQuery query,
            CompilerInfo compilerInfo,
            long pageHits,
            long pageFaults,
            long compilationTimeMicros,
            long elapsedTimeMicros,
            long cpuTimeMicros,
            long waitTimeMicros,
            String status,
            Map<String, Object> resourceInfo,
            List<ActiveLock> waitingLocks,
            long activeLockCount,
            long allocatedBytes,
            Optional<String> obfuscatedQueryText,
            Optional<MapValue> obfuscatedQueryParameters,
            long outerTransactionId,
            String parentDbName,
            long parentTransactionId,
            QueryCacheUsage executableQueryCacheUsage,
            QueryCacheUsage logicalPlanCacheUsage) {
        this.query = query;
        this.compilerInfo = compilerInfo;
        this.pageHits = pageHits;
        this.pageFaults = pageFaults;
        this.compilationTimeMicros = compilationTimeMicros;
        this.elapsedTimeMicros = elapsedTimeMicros;
        this.cpuTimeMicros = cpuTimeMicros;
        this.waitTimeMicros = waitTimeMicros;
        this.status = status;
        this.resourceInfo = resourceInfo;
        this.waitingLocks = waitingLocks;
        this.activeLockCount = activeLockCount;
        this.allocatedBytes = allocatedBytes;
        this.obfuscatedQueryText = obfuscatedQueryText;
        this.obfuscatedQueryParameters = obfuscatedQueryParameters;
        this.transactionId = outerTransactionId;
        this.parentDbName = parentDbName;
        this.parentTransactionId = parentTransactionId;
        this.executableQueryCacheUsage = executableQueryCacheUsage;
        this.logicalPlanCacheUsage = logicalPlanCacheUsage;
    }

    public long internalQueryId() {
        return query.internalQueryId();
    }

    public String id() {
        return query.id();
    }

    public String rawQueryText() {
        return query.rawQueryText();
    }

    public Optional<String> obfuscatedQueryText() {
        return obfuscatedQueryText;
    }

    public MapValue rawQueryParameters() {
        return query.rawQueryParameters();
    }

    public Optional<MapValue> obfuscatedQueryParameters() {
        return obfuscatedQueryParameters;
    }

    public Supplier<ExecutionPlanDescription> queryPlanSupplier() {
        return query.planDescriptionSupplier();
    }

    public DeprecationNotificationsProvider deprecationNotificationsProvider() {
        return query.getDeprecationNotificationsProvider();
    }

    public String executingUsername() {
        return query.executingUsername();
    }

    public String authenticatedUsername() {
        return query.authenticatedUsername();
    }

    public Optional<NamedDatabaseId> databaseId() {
        return query.databaseId();
    }

    public ClientConnectionInfo clientConnection() {
        return query.clientConnection();
    }

    public Map<String, Object> transactionAnnotationData() {
        return query.transactionAnnotationData();
    }

    public long activeLockCount() {
        return activeLockCount;
    }

    public String planner() {
        return compilerInfo == null ? null : compilerInfo.planner();
    }

    public String runtime() {
        return compilerInfo == null ? null : compilerInfo.runtime();
    }

    public List<Map<String, String>> indexes() {
        if (compilerInfo == null) {
            return Collections.emptyList();
        }
        return compilerInfo.indexes().stream().map(IndexUsage::asMap).collect(Collectors.toList());
    }

    public String status() {
        return status;
    }

    public Map<String, Object> resourceInformation() {
        return resourceInfo;
    }

    public long startTimestampMillis() {
        return query.startTimestampMillis();
    }

    /**
     * User transaction ID of the outer transaction that is executing this query.
     */
    public long transactionId() {
        return transactionId;
    }

    /**
     * The time spent planning the query, before the query actually starts executing.
     *
     * @return the time in microseconds spent planning the query.
     */
    public long compilationTimeMicros() {
        return compilationTimeMicros;
    }

    /**
     * The time that has been spent waiting on locks or other queries, as opposed to actively executing this query.
     *
     * @return the time in microseconds spent waiting on locks.
     */
    public long waitTimeMicros() {
        return waitTimeMicros;
    }

    /**
     * The time (wall time) that has elapsed since the execution of this query started.
     *
     * @return the time in microseconds since execution of this query started.
     */
    public long elapsedTimeMicros() {
        return elapsedTimeMicros;
    }

    /**
     * Time that the CPU has actively spent working on things related to this query.
     *
     * @return the time in microseconds that the CPU has spent on this query, or {@code null} if the cpu time could not
     * be measured.
     */
    public OptionalLong cpuTimeMicros() {
        return cpuTimeMicros < 0 ? OptionalLong.empty() : OptionalLong.of(cpuTimeMicros);
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
    public OptionalLong idleTimeMicros() {
        return cpuTimeMicros < 0
                ? OptionalLong.empty()
                : OptionalLong.of(elapsedTimeMicros - cpuTimeMicros - waitTimeMicros);
    }

    /**
     * The number of bytes allocated by the query.
     *
     * @return the number of bytes allocated by the execution of the query, or Optional.empty() if measurement was not possible or not enabled.
     */
    public long allocatedBytes() {
        return allocatedBytes;
    }

    public long pageHits() {
        return pageHits;
    }

    public long pageFaults() {
        return pageFaults;
    }

    public List<ActiveLock> waitingLocks() {
        return waitingLocks;
    }

    public String parentDbName() {
        return parentDbName;
    }

    public long parentTransactionId() {
        return parentTransactionId;
    }

    public Optional<QueryCacheUsage> executableQueryCacheUsage() {
        return Optional.ofNullable(executableQueryCacheUsage);
    }

    public Optional<QueryCacheUsage> logicalPlanCacheUsage() {
        return Optional.ofNullable(logicalPlanCacheUsage);
    }
}
