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
package org.neo4j.internal.collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;

/**
 * Thread-safe query collector.
 *
 * Delegates to {@link RecentQueryBuffer} to hard limit the number of collected queries at any point in time.
 */
class QueryCollector extends CollectorStateMachine<Iterator<TruncatedQuerySnapshot>> implements QueryExecutionMonitor {
    private volatile boolean isCollecting;
    private final RecentQueryBuffer recentQueryBuffer;
    private final NamedDatabaseId databaseId;
    private final JobScheduler jobScheduler;
    private final int maxQueryTextSize;

    QueryCollector(
            NamedDatabaseId databaseId,
            JobScheduler jobScheduler,
            RecentQueryBuffer recentQueryBuffer,
            int maxQueryTextSize) {
        super(true);
        this.databaseId = databaseId;
        this.jobScheduler = jobScheduler;
        this.maxQueryTextSize = maxQueryTextSize;
        this.recentQueryBuffer = recentQueryBuffer;
        isCollecting = false;
    }

    long numSilentQueryDrops() {
        return recentQueryBuffer.numSilentQueryDrops();
    }

    @Override
    protected Result doCollect(Map<String, Object> config, long collectionId) throws InvalidArgumentsException {
        int collectSeconds = QueryCollectorConfig.of(config).collectSeconds;
        if (collectSeconds > 0) {
            var monitoringParams = JobMonitoringParams.systemJob(databaseId.name(), "Timeout of query collection");
            jobScheduler.schedule(
                    Group.DATA_COLLECTOR,
                    monitoringParams,
                    () -> QueryCollector.this.stop(collectionId),
                    collectSeconds,
                    TimeUnit.SECONDS);
        }
        isCollecting = true;
        return success("Collection started.");
    }

    @Override
    protected Result doStop() {
        isCollecting = false;
        return success("Collection stopped.");
    }

    @Override
    protected Result doClear() {
        recentQueryBuffer.clear(databaseId);
        return success("Data cleared.");
    }

    @Override
    protected Iterator<TruncatedQuerySnapshot> doGetData() {
        List<TruncatedQuerySnapshot> querySnapshots = new ArrayList<>();
        recentQueryBuffer.foreach(databaseId, querySnapshots::add);
        return querySnapshots.iterator();
    }

    // QueryExecutionMonitor

    @Override
    public void startProcessing(ExecutingQuery query) {}

    @Override
    public void startExecution(ExecutingQuery query) {}

    @Override
    public void endFailure(ExecutingQuery query, Throwable failure) {}

    @Override
    public void endFailure(ExecutingQuery query, String reason, org.neo4j.kernel.api.exceptions.Status status) {}

    @Override
    public void endSuccess(ExecutingQuery query) {
        if (isCollecting) {
            QuerySnapshot snapshot = query.snapshot();
            var databaseId = query.databaseId().orElse(null);
            var queryText = snapshot.obfuscatedQueryText().orElse(null);
            var parameters = snapshot.obfuscatedQueryParameters().orElse(null);

            if (databaseId != null && queryText != null && parameters != null) {
                recentQueryBuffer.produce(new TruncatedQuerySnapshot(
                        databaseId,
                        queryText,
                        snapshot.queryPlanSupplier(),
                        parameters,
                        snapshot.elapsedTimeMicros(),
                        snapshot.compilationTimeMicros(),
                        snapshot.startTimestampMillis(),
                        maxQueryTextSize));
            }
        }
    }
}
