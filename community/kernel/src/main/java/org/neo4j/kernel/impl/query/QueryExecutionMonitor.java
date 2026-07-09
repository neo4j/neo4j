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
package org.neo4j.kernel.impl.query;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.query.statistic.PlanDetailsToBeLogged;
import org.neo4j.kernel.impl.query.statistic.PlanRuntimeInfo;

public interface QueryExecutionMonitor {
    void startProcessing(ExecutingQuery query);

    void startExecution(ExecutingQuery query);

    void endFailure(ExecutingQuery query, Throwable failure);

    void endFailure(ExecutingQuery query, String reason, Status status, ErrorGqlStatusObject errorGqlStatusObject);

    void endSuccess(ExecutingQuery query);

    default void beforeEnd(ExecutingQuery query, boolean success) {}

    /**
     * Called when time has been spent on logical query planning.
     *
     * @param executionPlanCacheKeyHash the hash of the execution plan cache key, as an 8-character hex string
     * @param queryId                   the id of the query that introduced the execution plan
     * @param planDetails               all details of the query plan that need to be logged
     * @param cypherVersion             the resolved Cypher version used by the query
     * @param runtimeInfo               runtime metadata for the plan
     * @param plannerVersion            Cypher planner version metadata, or {@code null} when not available
     * @param planningTimeMillis              the planning time in milliseconds
     * @param planningReason            the reason why the query had to be (re)planned, e.g., cache miss, stale statistics, invalid notification existing.
     */
    default void planComputed(
            String executionPlanCacheKeyHash,
            String queryId,
            PlanDetailsToBeLogged planDetails,
            String cypherVersion,
            PlanRuntimeInfo runtimeInfo,
            String plannerVersion,
            Long planningTimeMillis,
            String planningReason) {}

    QueryExecutionMonitor NO_OP = new QueryExecutionMonitor() {
        @Override
        public void startProcessing(ExecutingQuery query) {}

        @Override
        public void startExecution(ExecutingQuery query) {}

        @Override
        public void endFailure(ExecutingQuery query, Throwable failure) {}

        @Override
        public void endFailure(
                ExecutingQuery query, String reason, Status status, ErrorGqlStatusObject errorGqlStatusObject) {}

        @Override
        public void endSuccess(ExecutingQuery query) {}

        @Override
        public void beforeEnd(ExecutingQuery query, boolean success) {}
    };
}
