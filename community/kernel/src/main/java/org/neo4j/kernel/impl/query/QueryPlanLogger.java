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

import org.neo4j.kernel.impl.query.statistic.PlanDetailsToBeLogged;
import org.neo4j.kernel.impl.query.statistic.PlanRuntimeInfo;

/**
 * Logger for query plan cache events.
 */
public interface QueryPlanLogger {

    QueryPlanLogger NO_LOG = (executionPlanCacheKeyHash,
            queryId,
            planDetails,
            cypherVersion,
            runtimeInfo,
            plannerVersion,
            planningTime,
            planningReason) -> {};

    /**
     * Log a newly computed query plan.
     *
     * @param executionPlanCacheKeyHash the hash of the execution plan cache key, as an 8-character hex string
     * @param queryId the id of the query that introduced the execution plan
     * @param planDetails all details of a query plan that need to be logged
     * @param cypherVersion the resolved Cypher version used by the query
     * @param runtimeInfo runtime metadata for the plan
     * @param plannerVersion Cypher planner version metadata, or {@code null} when not available
     * @param planningTime the logical planning time in milliseconds
     * @param planningReason the reason why the query had to be (re)planned, e.g., cache miss, stale statistics, invalid notification existing.
     */
    void planComputed(
            String executionPlanCacheKeyHash,
            String queryId,
            PlanDetailsToBeLogged planDetails,
            String cypherVersion,
            PlanRuntimeInfo runtimeInfo,
            String plannerVersion,
            Long planningTime,
            String planningReason);
}
