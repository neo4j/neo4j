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

/**
 * Logger for query plan cache events.
 * Called when a new execution plan is computed and inserted into the execution plan cache.
 */
public interface QueryPlanLogger {

    QueryPlanLogger NO_LOG = (executionPlanCacheKeyHash, queryId, planDescription) -> {};

    /**
     * Log a newly computed query plan.
     *
     * @param executionPlanCacheKeyHash the hash of the execution plan cache key, as an 8-character hex string
     * @param queryId the id of the query that introduced the execution plan
     * @param planDescription the tree table rendered query plan description
     */
    void planComputed(String executionPlanCacheKeyHash, String queryId, String planDescription);
}
