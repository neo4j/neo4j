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
package org.neo4j.fleetmanagement.queries;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.fleetmanagement.communication.QueryService;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.logging.Log;

public class QueryInterceptor implements QueryExecutionMonitor {

    private final Log userLog;
    private final QueryService queryService;
    private final Config config;
    private final boolean exposeFullyObfuscatedQueryView;

    public QueryInterceptor(QueryService queryService, Config config) {
        this.userLog = Logger.getNeo4jLogger();
        this.queryService = queryService;
        this.config = config;
        this.exposeFullyObfuscatedQueryView =
                config.get(GraphDatabaseInternalSettings.expose_fully_obfuscated_query_view);
    }

    @Override
    public void startProcessing(ExecutingQuery query) {}

    @Override
    public void startExecution(ExecutingQuery query) {}

    @Override
    public void endFailure(ExecutingQuery query, Throwable failure) {}

    @Override
    public void endFailure(
            ExecutingQuery query, String reason, Status status, ErrorGqlStatusObject errorGqlStatusObject) {
        processInterceptedQuery(query, errorGqlStatusObject);
    }

    @Override
    public void endSuccess(ExecutingQuery query) {
        processInterceptedQuery(query, null);
    }

    @Override
    public void beforeEnd(ExecutingQuery query, boolean success) {}

    private void processInterceptedQuery(ExecutingQuery query, ErrorGqlStatusObject errorGqlStatusObject) {
        if (!exposeFullyObfuscatedQueryView && !config.get(GraphDatabaseSettings.log_queries_obfuscate_literals)) {
            userLog.error(String.format(
                    "Fleet Manager requires %s=true (or %s=true) to report queries",
                    GraphDatabaseInternalSettings.expose_fully_obfuscated_query_view.name(),
                    GraphDatabaseSettings.log_queries_obfuscate_literals.name()));
            return;
        }

        queryService.add(query, errorGqlStatusObject);
    }
}
