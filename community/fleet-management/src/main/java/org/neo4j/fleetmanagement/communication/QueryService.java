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
package org.neo4j.fleetmanagement.communication;

import java.util.stream.Collectors;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.fleetmanagement.bootstrap.FleetManagerTask;
import org.neo4j.fleetmanagement.communication.model.QueryReportMessage;
import org.neo4j.fleetmanagement.communication.upstream.Upstream;
import org.neo4j.fleetmanagement.configuration.ClusterSync;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.queries.model.AggregatedQueriesTimeSlice;
import org.neo4j.fleetmanagement.topology.TopologyMapper;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.logging.Log;

public class QueryService extends BufferedReportingService<AggregatedQueriesTimeSlice> {
    private static final int MAX_PAYLOAD_SIZE = 10 * 1024 * 1024; // 10 MiB

    private final ServerIdentity serverIdentity;
    private final Log userLog;

    public QueryService(
            ITransactor transactor,
            ServerIdentity serverIdentity,
            Upstream upstream,
            State state,
            Configuration configuration) {
        super(transactor, upstream, state, configuration, new AggregatedQueriesTimeSlice());
        this.serverIdentity = serverIdentity;
        this.userLog = Logger.getNeo4jLogger();
    }

    public void add(ExecutingQuery query, ErrorGqlStatusObject errorGqlStatusObject, boolean literalTypeHints) {
        if (isBufferLimitReached()) {
            // drop queries until the current buffer is cleared to avoid extra memory consumption
            return;
        }

        var currentSlice = getCurrent();
        if (currentSlice.cumulativeQueryTextSize() < MAX_PAYLOAD_SIZE) {
            currentSlice.add(query, errorGqlStatusObject, literalTypeHints);
        } else {
            // This limit normally shouldn't be reached, but it's here to print an error and cap the
            // current slice rather than rejecting the entire message on the service-side.
            userLog.warn(
                    "Fleet Manager: current query text volume (%s bytes) limit reached",
                    currentSlice.cumulativeQueryTextSize());
        }
    }

    @Override
    public void report() {
        if (!this.state.isConnected()) {
            this.updateBufferSize();
        }

        var oldQueries = getPrevious(new AggregatedQueriesTimeSlice());
        if (oldQueries.getAggregations().isEmpty()) {
            return;
        }

        var msg = new QueryReportMessage(oldQueries.getCreationTime());
        msg.dbmsId = TopologyMapper.getDbmsId(transactor::getDatabases);
        msg.serverId = serverIdentity.serverId().uuid().toString();
        msg.projectId = upstream.getApiKey().projectId();
        msg.queries = oldQueries.getAggregations().entrySet().stream()
                .map(entry -> new QueryReportMessage.AggregatedQueryData(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        transmitReport(msg, Upstream.Endpoint.QUERIES);
        this.resetBuffer();
    }

    public static class QueryReportingTask extends FleetManagerTask {
        private final QueryService queryService;

        public QueryReportingTask(State state, ClusterSync clusterSync, QueryService queryService) {
            super(state, clusterSync);
            this.queryService = queryService;
        }

        @Override
        protected void execute() {
            if (this.state.isConnected()) {
                this.queryService.report();
            }
        }
    }
}
