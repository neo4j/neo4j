/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.protocol.common.routing;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.neo4j.bolt.protocol.common.fsm.response.RecordHandler;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status.HasStatus;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * Implementation of {@link RoutingTableGetter} which uses the `dbms.routing.getRoutingTable` procedure.
 */
public class ProcedureRoutingTableGetter implements RoutingTableGetter {
    private static final String GET_ROUTING_TABLE_STATEMENT =
            "CALL dbms.routing.getRoutingTable($routingContext, $databaseName)";
    private static final String ROUTING_CONTEXT_PARAM = "routingContext";
    private static final String DATABASE_NAME_PARAM = "databaseName";

    @Override
    public CompletableFuture<MapValue> get(Transaction transaction, MapValue routingContext, String databaseName) {
        var params = getParams(routingContext, databaseName);
        var future = new CompletableFuture<MapValue>();

        try {
            var statement = transaction.run(GET_ROUTING_TABLE_STATEMENT, params);
            try {
                statement.consume(new RoutingTableResponseHandler(future), -1);
            } finally {
                statement.close();
            }
        } catch (TransactionException ex) {
            // if the transaction exception provides its own status code, we'll return it as-is in
            // order to retain control over status codes within Bolt where desired
            var cause = ex.getCause();
            if (!(ex instanceof HasStatus) && cause instanceof HasStatus) {
                future.completeExceptionally(cause);
            } else {
                future.completeExceptionally(ex);
            }
        }

        return future;
    }

    private static MapValue getParams(MapValue routingContext, String databaseName) {
        var paramsBuilder = new MapValueBuilder();
        paramsBuilder.add(ROUTING_CONTEXT_PARAM, routingContext);
        paramsBuilder.add(DATABASE_NAME_PARAM, Values.stringOrNoValue(databaseName));
        return paramsBuilder.build();
    }

    private static class RoutingTableResponseHandler implements ResponseHandler, RecordHandler {
        private final CompletableFuture<MapValue> future;
        private final MapValueBuilder mapValueBuilder;
        private Iterator<String> fieldsIt;

        private RoutingTableResponseHandler(CompletableFuture<MapValue> future) {
            this.future = future;
            this.mapValueBuilder = new MapValueBuilder();
        }

        @Override
        public void onStatementPrepared(
                TransactionType transactionType,
                long statementId,
                long timeSpentPreparingResults,
                List<String> fieldNames) {}

        @Override
        public RecordHandler onBeginStreaming(List<String> fieldNames) {
            this.fieldsIt = fieldNames.iterator();
            return this;
        }

        @Override
        public void onField(AnyValue value) {
            this.mapValueBuilder.add(this.fieldsIt.next(), value);
        }

        @Override
        public void onCompleted() {
            this.future.complete(this.mapValueBuilder.build());
        }

        @Override
        public void onFailure() {
            future.completeExceptionally(new RuntimeException("Failed to generate routing table"));
        }

        @Override
        public void onStreamingMetadata(
                long timeSpentStreaming,
                QueryExecutionType executionType,
                DatabaseReference database,
                QueryStatistics statistics,
                Iterable<Notification> notifications) {}

        @Override
        public void onStreamingExecutionPlan(ExecutionPlanDescription plan) {}

        @Override
        public void onCompleteStreaming(boolean hasRemaining) {}

        @Override
        public void onBookmark(String encodedBookmark) {}

        @Override
        public void onSuccess() {}

        @Override
        public void onFailure(Error error) {
            // FSM callback - not applicable
        }

        @Override
        public void onIgnored() {
            // FSM callback - not applicable
        }

        @Override
        public void onMetadata(String key, AnyValue value) {
            // metadata is discarded for ROUTE requests
        }
    }
}
