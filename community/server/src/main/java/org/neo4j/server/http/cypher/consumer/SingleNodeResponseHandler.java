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
package org.neo4j.server.http.cypher.consumer;

import java.util.List;
import java.util.function.Consumer;
import org.neo4j.bolt.protocol.common.fsm.response.RecordHandler;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.server.http.cypher.CachingWriter;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

public class SingleNodeResponseHandler implements ResponseHandler {
    private final CachingWriter cachingWriter;
    private final Consumer<Node> nodeConsumer;

    public SingleNodeResponseHandler(CachingWriter cachingWriter, Consumer<Node> nodeConsumer) {
        this.cachingWriter = cachingWriter;
        this.nodeConsumer = nodeConsumer;
    }

    @Override
    public void onMetadata(String key, AnyValue value) {}

    @Override
    public void onStatementPrepared(
            TransactionType transactionType,
            long statementId,
            long timeSpentPreparingResults,
            List<String> fieldNames) {}

    @Override
    public RecordHandler onBeginStreaming(List<String> fieldNames) {
        return new SingleNodeRecordHandler(cachingWriter, nodeConsumer);
    }

    @Override
    public void onStreamingMetadata(
            long timeSpentStreaming,
            QueryExecutionType executionType,
            DatabaseReference database,
            QueryStatistics statistics,
            Iterable<Notification> notifications,
            Iterable<GqlStatusObject> statuses) {}

    @Override
    public void onStreamingExecutionPlan(ExecutionPlanDescription plan) {}

    @Override
    public void onCompleteStreaming(boolean hasRemaining) {}

    @Override
    public void onRoutingTable(String databaseName, MapValue routingTable) {}

    @Override
    public void onBookmark(String encodedBookmark) {}

    @Override
    public void onFailure(Error error) {}

    @Override
    public void onIgnored() {}

    @Override
    public void onSuccess() {}
}
