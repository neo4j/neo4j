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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.fsm.response.AbstractMetadataAwareResponseHandler;
import org.neo4j.bolt.protocol.common.fsm.response.RecordHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.v44.fsm.response.metadata.MetadataHandlerV44;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.server.http.cypher.OutputEventStream;
import org.neo4j.server.http.cypher.TransactionHandle;
import org.neo4j.server.http.cypher.TransactionIndependentValueMapper;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.values.AnyValue;

public class OutputEventStreamResponseHandler extends AbstractMetadataAwareResponseHandler {

    private final OutputEventStream outputEventStream;
    private final Statement statement;
    private final TransactionIndependentValueMapper valueMapper;

    private Map<String, AnyValue> metadataMap = new HashMap<>();
    private QueryExecutionType executionType;
    private QueryStatistics queryStatistics;
    private Iterable<Notification> notifications;
    private ExecutionPlanDescription executionPlanDescription;

    private TransactionHandle transactionHandle;

    public OutputEventStreamResponseHandler(
            OutputEventStream outputEventStream,
            Statement statement,
            TransactionIndependentValueMapper valueMapper,
            TransactionHandle transactionHandle) {
        super(MetadataHandlerV44.getInstance());
        this.outputEventStream = outputEventStream;
        this.statement = statement;
        this.valueMapper = valueMapper;
        this.transactionHandle = transactionHandle;
    }

    @Override
    public void onMetadata(String key, AnyValue value) {
        metadataMap.put(key, value);
    }

    @Override
    public void onStatementPrepared(
            TransactionType transactionType,
            long statementId,
            long timeSpentPreparingResults,
            List<String> fieldNames) {}

    @Override
    public RecordHandler onBeginStreaming(List<String> fieldNames) {
        outputEventStream.writeStatementStart(statement, fieldNames);
        return new OutputEventStreamRecordHandler(fieldNames, outputEventStream, valueMapper);
    }

    @Override
    public void onStreamingMetadata(
            long timeSpentStreaming,
            QueryExecutionType executionType,
            DatabaseReference database,
            QueryStatistics statistics,
            Iterable<Notification> notifications,
            Iterable<GqlStatusObject> statuses) {
        this.executionType = executionType;
        this.queryStatistics = statistics;
        this.notifications = notifications;
    }

    @Override
    public void onStreamingExecutionPlan(ExecutionPlanDescription plan) {
        this.executionPlanDescription = plan;
    }

    @Override
    public void onCompleteStreaming(boolean hasRemaining) {
        outputEventStream.writeStatementEnd(executionType, queryStatistics, executionPlanDescription, notifications);

        executionType = null;
        queryStatistics = null;
        notifications = null;
        executionPlanDescription = null;
    }

    @Override
    public void onBookmark(String encodedBookmark) {
        if (encodedBookmark != null) {
            transactionHandle.setOutputBookmark(encodedBookmark);
        }
    }

    @Override
    public void onFailure(Error error) {
        // Bolt-only event
    }

    @Override
    public void onIgnored() {
        // Bolt-only event
    }

    @Override
    public void onSuccess() {
        // Bolt-only event
    }
}
