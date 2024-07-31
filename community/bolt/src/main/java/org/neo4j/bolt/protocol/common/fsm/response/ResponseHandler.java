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
package org.neo4j.bolt.protocol.common.fsm.response;

import java.util.List;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.values.virtual.MapValue;

/**
 * Handles the conversion of state machine results to their network representation.
 */
public interface ResponseHandler extends MetadataConsumer {

    /**
     * Handles the metadata generated as a result of preparing a statement for execution.
     *
     * @param transactionType a transaction type as specified at creation time.
     * @param statementId transaction specific identifier via which this statement may be accessed.
     * @param timeSpentPreparingResults time spent to prepare the result stream (in milliseconds).
     * @param fieldNames identifiers assigned to the respective fields returned by the statement.
     */
    void onStatementPrepared(
            TransactionType transactionType, long statementId, long timeSpentPreparingResults, List<String> fieldNames);

    /**
     * Initiates a stream of records.
     * <p />
     * The contents of the records will be passed to the returned record handler implementation to
     * be converted.
     *
     * @param fieldNames names of the fields within the records.
     * @return a record handler.
     */
    RecordHandler onBeginStreaming(List<String> fieldNames);

    /**
     * Handles the metadata generated as a result of consuming all results within a statement.
     *
     * @param timeSpentStreaming time spent streaming (in milliseconds; including time spent in
     *                           callbacks).
     * @param executionType      execution type of the completed statement.
     * @param database           database on which the statement was executed.
     * @param statistics         number of operations carried out within the statement grouped by operation type.
     * @param notifications      notifications generated during statement execution.
     * @param statuses           gql statuses generated during statement execution.
     */
    void onStreamingMetadata(
            long timeSpentStreaming,
            QueryExecutionType executionType,
            DatabaseReference database,
            QueryStatistics statistics,
            Iterable<Notification> notifications,
            Iterable<GqlStatusObject> statuses);

    /**
     * Handles the execution plan generated as a result of an {@code EXPLAIN} statement.
     *
     * @param plan an execution plan.
     */
    void onStreamingExecutionPlan(ExecutionPlanDescription plan);

    /**
     * Concludes the streaming of records.
     *
     * @param hasRemaining true if records remain within the targeted statement, false otherwise.
     */
    void onCompleteStreaming(boolean hasRemaining);

    void onBookmark(String encodedBookmark);

    /**
     * Handles the routing table generated as a result of a {@code ROUTE} message.
     *
     * @param databaseName the database against which the routing table request has been executed.
     * @param routingTable the resulting routing table.
     */
    void onRoutingTable(String databaseName, MapValue routingTable);

    /**
     * Handles an exceptional termination of the current operation.
     *
     * @param error an object encapsulating additional information on the exceptional termination.
     */
    void onFailure(Error error);

    /**
     * Handles an ignored operation.
     * <p />
     * This method is invoked when an operation is ignored due to the target state machine being in
     * a failed state.
     */
    void onIgnored();

    /**
     * Handles the successful completion of the current operation.
     * <p />
     * When consuming records, this method will be invoked when the last record for the current
     * operation has been consumed. For instance, when invoking `PULL 20`, it will be invoked
     * when 20 records have been streamed or none remain.
     */
    void onSuccess();
}
