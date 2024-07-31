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
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.values.virtual.MapValue;

public abstract class AbstractMetadataAwareResponseHandler implements ResponseHandler {
    protected final MetadataHandler metadataHandler;

    public AbstractMetadataAwareResponseHandler(MetadataHandler metadataHandler) {
        this.metadataHandler = metadataHandler;
    }

    @Override
    public void onStatementPrepared(
            TransactionType transactionType,
            long statementId,
            long timeSpentPreparingResults,
            List<String> fieldNames) {
        // statement identifiers are inaccessible within implicit transaction mode and are thus
        // omitted here
        if (transactionType != TransactionType.IMPLICIT) {
            this.metadataHandler.onStatementId(this, statementId);
        }

        this.metadataHandler.onTimeSpentPreparingResults(this, timeSpentPreparingResults);
        this.metadataHandler.onFieldNames(this, fieldNames);
    }

    @Override
    public void onStreamingMetadata(
            long timeSpentStreaming,
            QueryExecutionType executionType,
            DatabaseReference database,
            QueryStatistics statistics,
            Iterable<Notification> notifications,
            Iterable<GqlStatusObject> statuses) {
        this.metadataHandler.onTimeSpentStreaming(this, timeSpentStreaming);

        this.metadataHandler.onExecutionType(this, executionType);
        this.metadataHandler.onDatabase(this, database);
        this.metadataHandler.onQueryStatistics(this, statistics);
        this.metadataHandler.onNotifications(this, notifications, statuses);
    }

    @Override
    public void onStreamingExecutionPlan(ExecutionPlanDescription plan) {
        this.metadataHandler.onExecutionPlan(this, plan);
    }

    @Override
    public void onCompleteStreaming(boolean hasRemaining) {
        this.metadataHandler.onResultsRemaining(this, hasRemaining);
    }

    @Override
    public void onRoutingTable(String databaseName, MapValue routingTable) {
        this.metadataHandler.onRoutingTable(this, databaseName, routingTable);
    }

    @Override
    public void onBookmark(String encodedBookmark) {
        this.metadataHandler.onBookmark(this, encodedBookmark);
    }
}
