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
package org.neo4j.fabric.bolt;

import java.util.List;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.cypher.internal.javacompat.ResultSubscriber;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.Exceptions;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Rx2SyncStream;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import reactor.core.publisher.Mono;

public class BoltQueryExecutionImpl implements BoltQueryExecution {
    private final QueryExecutionImpl queryExecution;
    private final QuerySubscriber subscriber;

    public BoltQueryExecutionImpl(
            StatementResult statementResult, QuerySubscriber subscriber, FabricConfig fabricConfig) {
        this.subscriber = subscriber;
        var config = fabricConfig.getDataStream();
        var rx2SyncStream = new Rx2SyncStream(statementResult.records(), config.getBatchSize());
        queryExecution = new QueryExecutionImpl(
                rx2SyncStream,
                subscriber,
                statementResult.columns(),
                statementResult.summary(),
                statementResult.executionType());
    }

    public void initialize() throws Exception {
        // Mimic eager execution as triggered in org.neo4j.cypher.internal.result.StandardInternalExecutionResult

        boolean isWriteOnly = queryExecution.executionType().queryType() == QueryExecutionType.QueryType.WRITE;
        boolean isReadOnly = queryExecution.executionType().queryType() == QueryExecutionType.QueryType.READ_ONLY;
        boolean isExplain = queryExecution.executionType().isExplained();
        boolean noResult = queryExecution.fieldNames().length == 0;

        boolean triggerArtificialDemand = isWriteOnly || isExplain || noResult;

        if (triggerArtificialDemand) {
            queryExecution.request(1);
            queryExecution.await();
        }

        if (subscriber instanceof ResultSubscriber && (!isReadOnly || isExplain)) {
            ((ResultSubscriber) subscriber).materialize(queryExecution);
        }
    }

    @Override
    public QueryExecution queryExecution() {
        return queryExecution;
    }

    @Override
    public void close() {
        queryExecution.cancel();
    }

    @Override
    public void terminate() {
        queryExecution.cancel();
    }

    private static class QueryExecutionImpl implements QueryExecution {

        private final Rx2SyncStream rx2SyncStream;
        private final QuerySubscriber subscriber;
        private boolean hasMore = true;
        private boolean initialised;
        private final Mono<Summary> summary;
        private final Mono<QueryExecutionType> queryExecutionType;
        private final List<String> columns;

        private QueryExecutionImpl(
                Rx2SyncStream rx2SyncStream,
                QuerySubscriber subscriber,
                List<String> columns,
                Mono<Summary> summary,
                Mono<QueryExecutionType> queryExecutionType) {
            this.rx2SyncStream = rx2SyncStream;
            this.subscriber = subscriber;
            this.summary = summary;
            this.queryExecutionType = queryExecutionType;
            this.columns = columns;
        }

        private Summary getSummary() {
            return summary.cache().block();
        }

        @Override
        public QueryExecutionType executionType() {
            return queryExecutionType.cache().block();
        }

        @Override
        public ExecutionPlanDescription executionPlanDescription() {
            return getSummary().executionPlanDescription();
        }

        @Override
        public Iterable<Notification> getNotifications() {
            return getSummary().getNotifications();
        }

        @Override
        public Iterable<GqlStatusObject> getGqlStatusObjects() {
            return getSummary().getGqlStatusObjects();
        }

        @Override
        public String[] fieldNames() {
            return columns.toArray(new String[0]);
        }

        @Override
        public void request(long numberOfRecords) throws Exception {
            if (!hasMore) {
                return;
            }

            if (!initialised) {
                initialised = true;
                subscriber.onResult(columns.size());
            }

            try {
                for (int i = 0; i < numberOfRecords; i++) {
                    Record record = rx2SyncStream.readRecord();

                    if (record == null) {
                        hasMore = false;
                        subscriber.onResultCompleted(getSummary().getQueryStatistics());
                        return;
                    }

                    subscriber.onRecord();
                    publishFields(record);
                    subscriber.onRecordCompleted();
                }

                // Let's check if the last record exhausted the stream,
                // This is not necessary for correctness, but might save one extra
                // round trip.
                if (rx2SyncStream.completed()) {
                    hasMore = false;
                    subscriber.onResultCompleted(getSummary().getQueryStatistics());
                }
            } catch (Exception e) {
                throw Exceptions.transform(Status.Statement.ExecutionFailed, e);
            }
        }

        private void publishFields(Record record) throws Exception {
            for (int i = 0; i < columns.size(); i++) {
                subscriber.onField(i, record.getValue(i));
            }
        }

        @Override
        public void cancel() {
            rx2SyncStream.close();
        }

        @Override
        public boolean await() {
            return hasMore;
        }
    }
}
