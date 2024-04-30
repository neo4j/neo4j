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
package org.neo4j.cypher.internal.javacompat;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.cypher.result.EagerQuerySubscription;
import org.neo4j.exceptions.CypherExecutionException;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;

class MaterialisedResult implements QuerySubscriber {
    private final List<AnyValue[]> materialisedRecords = new ArrayList<>();

    private int numberOfFields;
    private AnyValue[] currentRecord;
    private Throwable error;
    private QueryStatistics statistics = QueryStatistics.EMPTY;
    private QueryExecution queryExecution;

    void consumeAll(QueryExecution queryExecution) throws QueryExecutionKernelException {
        this.queryExecution = queryExecution;
        try {
            queryExecution.consumeAll();
        } catch (Exception e) {
            if (e instanceof Status.HasStatus) {
                throw new QueryExecutionKernelException((Exception & Status.HasStatus) e);
            }
            throw new QueryExecutionKernelException(new CypherExecutionException("Query execution failed", e));
        }
    }

    QueryStatistics getQueryStatistics() {
        return statistics;
    }

    @Override
    public void onResult(int numberOfFields) {
        this.numberOfFields = numberOfFields;
    }

    @Override
    public void onRecord() {
        currentRecord = new AnyValue[numberOfFields];
    }

    @Override
    public void onField(int offset, AnyValue value) {
        currentRecord[offset] = value;
    }

    @Override
    public void onRecordCompleted() {
        materialisedRecords.add(currentRecord);
        currentRecord = null;
    }

    @Override
    public void onError(Throwable throwable) {
        error = throwable;
    }

    @Override
    public void onResultCompleted(QueryStatistics statistics) {
        this.statistics = statistics;
    }

    QueryExecution stream(QuerySubscriber subscriber) {
        return new StreamingExecution(subscriber);
    }

    private class StreamingExecution extends EagerQuerySubscription implements QueryExecution {

        StreamingExecution(QuerySubscriber subscriber) {
            super(subscriber);
            error = MaterialisedResult.this.error;

            try {
                subscriber.onResult(queryExecution.fieldNames().length);
            } catch (Exception e) {
                if (error == null) {
                    error = e;
                }
            }
        }

        @Override
        public QueryExecutionType executionType() {
            return queryExecution.executionType();
        }

        @Override
        public ExecutionPlanDescription executionPlanDescription() {
            return queryExecution.executionPlanDescription();
        }

        @Override
        public Iterable<Notification> getNotifications() {
            return queryExecution.getNotifications();
        }

        @Override
        public String[] fieldNames() {
            return queryExecution.fieldNames();
        }

        @Override
        protected QueryStatistics queryStatistics() {
            return statistics;
        }

        @Override
        protected int resultSize() {
            return materialisedRecords.size();
        }

        @Override
        protected void materializeIfNecessary() {
            // Result is already materialized
        }

        @Override
        protected void streamRecordToSubscriber(int servedRecords) throws Exception {
            AnyValue[] recordValues = materialisedRecords.get(servedRecords);
            for (int i = 0; i < recordValues.length; i++) {
                subscriber.onField(i, recordValues[i]);
            }
        }
    }
}
