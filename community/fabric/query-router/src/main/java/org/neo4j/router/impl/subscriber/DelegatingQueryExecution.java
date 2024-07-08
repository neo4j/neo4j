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
package org.neo4j.router.impl.subscriber;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.impl.query.QueryExecution;

public class DelegatingQueryExecution implements QueryExecution {
    private final QueryExecution queryExecution;

    protected DelegatingQueryExecution(QueryExecution queryExecution) {
        this.queryExecution = queryExecution;
    }

    @Override
    public boolean executionMetadataAvailable() {
        return queryExecution.executionMetadataAvailable();
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
    public Iterable<GqlStatusObject> getGqlStatusObjects() {
        return queryExecution.getGqlStatusObjects();
    }

    @Override
    public String[] fieldNames() {
        return queryExecution.fieldNames();
    }

    @Override
    public void awaitCleanup() {
        queryExecution.awaitCleanup();
    }

    @Override
    public void request(long numberOfRecords) throws Exception {
        queryExecution.request(numberOfRecords);
    }

    @Override
    public void cancel() {
        queryExecution.cancel();
    }

    @Override
    public boolean await() throws Exception {
        return queryExecution.await();
    }

    @Override
    public void consumeAll() throws Exception {
        queryExecution.consumeAll();
    }
}
