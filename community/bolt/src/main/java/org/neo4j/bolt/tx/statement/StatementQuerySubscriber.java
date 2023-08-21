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
package org.neo4j.bolt.tx.statement;

import org.neo4j.bolt.protocol.common.fsm.response.RecordHandler;
import org.neo4j.exceptions.CypherExecutionException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;

public class StatementQuerySubscriber implements QuerySubscriber {
    private RecordHandler handler;

    private Throwable pendingException;
    private QueryStatistics statistics;

    public void setHandler(RecordHandler handler) {
        this.handler = handler;
    }

    public Throwable getPendingException() {
        return this.pendingException;
    }

    public QueryStatistics getStatistics() {
        return this.statistics;
    }

    @Override
    public void onResult(int numberOfFields) throws Exception {}

    @Override
    public void onRecord() throws Exception {
        this.handler.onBegin();
    }

    @Override
    public void onField(int offset, AnyValue value) throws Exception {
        this.handler.onField(value);
    }

    @Override
    public void onRecordCompleted() throws Exception {
        this.handler.onCompleted();
    }

    @Override
    public void onError(Throwable throwable) throws Exception {
        // TODO: Do we need to handle multiple errors here?
        if (this.pendingException == null) {
            this.pendingException = throwable;
        }

        if (this.handler != null) {
            this.handler.onFailure();
        }
    }

    @Override
    public void onResultCompleted(QueryStatistics statistics) {
        this.statistics = statistics;
    }

    public void assertSuccess() throws KernelException {
        if (this.pendingException != null) {
            if (this.pendingException instanceof KernelException kernelException) {
                throw kernelException;
            } else if (this.pendingException instanceof Status.HasStatus hasStatus) {
                throw new QueryExecutionKernelException((Throwable & Status.HasStatus) hasStatus);
            } else {
                throw new QueryExecutionKernelException(
                        new CypherExecutionException(this.pendingException.getMessage(), this.pendingException));
            }
        }
    }
}
