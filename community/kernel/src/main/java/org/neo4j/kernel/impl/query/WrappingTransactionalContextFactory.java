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
package org.neo4j.kernel.impl.query;

import java.util.function.Function;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.values.virtual.MapValue;

public class WrappingTransactionalContextFactory implements TransactionalContextFactory {

    private final TransactionalContextFactory inner;
    private final Function<TransactionalContext, TransactionalContext> wrap;

    public WrappingTransactionalContextFactory(
            TransactionalContextFactory inner, Function<TransactionalContext, TransactionalContext> wrap) {
        this.inner = inner;
        this.wrap = wrap;
    }

    @Override
    public TransactionalContext newContext(
            InternalTransaction tx,
            String queryText,
            ExecutingQuery parentQuery,
            MapValue queryParameters,
            QueryExecutionConfiguration queryExecutionConfiguration) {
        return wrap.apply(inner.newContext(tx, queryText, parentQuery, queryParameters, queryExecutionConfiguration));
    }

    @Override
    public TransactionalContext newContext(
            InternalTransaction tx,
            String queryText,
            MapValue queryParameters,
            QueryExecutionConfiguration queryExecutionConfiguration) {
        return wrap.apply(inner.newContext(tx, queryText, queryParameters, queryExecutionConfiguration));
    }

    @Override
    public TransactionalContext newContextForQuery(
            InternalTransaction tx,
            ExecutingQuery executingQuery,
            QueryExecutionConfiguration queryExecutionConfiguration) {
        return wrap.apply(inner.newContextForQuery(tx, executingQuery, queryExecutionConfiguration));
    }

    @Override
    public TransactionalContext newContextForQuery(
            InternalTransaction tx,
            ExecutingQuery executingQuery,
            QueryExecutionConfiguration queryExecutionConfiguration,
            ConstituentTransactionFactory constituentTransactionFactory) {
        return wrap.apply(inner.newContextForQuery(
                tx, executingQuery, queryExecutionConfiguration, constituentTransactionFactory));
    }
}
