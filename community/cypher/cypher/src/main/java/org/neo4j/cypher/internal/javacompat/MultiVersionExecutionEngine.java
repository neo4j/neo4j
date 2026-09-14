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

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.cache.CypherQueryCaches;
import org.neo4j.cypher.internal.preparser.FullyParsedQuery;
import org.neo4j.cypher.internal.runtime.InputDataStream;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.values.virtual.MapValue;

public class MultiVersionExecutionEngine extends ExecutionEngine {
    private final MultiVersionQueryRetry queryRetry;

    public MultiVersionExecutionEngine(
            GraphDatabaseQueryService queryService,
            Config config,
            CypherQueryCaches queryCaches,
            InternalLogProvider logProvider,
            CompilerFactory compilerFactory) {
        super(queryService, queryCaches, logProvider, compilerFactory);
        this.queryRetry = new MultiVersionQueryRetry(config, false, false);
    }

    @Override
    public QueryExecution executeQuery(
            String query,
            MapValue parameters,
            TransactionalContext context,
            boolean prePopulate,
            QuerySubscriber subscriber)
            throws QueryExecutionKernelException {
        return queryRetry.executeWithRetry(
                context, () -> super.executeQuery(query, parameters, context, prePopulate, subscriber));
    }

    @Override
    public QueryExecution executeQuery(
            String query,
            MapValue parameters,
            TransactionalContext context,
            boolean prePopulate,
            QuerySubscriber subscriber,
            QueryExecutionMonitor monitor)
            throws QueryExecutionKernelException {
        return queryRetry.executeWithRetry(
                context, () -> super.executeQuery(query, parameters, context, prePopulate, subscriber, monitor));
    }

    @Override
    public QueryExecution executeQuery(
            FullyParsedQuery query,
            MapValue parameters,
            TransactionalContext context,
            boolean prePopulate,
            InputDataStream input,
            QueryExecutionMonitor queryMonitor,
            QuerySubscriber subscriber)
            throws QueryExecutionKernelException {
        return queryRetry.executeWithRetry(
                context,
                () -> super.executeQuery(query, parameters, context, prePopulate, input, queryMonitor, subscriber));
    }
}
