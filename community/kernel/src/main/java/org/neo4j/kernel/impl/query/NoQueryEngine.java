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

import java.util.List;
import org.neo4j.graphdb.Result;
import org.neo4j.values.virtual.MapValue;

enum NoQueryEngine implements QueryExecutionEngine {
    INSTANCE;

    @Override
    public Result executeQuery(String query, MapValue parameters, TransactionalContext context, boolean prePopulate) {
        throw noQueryEngine();
    }

    @Override
    public QueryExecution executeQuery(
            String query,
            MapValue parameters,
            TransactionalContext context,
            boolean prePopulate,
            QuerySubscriber subscriber) {
        throw noQueryEngine();
    }

    @Override
    public QueryExecution executeQuery(
            String query,
            MapValue parameters,
            TransactionalContext context,
            boolean prePopulate,
            QuerySubscriber subscriber,
            QueryExecutionMonitor monitor) {
        throw noQueryEngine();
    }

    @Override
    public long clearQueryCaches() {
        throw noQueryEngine();
    }

    @Override
    public long clearPreParserCache() {
        throw noQueryEngine();
    }

    @Override
    public long clearExecutableQueryCache() {
        throw noQueryEngine();
    }

    @Override
    public long clearCompilerCache() {
        throw noQueryEngine();
    }

    @Override
    public List<FunctionInformation> getProvidedLanguageFunctions() {
        throw noQueryEngine();
    }

    private static RuntimeException noQueryEngine() {
        return new UnsupportedOperationException("No query engine installed.");
    }
}
