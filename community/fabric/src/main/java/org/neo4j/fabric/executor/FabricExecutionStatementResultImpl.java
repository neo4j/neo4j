/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.executor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.QueryExecutionType;

class FabricExecutionStatementResultImpl implements StatementResult
{
    private final StatementResult statementResult;
    private final Consumer<Throwable> failureHandler;

    FabricExecutionStatementResultImpl( StatementResult statementResult, Consumer<Throwable> failureHandler )
    {
        this.statementResult = statementResult;
        this.failureHandler = failureHandler;
    }

    @Override
    public Flux<String> columns()
    {
        return statementResult.columns().doOnError( failureHandler );
    }

    @Override
    public Flux<Record> records()
    {
        return statementResult.records().doOnError( failureHandler );
    }

    @Override
    public Mono<Summary> summary()
    {
        return statementResult.summary().doOnError( failureHandler );
    }

    @Override
    public Mono<QueryExecutionType> executionType()
    {
        return statementResult.executionType().doOnError( failureHandler );
    }
}
